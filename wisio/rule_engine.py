import dask.bag as db
import dask.dataframe as dd
import dataclasses
import itertools as it
import numpy as np
import pandas as pd
from dask import compute, delayed, persist
from typing import Dict, List, Tuple

from .rules import (
    KNOWN_RULES,
    MAX_REASONS,
    BottleneckRule,
    CharacteristicAccessPatternRule,
    CharacteristicComplexityRule,
    CharacteristicFileCountRule,
    CharacteristicIOOpsRule,
    CharacteristicIOSizeRule,
    CharacteristicIOTimeRule,
    CharacteristicProcessCount,
    CharacteristicTimePeriodCountRule,
    CharacteristicXferSizeRule,
    KnownCharacteristics as kc,
)
from .types import (
    Characteristics,
    Metric,
    RawStats,
    RuleResult,
    ScoringPerViewPerMetric,
    ScoringResult,
    ViewKey,
    ViewResult,
    ViewType,
    view_name,
)


class RuleEngine(object):

    def __init__(
        self,
        rules: Dict[ViewType, List[str]],
        raw_stats: RawStats,
        verbose: bool = False,
    ) -> None:
        self.raw_stats = raw_stats
        self.rules = rules
        self.verbose = verbose

    def process_characteristics(
        self,
        main_view: dd.DataFrame,
        view_results: Dict[Metric, Dict[ViewKey, ViewResult]],
        exclude_characteristics: List[str] = [],
    ) -> Characteristics:

        rules = [
            CharacteristicIOTimeRule(),
            CharacteristicIOOpsRule(),
            CharacteristicIOSizeRule(),
            CharacteristicXferSizeRule(rule_key=kc.READ_XFER_SIZE.value),
            CharacteristicXferSizeRule(rule_key=kc.WRITE_XFER_SIZE.value),
            CharacteristicProcessCount(rule_key=kc.NODE_COUNT.value),
            CharacteristicProcessCount(rule_key=kc.APP_COUNT.value),
            CharacteristicProcessCount(rule_key=kc.PROC_COUNT.value),
            CharacteristicFileCountRule(),
            CharacteristicTimePeriodCountRule(),
            CharacteristicAccessPatternRule(),
            CharacteristicComplexityRule(),
        ]

        rule_dict = {rule.rule_key: rule for rule in rules}
        for exclude_characteristic in np.array(exclude_characteristics).flat:
            rule_dict.pop(exclude_characteristic)

        tasks = {}
        for rule, impl in rule_dict.items():
            tasks[rule] = delayed(impl.handle_task_results)(
                characteristics={dep: tasks[dep] for dep in impl.deps},
                dask_key_name=f"characteristics-{rule}",
                raw_stats=self.raw_stats,
                result=impl.define_tasks(main_view=main_view, view_results=view_results),
            )

        characteristics, = persist(tasks)

        return characteristics

    def process_bottlenecks(
        self,
        evaluated_views: ScoringPerViewPerMetric,
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        exclude_bottlenecks: List[str] = [],
    ) -> Tuple[List[dict], Dict[str, BottleneckRule]]:

        rule_dict = {
            rule: BottleneckRule(rule_key=rule, rule=KNOWN_RULES[rule], verbose=self.verbose)
            for rule in KNOWN_RULES if rule not in exclude_bottlenecks
        }

        metrics = list(evaluated_views.keys())

        view_keys_per_metric = list(it.chain.from_iterable(
            evaluated_views[metric].keys() for metric in metrics))
        evaluated_views_per_metric = list(it.chain.from_iterable(
            evaluated_views[metric].values() for metric in metrics))

        rules_bag = db.from_sequence(
            np.repeat(list(rule_dict.keys()), len(view_keys_per_metric)))
        metrics_bag = db.from_sequence(
            np.repeat(metrics, len(rule_dict) * len(view_keys_per_metric)))
        view_keys_bag = db.from_sequence(
            list(it.chain.from_iterable(it.repeat(view_keys_per_metric, len(rule_dict)))))
        evaluated_views_bag = db.from_sequence(
            list(it.chain.from_iterable(it.repeat(evaluated_views_per_metric, len(rule_dict)))))

        bottlenecks = (
            db.zip(rules_bag, metrics_bag, view_keys_bag, evaluated_views_bag)
            .map(self._define_bottleneck_tasks, rules=rule_dict, metric_boundaries=metric_boundaries)
            .map_partitions(compute)
            .flatten()
            .map(self._consolidate_bottlenecks)
            .flatten()
            .persist()
        )

        return bottlenecks, rule_dict

    @staticmethod
    def _consolidate_bottlenecks(zipped: Tuple[str, str, ViewKey, tuple]):
        rule, metric, view_key, result = zipped

        bottlenecks = result['bottlenecks']
        details = result['details']

        groupped_details = details.groupby(bottlenecks.index.names).nunique()
        groupped_details.columns = groupped_details.columns.map(lambda col: f"num_{col}")

        consolidated = bottlenecks.join(groupped_details)

        for col in bottlenecks.index.names:
            consolidated[f"num_{col}"] = 1  # current view type

        consolidated['metric'] = metric
        consolidated['view_depth'] = len(view_key) if isinstance(view_key, tuple) else 1
        consolidated['view_name'] = view_name(view_key, '.')
        consolidated['rule'] = rule

        for i in range(MAX_REASONS):
            reasoning = result.get(f"reason{i}", pd.Series())
            reasoning.name = f"reason_{i}"
            consolidated = consolidated.join(reasoning)
            consolidated[f"reason_{i}"] = consolidated[f"reason_{i}"].fillna(False)

        if len(bottlenecks.index.names) == 1:
            consolidated.index.rename('subject', inplace=True)  # change index name as subject

        consolidated = consolidated.reset_index()
        consolidated['subject'] = consolidated['subject'].astype(str)  # change int type subject to str

        return consolidated.to_dict(orient='records')

    @staticmethod
    def _define_bottleneck_tasks(
        zipped: Tuple[str, str, ViewKey, ScoringResult],
        rules: Dict[str, BottleneckRule],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
    ):
        rule, metric, view_key, scoring_result = zipped
        rule_impl = rules[rule]

        tasks = rule_impl.define_tasks(
            metric,
            metric_boundary=metric_boundaries[metric],
            view_key=view_key,
            scoring_result=scoring_result,
        )

        return rule, metric, view_key, tasks

    @staticmethod
    def _handle_bottleneck_task_results(
        zipped: Tuple[str, str, ViewKey, tuple],
        rules: Dict[str, BottleneckRule],
    ):
        rule, metric, view_key, result = zipped
        rule_impl = rules[rule]

        results = rule_impl.handle_task_results(
            metric=metric,
            view_key=view_key,
            result=result,
        )

        def convert_rule_result_to_bottleneck(result: RuleResult):
            bottleneck = dataclasses.asdict(result)
            bottleneck['metric'] = metric
            bottleneck['rule'] = rule
            bottleneck['view_name'] = view_name(view_key, '>')
            return bottleneck

        return map(convert_rule_result_to_bottleneck, results)
