import functools as ft
import itertools as it
import dask.bag as db
import dask.dataframe as dd
import numpy as np
from dask import compute
from dask.delayed import Delayed
from typing import Dict, List, Tuple, Union

from .rules import (
    KNOWN_RULES,
    BottleneckRule,
    CharacteristicAccessPatternRule,
    CharacteristicFileCountRule,
    CharacteristicIOOpsRule,
    CharacteristicIOSizeRule,
    CharacteristicIOTimeRule,
    CharacteristicProcessCount,
    CharacteristicRule,
    CharacteristicXferSizeRule,
    KnownCharacteristics as kc,
)
from .types import (
    BottleneckResult,
    BottlenecksPerViewPerMetric,
    Characteristics,
    Metric,
    RuleResult,
    RuleResultsPerViewPerMetricPerRule,
    ViewKey,
    ViewType,
)
from .utils.collection_utils import deepmerge


class RuleEngine(object):

    def __init__(self, rules: Dict[ViewType, List[str]]) -> None:
        self.rules = rules

    def process_characteristics(self, main_view: dd.DataFrame) -> Characteristics:

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
            CharacteristicAccessPatternRule(),
        ]

        rules_bag = db.from_sequence(rules)
        main_view_bag = db.from_sequence(it.repeat(main_view, len(rules)))

        tasks = db.zip(rules_bag, main_view_bag) \
            .map(self._define_characteristic_tasks) \
            .map(compute) \
            .flatten()

        results = db.zip(rules_bag, tasks) \
            .map(self._handle_characteristic_task_results, characteristics=tasks.fold(deepmerge)) \
            .compute()

        characteristics = ft.reduce(deepmerge, results)

        rule_keys = [rule.rule_key for rule in rules]
        sorted_characteristics = dict(
            sorted(characteristics.items(),
                   key=lambda characteristic: rule_keys.index(characteristic[0]))
        )

        return sorted_characteristics

    def process_bottlenecks(
        self,
        evaluated_views: BottlenecksPerViewPerMetric,
        characteristics: Dict[str, RuleResult],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        threshold=0.5,
    ) -> RuleResultsPerViewPerMetricPerRule:

        rules = {rule: BottleneckRule(rule_key=rule, rule=KNOWN_RULES[rule])
                 for rule in KNOWN_RULES}

        metrics = list(evaluated_views.keys())

        view_keys_per_metric = list(it.chain.from_iterable(
            evaluated_views[metric].keys() for metric in metrics))
        evaluated_views_per_metric = list(it.chain.from_iterable(
            evaluated_views[metric].values() for metric in metrics))

        rules_bag = db.from_sequence(
            np.repeat(list(rules.keys()), len(view_keys_per_metric)))
        metrics_bag = db.from_sequence(
            np.repeat(metrics, len(rules) * len(view_keys_per_metric)))
        view_keys_bag = db.from_sequence(
            list(it.chain.from_iterable(it.repeat(view_keys_per_metric, len(rules)))))
        evaluated_views_bag = db.from_sequence(
            list(it.chain.from_iterable(it.repeat(evaluated_views_per_metric, len(rules)))))

        tasks = db.zip(rules_bag, metrics_bag, view_keys_bag, evaluated_views_bag) \
            .map(self._define_bottleneck_tasks,
                 rules=rules,
                 characteristics=characteristics,
                 metric_boundaries=metric_boundaries,
                 threshold=threshold) \
            .map(compute) \
            .flatten()

        results = db.zip(rules_bag, metrics_bag, view_keys_bag, tasks) \
            .map(self._handle_bottleneck_task_results, rules=rules, characteristics=characteristics) \
            .compute()

        bottlenecks = ft.reduce(deepmerge, results)

        return bottlenecks

    @staticmethod
    def _define_characteristic_tasks(zipped: Tuple[CharacteristicRule, dd.DataFrame]):
        rule, main_view = zipped
        tasks = {}
        tasks[rule.rule_key] = rule.define_tasks(main_view=main_view)
        return tasks

    @staticmethod
    def _handle_characteristic_task_results(
        zipped: Tuple[CharacteristicRule, Dict[str, Delayed]],
        characteristics: Dict[str, Union[dict, list, int, float]],
    ):
        rule, result = zipped
        characteristic = {}
        characteristic[rule.rule_key] = rule.handle_task_results(
            result=result[rule.rule_key],
            characteristics=characteristics,
        )
        return characteristic

    @staticmethod
    def _define_bottleneck_tasks(
        zipped: Tuple[str, str, ViewKey, BottleneckResult],
        rules: Dict[str, BottleneckRule],
        characteristics: Dict[str, RuleResult],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        threshold: float,
    ):
        rule, metric, view_key, bottleneck_result = zipped
        rule_impl = rules[rule]
        return rule_impl.define_tasks(
            metric,
            metric_boundary=metric_boundaries[metric],
            view_key=view_key,
            bottleneck_result=bottleneck_result,
            characteristics=characteristics,
            threshold=threshold,
        )

    @staticmethod
    def _handle_bottleneck_task_results(
        zipped: Tuple[str, str, ViewKey, tuple],
        rules: Dict[str, BottleneckRule],
        characteristics: Dict[str, RuleResult],
    ):
        rule, metric, view_key, result = zipped
        rule_impl = rules[rule]
        bottleneck = {}
        bottleneck[rule] = {}
        bottleneck[rule][metric] = {}
        bottleneck[rule][metric][view_key] = rule_impl.handle_task_results(
            metric=metric,
            view_key=view_key,
            result=result,
            characteristics=characteristics,
        )
        return bottleneck
