import dask.dataframe as dd
import dataclasses
import pandas as pd
from dask import delayed, persist
from typing import Dict, List, Tuple, Union

from .analysis_utils import set_file_dir, set_file_pattern, set_proc_name_parts
from .constants import (
    COL_APP_NAME,
    COL_FILE_DIR,
    COL_FILE_NAME,
    COL_FILE_PATTERN,
    COL_NODE_NAME,
    COL_PROC_NAME,
    COL_RANK,
)
from .rules import (
    KNOWN_RULES,
    MAX_REASONS,
    BottleneckRule,
    CharacteristicAccessPatternRule,
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
        ]

        rule_dict = {rule.rule_key: rule for rule in rules}
        for excluded_char in exclude_characteristics:
            if excluded_char in rule_dict:
                rule_dict.pop(excluded_char)

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

        bottlenecks = []

        for metric in evaluated_views:
            for view_key in evaluated_views[metric]:
                scored_view = evaluated_views[metric][view_key].scored_view
                records_index = evaluated_views[metric][view_key].records_index
                bottlenecks.append(
                    scored_view.map_partitions(
                        self._assign_bottlenecks,
                        metric=metric,
                        metric_boundary=metric_boundaries[metric],
                        records_index=records_index,
                        rule_dict=rule_dict,
                        view_key=view_key,
                    )
                )

        concatenated = dd.concat(bottlenecks).persist()

        return concatenated, rule_dict

    @staticmethod
    def _assign_bottlenecks(
        scored_view: pd.DataFrame,
        metric: Metric,
        metric_boundary: Union[int, float],
        records_index: pd.Index,
        rule_dict: Dict[str, BottleneckRule],
        view_key: ViewKey,
    ):
        # TODO make generic
        scored_view['time_overall'] = scored_view['time'] / metric_boundary

        view_types = scored_view.index.names

        details = records_index.to_frame(index=False)

        # Logical view type fix
        if COL_FILE_DIR in view_types:
            details = set_file_dir(df=details.set_index(COL_FILE_NAME)).reset_index()
        elif COL_FILE_PATTERN in view_types:
            details = set_file_pattern(df=details.set_index(COL_FILE_NAME)).reset_index()
        elif any(view_type in view_types for view_type in [COL_APP_NAME, COL_NODE_NAME, COL_RANK]):
            details = set_proc_name_parts(df=details.set_index(COL_PROC_NAME)).reset_index()

        # TODO unique instead of nunique
        details = details.groupby(view_types).nunique()
        details.columns = details.columns.map(lambda col: f"num_{col}")

        # Create bottlenecks
        bottlenecks = scored_view.join(details)

        for view_type in view_types:
            bottlenecks[f"num_{view_type}"] = 1  # current view type fix

        bottlenecks['metric'] = metric
        bottlenecks['view_depth'] = len(view_key) if isinstance(view_key, tuple) else 1
        bottlenecks['view_name'] = view_name(view_key, '.')

        for rule, impl in rule_dict.items():
            rule_result = bottlenecks.eval(impl.rule.condition)
            rule_result.name = rule
            bottlenecks = bottlenecks.join(rule_result)

            for i, reason in enumerate(impl.rule.reasons):
                reason_result = bottlenecks.eval(reason.condition)
                reason_result.name = f"{rule}.reason.{i}"
                bottlenecks = bottlenecks.join(reason_result)

        if len(view_types) == 1:
            bottlenecks.index.rename('subject', inplace=True)  # change index name as subject

        bottlenecks = bottlenecks.reset_index()
        bottlenecks['subject'] = bottlenecks['subject'].astype(str)  # change int type subject to str

        return bottlenecks

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
