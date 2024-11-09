import dask.dataframe as dd
import dataclasses
import itertools as it
import pandas as pd
from dask import delayed, persist
from scipy.cluster.hierarchy import linkage, fcluster
from sklearn.preprocessing import StandardScaler
from typing import Dict, List, Tuple, Union

from .analysis_utils import set_file_dir, set_file_pattern, set_proc_name_parts
from .constants import (
    COL_APP_NAME,
    COL_BEHAVIOR,
    COL_FILE_DIR,
    COL_FILE_NAME,
    COL_FILE_PATTERN,
    COL_NODE_NAME,
    COL_PROC_NAME,
    COL_RANK,
    Layer,
)
from .rules import (
    KNOWN_RULES,
    MAX_REASONS,
    BottleneckRule,
    CharacteristicAccessPatternRule,
    CharacteristicAppTimeRule,
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
    BottleneckRules,
    Bottlenecks,
    Characteristics,
    Metric,
    RawStats,
    RuleResult,
    ViewKey,
    Views,
    ViewType,
    view_name as format_view_name,
)


def compute_characteristics(
    layer: Layer,
    view: dd.DataFrame,
    raw_stats: RawStats,
    app_characteristics: Characteristics = None,
    exclude_characteristics: List[str] = [],
    unoverlapped_posix_only: bool = False,
) -> Characteristics:
    rules = {
        Layer.APP: [
            CharacteristicAppTimeRule(),
        ],
        Layer.DATALOADER: [],
        Layer.POSIX: [
            CharacteristicIOTimeRule(is_unoverlapped=unoverlapped_posix_only),
            CharacteristicIOOpsRule(is_unoverlapped=unoverlapped_posix_only),
            CharacteristicIOSizeRule(is_unoverlapped=unoverlapped_posix_only),
            CharacteristicXferSizeRule(rule_key=kc.READ_XFER_SIZE.value),
            CharacteristicXferSizeRule(rule_key=kc.WRITE_XFER_SIZE.value),
            CharacteristicProcessCount(rule_key=kc.NODE_COUNT.value),
            CharacteristicProcessCount(rule_key=kc.APP_COUNT.value),
            CharacteristicProcessCount(rule_key=kc.PROC_COUNT.value),
            CharacteristicFileCountRule(),
            CharacteristicTimePeriodCountRule(),
            CharacteristicAccessPatternRule(),
        ],
    }
    rule_dict = {rule.rule_key: rule for rule in rules[layer]}
    for excluded_char in exclude_characteristics:
        if excluded_char in rule_dict:
            rule_dict.pop(excluded_char)
    tasks = {}
    for rule, impl in rule_dict.items():
        if impl.prefer_app_characteristics:
            if app_characteristics is None:
                raise ValueError(
                    f"App characteristics are required for rule {rule} but not provided"
                )
            tasks[rule] = delayed(impl.handle_task_results)(
                characteristics={dep: app_characteristics[dep] for dep in impl.deps},
                dask_key_name=f"characteristics-{rule}",
                raw_stats=raw_stats,
                result=impl.define_tasks(view),
            )
        else:
            tasks[rule] = delayed(impl.handle_task_results)(
                characteristics={dep: tasks[dep] for dep in impl.deps},
                dask_key_name=f"characteristics-{rule}",
                raw_stats=raw_stats,
                result=impl.define_tasks(view),
            )
    (characteristics,) = persist(tasks)
    return characteristics


def reason_bottlenecks(
    bottlenecks: Dict[Layer, Bottlenecks],
    main_indexes: Dict[Layer, dd.DataFrame],
    rules: BottleneckRules,
    view_types: Dict[Layer, List[ViewType]],
    exclude_bottlenecks: List[str] = [],
) -> Dict[Layer, Bottlenecks]:
    reasoned_bottlenecks = {}
    for layer in bottlenecks:
        layer_rules = dict(rules)
        for rule, rule_def in rules.items():
            if rule_def.layers and layer not in rule_def.layers:
                del layer_rules[rule]
        reasoned_bottlenecks[layer] = {}
        for view_key in bottlenecks[layer]:
            reasoned_bottlenecks[layer][view_key] = {}
            for metric in bottlenecks[layer][view_key]:
                view = bottlenecks[layer][view_key][metric]
                detail_index = main_indexes[layer].copy()
                for view_type in view_key:
                    detail_index = detail_index.query(
                        f"{view_type} in @view_index",
                        local_dict={
                            "view_index": bottlenecks[layer][(view_type,)][metric].index
                        },
                    )
                view_type = view_key[-1]
                for detail_view_type in view_types[layer]:
                    detail_view = detail_index.groupby(view_type)[
                        detail_view_type
                    ].nunique()
                    detail_view.name = f"n_{detail_view_type}"
                    view = view.merge(detail_view.to_frame())
                reasoned_bottlenecks[layer][view_key][metric] = view.map_partitions(
                    _set_bottleneck_reasons, rules=layer_rules
                )
    return reasoned_bottlenecks


def _set_bottleneck_reasons(bottlenecks: pd.DataFrame, rules: BottleneckRules):
    for rule_key, rule_def in rules.items():
        rule_result = bottlenecks.eval(rule_def.condition)
        rule_result.name = f"b_{rule_key}"
        bottlenecks = bottlenecks.join(rule_result)
        for i, reason in enumerate(rule_def.reasons):
            reason_result = bottlenecks.eval(reason.condition)
            reason_result.name = f"b_{rule_key}_{i}"
            bottlenecks = bottlenecks.join(reason_result)
    return bottlenecks


def _assign_behavior(bottlenecks: pd.DataFrame, metric: str):
    view_name_col, score_col = 'view_name', f"{metric}_score"

    view_names = list(bottlenecks[view_name_col].unique())
    scores = list(bottlenecks[score_col].unique())

    bottlenecks = bottlenecks.set_index([view_name_col, score_col]).sort_index()

    for view_name, score in it.product(view_names, scores):
        behavior_key = (view_name, score)

        if behavior_key not in bottlenecks.index:
            continue

        behaviors = bottlenecks.loc[[behavior_key]]

        bottlenecks.loc[behavior_key, COL_BEHAVIOR] = 1  # default behavior

        if len(behaviors) > 1:
            cols = bottlenecks.columns

            include_cond = (
                '_min|_max|_count|_size|_time|excessive_|small_|_imbalance|_bin'
            )
            exclude_cond = 'num_'

            behavior_cols = cols[
                cols.str.contains(include_cond) & ~cols.str.contains(exclude_cond)
            ]

            scaler = StandardScaler()
            scaled_behavior = scaler.fit_transform(behaviors[behavior_cols])

            link_mat = linkage(scaled_behavior, method='weighted')
            clusters = fcluster(link_mat, t=1, criterion='distance')

            bottlenecks.loc[behavior_key, COL_BEHAVIOR] = clusters

    return bottlenecks.reset_index()


class RuleEngine(object):
    def __init__(
        self,
        rules: BottleneckRules,
        raw_stats: RawStats,
        verbose: bool = False,
    ) -> None:
        self.raw_stats = raw_stats
        self.rules = rules
        self.verbose = verbose

    def process_bottlenecks(
        self,
        views: Views,
        group_behavior: bool,
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        exclude_bottlenecks: List[str] = [],
    ) -> Tuple[List[dict], Dict[str, BottleneckRule]]:
        rule_dict = {
            rule: BottleneckRule(
                rule_key=rule, rule=KNOWN_RULES[rule], verbose=self.verbose
            )
            for rule in KNOWN_RULES
            if rule not in exclude_bottlenecks
        }

        bottlenecks = []

        for metric in views:
            for view_key in views[metric]:
                scored_view = views[metric][view_key].scored_view
                records_index = views[metric][view_key].records_index
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

        if group_behavior:
            concatenated = (
                dd.concat(bottlenecks)
                .map_partitions(_assign_behavior, metric=metric)
                .persist()
            )
        else:
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
            details = set_file_pattern(
                df=details.set_index(COL_FILE_NAME)
            ).reset_index()
        elif any(
            view_type in view_types
            for view_type in [COL_APP_NAME, COL_NODE_NAME, COL_RANK]
        ):
            details = set_proc_name_parts(
                df=details.set_index(COL_PROC_NAME)
            ).reset_index()

        # TODO unique instead of nunique
        details = details.groupby(view_types).nunique()
        details.columns = details.columns.map(lambda col: f"num_{col}")

        # Create bottlenecks
        bottlenecks = scored_view.join(details)

        for view_type in view_types:
            bottlenecks[f"num_{view_type}"] = 1  # current view type fix

        bottlenecks['metric'] = metric
        bottlenecks['view_depth'] = len(view_key) if isinstance(view_key, tuple) else 1
        bottlenecks['view_name'] = format_view_name(view_key, '.')

        for rule, impl in rule_dict.items():
            rule_result = bottlenecks.eval(impl.rule.condition)
            rule_result.name = rule
            bottlenecks = bottlenecks.join(rule_result)

            for i, reason in enumerate(impl.rule.reasons):
                reason_result = bottlenecks.eval(reason.condition)
                reason_result.name = f"{rule}.reason.{i}"
                bottlenecks = bottlenecks.join(reason_result)

        if len(view_types) == 1:
            # change index name as subject
            bottlenecks.index.rename('subject', inplace=True)

        bottlenecks = bottlenecks.reset_index()
        # change int type subject to str
        bottlenecks['subject'] = bottlenecks['subject'].astype(str)

        return bottlenecks

    @staticmethod
    def _consolidate_bottlenecks(zipped: Tuple[str, str, ViewKey, tuple]):
        rule, metric, view_key, result = zipped

        bottlenecks = result['bottlenecks']
        details = result['details']

        groupped_details = details.groupby(bottlenecks.index.names).nunique()
        groupped_details.columns = groupped_details.columns.map(
            lambda col: f"num_{col}"
        )

        consolidated = bottlenecks.join(groupped_details)

        for col in bottlenecks.index.names:
            consolidated[f"num_{col}"] = 1  # current view type

        consolidated['metric'] = metric
        consolidated['view_depth'] = len(view_key) if isinstance(view_key, tuple) else 1
        consolidated['view_name'] = format_view_name(view_key, '.')
        consolidated['rule'] = rule

        for i in range(MAX_REASONS):
            reasoning = result.get(f"reason{i}", pd.Series())
            reasoning.name = f"reason_{i}"
            consolidated = consolidated.join(reasoning)
            consolidated[f"reason_{i}"] = consolidated[f"reason_{i}"].fillna(False)

        if len(bottlenecks.index.names) == 1:
            consolidated.index.rename(
                'subject', inplace=True
            )  # change index name as subject

        consolidated = consolidated.reset_index()
        consolidated['subject'] = consolidated['subject'].astype(
            str
        )  # change int type subject to str

        return consolidated.to_dict(orient='records')

    @staticmethod
    def _define_bottleneck_tasks(
        zipped: Tuple[str, str, ViewKey, Views],
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
            bottleneck['view_name'] = format_view_name(view_key, '>')
            return bottleneck

        return map(convert_rule_result_to_bottleneck, results)
