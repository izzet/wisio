import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import compute, delayed
from typing import Any, Dict, List, Tuple, Type
from ..rules import Rule, RuleEngine, RuleReason, RuleResult
from ..types import (
    COL_APP_NAME,
    COL_FILE_DIR,
    COL_FILE_NAME,
    COL_FILE_REGEX,
    COL_NODE_NAME,
    COL_PROC_NAME,
    COL_RANK,
    ViewKey,
    ViewType,
)
from ..utils.collection_utils import get_intervals, join_with_and
from ..utils.common_utils import convert_bytes_to_unit
from .analysis import (
    ACC_PAT_SUFFIXES,
    DERIVED_MD_OPS,
    IO_TYPES,
    compute_max_io_time,
)
from .bottlenecks import BOTTLENECK_ORDER
from .constants import AccessPattern


METADATA_ACCESS_RATIO_THRESHOLD = 0.5
SMALL_READ_SIZE_THRESHOLD = 256 * 1024
SMALL_READ_RATIO_THRESHOLD = 0.75
SMALL_WRITE_SIZE_THRESHOLD = 256 * 1024
SMALL_WRITE_RATIO_THRESHOLD = 0.75
XFER_SIZE_BINS = [
    -np.inf,
    4 * 1024.0,
    16 * 1024.0,
    64 * 1024.0,
    256 * 1024.0,
    1 * 1024.0 * 1024.0,
    4 * 1024.0 * 1024.0,
    16 * 1024.0 * 1024.0,
    64 * 1024.0 * 1024.0,
    np.inf
]
XFER_SIZE_BIN_LABELS = [
    '<4 KB',
    '4-16 KB',
    '16-64 KB',
    '64-256 KB',
    '256 KB-1 MB',
    '1-4 MB',
    '4-16 MB',
    '16-64 MB',
    '>64 MB',
]
XFER_SIZE_BIN_NAMES = [
    '<4 KB',
    '4 KB',
    '16 KB',
    '64 KB',
    '256 KB',
    '1 MB',
    '4 MB',
    '16 MB',
    '64 MB',
    '>64 MB'
]


def _describe_bottleneck(
    row: Type[tuple],
    view_type: str,
    low_level_view: pd.DataFrame,
    max_io_time: float
) -> Tuple[Any, str]:

    ix = row.Index
    ix_low_level_view = low_level_view.loc[[ix]]

    time_ranges = list(ix_low_level_view.index.unique(level='time_range'))
    time_range_intervals = get_intervals(values=time_ranges)

    if view_type in [COL_FILE_NAME, COL_FILE_DIR, COL_FILE_REGEX]:
        processes = list(ix_low_level_view.index.unique(level=COL_PROC_NAME))
        description = (
            f"'{ix}' is accessed by {len(processes)} process(es) "
            f"during the {join_with_and(values=time_range_intervals)}th second(s) "
            f"has an I/O time of {row.duration_sum:.2f} seconds which is "
            f"{row.duration_sum/max_io_time*100:.2f}% of overall I/O time of the workload."
        )
    elif view_type in [COL_APP_NAME, COL_NODE_NAME, COL_PROC_NAME, COL_RANK]:
        files = list(ix_low_level_view.index.unique(level=COL_FILE_NAME))
        description = (
            f"'{ix}' accesses {len(files)} file(s) "
            f"during the {join_with_and(values=time_range_intervals)}th second(s) "
            f"and has an I/O time of {row.duration_sum:.2f} seconds which is "
            f"{row.duration_sum/max_io_time*100:.2f}% of overall I/O time of the workload."
        )
    else:
        files = list(ix_low_level_view.index.unique(level=COL_FILE_NAME))
        processes = list(ix_low_level_view.index.unique(level=COL_PROC_NAME))
        description = (
            f"{len(processes)} process(es) access(es) {len(files)} file(s) "
            f"during the {join_with_and(values=time_range_intervals)}th second(s) "
            f"and has an I/O time of {row.duration_sum:.2f} seconds which is "
            f"{row.duration_sum/max_io_time*100:.2f}% of overall I/O time of the workload."
        )

    return ix, description


def _get_data_dict(row: Type[tuple], view_type: str, low_level_view: pd.DataFrame):

    ix = row.Index
    ix_low_level_view = low_level_view.loc[[ix]]

    data_dict = {}
    data_dict['llc'] = dict(row._asdict())
    data_dict['llc'].pop('Index')

    levels = list(BOTTLENECK_ORDER[view_type])
    levels.remove(view_type)

    for level in levels:
        data_dict[f"{level}_list"] = list(ix_low_level_view.index.unique(level=level))

    return data_dict


def _get_xfer_size(size: float):
    size_bin = np.digitize(size, bins=XFER_SIZE_BINS, right=True)
    size_label = np.choose(size_bin, choices=XFER_SIZE_BIN_NAMES, mode='clip')
    return size_label


def _get_xfer_dist(view: pd.DataFrame, io_op: str):
    count_col, min_col, max_col, per_col, xfer_col = (
        'count',
        f"{io_op}_min",
        f"{io_op}_max",
        'per',
        'xfer',
    )

    min_view = view[view[min_col] > 0]
    max_view = view[view[max_col] > 0]

    min_val = int(min_view[min_col].min())
    max_val = int(max_view[max_col].max())

    max_value_counts = pd.DataFrame(max_view[max_col].value_counts()).rename(columns={max_col: count_col})
    max_value_counts[xfer_col] = pd.cut(max_value_counts.index, bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS)
    xfer_bins = max_value_counts.groupby([xfer_col]).sum().replace(0, np.nan).dropna()
    xfer_bins[per_col] = xfer_bins[count_col] / xfer_bins[count_col].sum()

    total_ops = int(xfer_bins[count_col].sum())

    detail_list = []
    for xfer, row in xfer_bins.iterrows():
        detail_list.append(f"{xfer} - {int(row[count_col]):,} ops ({row['per'] * 100:.2f}%)")

    return min_val, max_val, total_ops, detail_list


@delayed
def _process_bott_metadata_access(
    metric: str,
    rule: Rule,
    view_key: ViewKey,
    high_level_view: pd.DataFrame,
    mid_level_view: pd.DataFrame,
    low_level_view: pd.DataFrame,
    threshold: float,
    deps: Dict[Rule, RuleResult],
):
    view_type = view_key[-1]

    max_io_time = deps[Rule.CHAR_IO_TIME].value

    results = {}
    # Run through high level view
    for row in high_level_view.query(f"duration_th >= {threshold}").itertuples():

        ix, description = _describe_bottleneck(
            row=row,
            view_type=view_type,
            low_level_view=low_level_view,
            max_io_time=max_io_time
        )

        md_time_ratio = row.metadata_time / row.duration_sum

        # Check metadata time ratio
        if md_time_ratio > METADATA_ACCESS_RATIO_THRESHOLD:

            reasons = []

            md_ops = {}
            for md_op in DERIVED_MD_OPS:
                md_op_key = f"{md_op}_time"
                md_ops[md_op_key] = getattr(row, md_op_key)

            max_md_op = max(md_ops, key=md_ops.get)
            max_md_op_time = md_ops[max_md_op]
            max_md_op_ratio = max_md_op_time / row.duration_sum

            reasons.append(RuleReason(
                description=(
                    f"Overall {md_time_ratio * 100:.2f}% ({row.metadata_time:.2f} seconds) of I/O time is spent on metadata access, "
                    f"specifically {max_md_op_ratio * 100:.2f}% ({max_md_op_time:.2f} seconds) on the '{max_md_op.replace('_time', '')}' operation."
                ),
                value=max_md_op_ratio,
            ))

            ix_low_level_view = low_level_view.loc[[ix]]

            file_based_level = next((l for l in ix_low_level_view.index.names if l.startswith('file_')))
            files = list(ix_low_level_view.index.unique(level=file_based_level))

            if all(['/gpfs' in file for file in files]) and 'open' in max_md_op:
                reasons.append(RuleReason(
                    description=f"All files are stored on GPFS and 'open's become costly without other I/O operations.",
                    value=None,
                ))

            data_dict = _get_data_dict(
                row=row,
                view_type=view_type,
                low_level_view=low_level_view
            )

            results[ix] = RuleResult(
                data_dict=data_dict,
                description=description,
                detail_list=None,
                reasons=reasons,
                rule=rule,
                value=md_time_ratio,
                value_fmt=f"{md_time_ratio*100:.2f}%",
            )

    return metric, rule, view_key, results


def _process_bott_small_access(
    metric: str,
    rule: Rule,
    view_key: ViewKey,
    high_level_view: pd.DataFrame,
    mid_level_view: pd.DataFrame,
    low_level_view: pd.DataFrame,
    threshold: float,
    access_type: str,
    access_ratio_threshold: float,
    access_size_threshold: float,
    max_io_time: float,
):
    view_type = view_key[-1]

    results = {}
    # Run through high level view
    for row in high_level_view.query(f"duration_th >= {threshold}").itertuples():

        ix, description = _describe_bottleneck(
            row=row,
            view_type=view_type,
            low_level_view=low_level_view,
            max_io_time=max_io_time
        )

        avg_size = row.size_sum / row.index_count
        access_time = getattr(row, f"{access_type}_time")
        access_time_ratio = access_time / row.duration_sum

        if access_time_ratio > access_ratio_threshold and avg_size < access_size_threshold:

            reasons = []

            reasons.append(RuleReason(
                description=f"'{access_type}' time is {access_time_ratio*100:.2f}% ({access_time:.2f} seconds) of I/O time.",
                value=access_time_ratio,
            ))

            reasons.append(RuleReason(
                description=(
                    f"Average I/O accesses are {convert_bytes_to_unit(avg_size, 'KB'):.2f} KB, "
                    f"which is smaller than {convert_bytes_to_unit(access_size_threshold, 'KB')} KB"
                ),
                value=avg_size,
            ))

            data_dict = _get_data_dict(
                row=row,
                view_type=view_type,
                low_level_view=low_level_view
            )

            results[ix] = RuleResult(
                data_dict=data_dict,
                description=description,
                detail_list=None,
                reasons=reasons,
                rule=rule,
                value=access_time_ratio,
                value_fmt=f"{access_time_ratio*100:.2f}%",
            )

    return metric, rule, view_key, results


@delayed
def _process_bott_small_reads(
    metric: str,
    rule: Rule,
    view_key: ViewKey,
    high_level_view: pd.DataFrame,
    mid_level_view: pd.DataFrame,
    low_level_view: pd.DataFrame,
    threshold: float,
    deps: Dict[Rule, RuleResult],
):
    return _process_bott_small_access(
        metric=metric,
        rule=rule,
        view_key=view_key,
        high_level_view=high_level_view,
        mid_level_view=mid_level_view,
        low_level_view=low_level_view,
        threshold=threshold,
        access_type='read',
        access_ratio_threshold=SMALL_READ_RATIO_THRESHOLD,
        access_size_threshold=SMALL_READ_SIZE_THRESHOLD,
        max_io_time=deps[Rule.CHAR_IO_TIME].value,
    )


@delayed
def _process_bott_small_writes(
    metric: str,
    rule: Rule,
    view_key: ViewKey,
    high_level_view: pd.DataFrame,
    mid_level_view: pd.DataFrame,
    low_level_view: pd.DataFrame,
    threshold: float,
    deps: Dict[Rule, RuleResult],
):
    return _process_bott_small_access(
        metric=metric,
        rule=rule,
        view_key=view_key,
        high_level_view=high_level_view,
        mid_level_view=mid_level_view,
        low_level_view=low_level_view,
        threshold=threshold,
        access_type='write',
        access_ratio_threshold=SMALL_WRITE_RATIO_THRESHOLD,
        access_size_threshold=SMALL_WRITE_SIZE_THRESHOLD,
        max_io_time=deps[Rule.CHAR_IO_TIME].value,
    )


@delayed
def _process_char_access_pattern(rule: Rule, view: pd.DataFrame) -> RuleResult:
    acc_pat_cols = []
    for acc_pat in list(AccessPattern):
        for col_suffix in ACC_PAT_SUFFIXES:
            col_name = f"{acc_pat.name.lower()}_{col_suffix}"
            acc_pat_cols.append(col_name)

    acc_pat_summary = view[acc_pat_cols].sum()

    sequential_count = int(acc_pat_summary['sequential_count'])
    random_count = int(acc_pat_summary['random_count'])
    total_count = sequential_count + random_count

    result = RuleResult(
        data_dict=dict(acc_pat_summary),
        description='Access Pattern',
        detail_list=None,
        reasons=None,
        rule=rule,
        value=None,
        value_fmt=(
            f"{sequential_count/total_count*100:.2f}% Sequential - "
            f"{random_count/total_count*100:.2f}% Random"
        )
    )
    return result


@delayed
def _process_char_io_time(rule: Rule, view: pd.DataFrame) -> RuleResult:
    max_io_time = compute_max_io_time(main_view=view, time_col='duration_sum')

    detail_list = []
    for io_type in IO_TYPES:
        time_col = f"{io_type}_time"
        time = compute_max_io_time(main_view=view, time_col=time_col)
        detail_list.append(f"{io_type.capitalize()} - {time:.2f} seconds ({time/max_io_time*100:.2f}%)")

    result = RuleResult(
        data_dict=None,
        description='I/O Time',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=max_io_time,
        value_fmt=f"{max_io_time:.2f} seconds",
    )
    return result


@delayed
def _process_char_io_count(rule: Rule, view: pd.DataFrame) -> RuleResult:
    total_count = int(view['index_count'].sum())

    detail_list = []
    for io_type in IO_TYPES:
        count_col = f"{io_type}_count"
        count = int(view[count_col].sum())
        detail_list.append(f"{io_type.capitalize()} - {count:,} ops ({count/total_count*100:.2f}%)")

    result = RuleResult(
        data_dict=None,
        description='I/O Ops',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=total_count,
        value_fmt=f"{total_count:,} ops",
    )
    return result


@delayed
def _process_char_io_size(rule: Rule, view: pd.DataFrame) -> RuleResult:
    total_size = view['size_sum'].sum()

    detail_list = []
    for io_type in IO_TYPES:
        if io_type != 'metadata':
            size_col = f"{io_type}_size"
            size = view[size_col].sum()
            detail_list.append((
                f"{io_type.capitalize()} - "
                f"{convert_bytes_to_unit(size, 'GB'):.2f} GB "
                f"({size/total_size*100:.2f}%)"
            ))

    result = RuleResult(
        data_dict=None,
        description='I/O Size',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=total_size,
        value_fmt=f"{convert_bytes_to_unit(total_size, 'GB'):.2f} GB",
    )
    return result


@delayed
def _process_char_file_count(rule: Rule, view: pd.DataFrame) -> RuleResult:
    file_count = len(view.index.unique(COL_FILE_NAME))

    nunique_file_per_proc = view \
        .reset_index() \
        .groupby([COL_PROC_NAME]) \
        .agg({COL_FILE_NAME: 'nunique'})

    fpp_count = int(nunique_file_per_proc[nunique_file_per_proc[COL_FILE_NAME] == 1][COL_FILE_NAME].count())
    shared_count = file_count - fpp_count

    detail_list = []
    detail_list.append(f"Shared: {shared_count} files ({shared_count/file_count*100:.2f}%)")
    detail_list.append(f"FPP: {fpp_count} files ({fpp_count/file_count*100:.2f}%)")

    result = RuleResult(
        data_dict=None,
        description='Files',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=file_count,
        value_fmt=f"{file_count} files",
    )
    return result


@delayed
def _process_char_read_xfer_size(rule: Rule, view: pd.DataFrame) -> RuleResult:
    min_val, max_val, total_ops, detail_list = _get_xfer_dist(view=view, io_op='read')
    value_fmt = _get_xfer_size(max_val)
    if min_val > 0 and max_val > 0 and min_val != max_val:
        value_fmt = f"{_get_xfer_size(min_val)}-{_get_xfer_size(max_val)}"
    value_fmt = f"{value_fmt} - {total_ops:,} ops"
    result = RuleResult(
        data_dict=None,
        description='Read Xfer',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=(min_val, max_val),
        value_fmt=value_fmt,
    )
    return result


@delayed
def _process_char_write_xfer_size(rule: Rule, view: pd.DataFrame) -> RuleResult:
    min_val, max_val, total_ops, detail_list = _get_xfer_dist(view=view, io_op='write')
    value_fmt = _get_xfer_size(max_val)
    if min_val > 0 and max_val > 0 and min_val != max_val:
        value_fmt = f"{_get_xfer_size(min_val)}-{_get_xfer_size(max_val)}"
    value_fmt = f"{value_fmt} - {total_ops:,} ops"
    result = RuleResult(
        data_dict=None,
        description='Write Xfer',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=(min_val, max_val),
        value_fmt=value_fmt,
    )
    return result


@delayed
def _process_char_app_count(rule: Rule, view: pd.DataFrame, deps: Dict[Rule, RuleResult]):

    max_io_time = deps[Rule.CHAR_IO_TIME].value
    total_ops = deps[Rule.CHAR_IO_COUNT].value
    total_size = deps[Rule.CHAR_IO_SIZE].value

    app_col = 'app_name'

    apps = view \
        .reset_index() \
        .assign(
            proc_name_parts=lambda df: df[COL_PROC_NAME].str.split('#'),
            app_name=lambda df: df.proc_name_parts.str[0]
        ) \
        .groupby([app_col, COL_PROC_NAME]) \
        .agg({
            'index_count': sum,
            'duration_sum': sum,
            'read_size': sum,
            'write_size': sum,
        }) \
        .groupby([app_col]) \
        .agg({
            'index_count': sum,
            'duration_sum': max,
            'read_size': sum,
            'write_size': sum,
        }) \
        .sort_values('duration_sum', ascending=False)

    detail_list = []
    for app, row in apps.iterrows():
        read_size = row['read_size']
        write_size = row['write_size']
        read_size_gb = convert_bytes_to_unit(read_size, 'GB')
        write_size_gb = convert_bytes_to_unit(write_size, 'GB')
        read_size_per = read_size / total_size * 100
        write_size_per = write_size / total_size * 100
        detail_list.append(' - '.join([
            app,
            f"{row['duration_sum']:.2f} s ({row['duration_sum'] / max_io_time * 100:.2f}%)",
            f"{read_size_gb:.2f}/{write_size_gb:.2f} GB R/W ({read_size_per:.2f}/{write_size_per:.2f}%)",
            f"{int(row['index_count']):,} ops ({row['index_count'] / total_ops * 100:.2f}%)"
        ]))

    result = RuleResult(
        data_dict=None,
        description='Apps',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=len(apps),
        value_fmt=f"{len(apps)} apps",
    )

    return result


@delayed
def _process_char_node_count(rule: Rule, view: pd.DataFrame, deps: Dict[Rule, RuleResult]):

    max_io_time = deps[Rule.CHAR_IO_TIME].value
    total_ops = deps[Rule.CHAR_IO_COUNT].value
    total_size = deps[Rule.CHAR_IO_SIZE].value

    node_col = 'node_name'

    nodes = view \
        .reset_index() \
        .assign(
            proc_name_parts=lambda df: df[COL_PROC_NAME].str.split('#'),
            node_name=lambda df: df.proc_name_parts.str[1]
        ) \
        .groupby([node_col, COL_PROC_NAME]) \
        .agg({
            'index_count': sum,
            'duration_sum': sum,
            'read_size': sum,
            'write_size': sum,
        }) \
        .groupby([node_col]) \
        .agg({
            'index_count': sum,
            'duration_sum': max,
            'read_size': sum,
            'write_size': sum,
        }) \
        .sort_values('duration_sum', ascending=False)

    detail_list = []
    for node, row in nodes.iterrows():
        read_size = row['read_size']
        write_size = row['write_size']
        read_size_gb = convert_bytes_to_unit(read_size, 'GB')
        write_size_gb = convert_bytes_to_unit(write_size, 'GB')
        read_size_per = read_size / total_size * 100
        write_size_per = write_size / total_size * 100
        detail_list.append(' - '.join([
            node,
            f"{row['duration_sum']:.2f} s ({row['duration_sum'] / max_io_time * 100:.2f}%)",
            f"{read_size_gb:.2f}/{write_size_gb:.2f} GB R/W ({read_size_per:.2f}/{write_size_per:.2f}%)",
            f"{int(row['index_count']):,} ops ({row['index_count'] / total_ops * 100:.2f}%)"
        ]))

    result = RuleResult(
        data_dict=None,
        description='Nodes',
        detail_list=detail_list,
        reasons=None,
        rule=rule,
        value=len(nodes),
        value_fmt=f"{len(nodes)} nodes",
    )

    return result


BOTTLENECK_FUNCTIONS = {
    Rule.BOTT_METADATA_ACCESS: (_process_bott_metadata_access, [Rule.CHAR_IO_TIME]),
    Rule.BOTT_SMALL_READS: (_process_bott_small_reads, [Rule.CHAR_IO_TIME]),
    Rule.BOTT_SMALL_WRITES: (_process_bott_small_writes, [Rule.CHAR_IO_TIME]),
}

CHARACTERISTIC_FUNCTIONS = {
    Rule.CHAR_IO_TIME: (_process_char_io_time, None),
    Rule.CHAR_IO_COUNT: (_process_char_io_count, None),
    Rule.CHAR_IO_SIZE: (_process_char_io_size, None),
    Rule.CHAR_READ_XFER_SIZE: (_process_char_read_xfer_size, None),
    Rule.CHAR_WRITE_XFER_SIZE: (_process_char_write_xfer_size, None),
    Rule.CHAR_APP_COUNT: (_process_char_app_count, [Rule.CHAR_IO_COUNT, Rule.CHAR_IO_SIZE, Rule.CHAR_IO_TIME]),
    Rule.CHAR_NODE_COUNT: (_process_char_node_count, [Rule.CHAR_IO_COUNT, Rule.CHAR_IO_SIZE, Rule.CHAR_IO_TIME]),
    Rule.CHAR_FILE_COUNT: (_process_char_file_count, None),
    Rule.CHAR_ACCESS_PATTERN: (_process_char_access_pattern, None),
}


class RecorderRuleEngine(RuleEngine):

    def __init__(self, rules: Dict[ViewType, List[Rule]]) -> None:
        super().__init__(rules)

    def process_characteristics(self, view: dd.DataFrame) -> Dict[Rule, RuleResult]:
        # Keep rule tasks
        rule_tasks = {}
        # Run through characteristics rules
        for rule, (rule_func, rule_deps) in CHARACTERISTIC_FUNCTIONS.items():
            if rule_deps is None:
                rule_tasks[rule] = rule_func(rule=rule, view=view)
            else:
                rule_tasks[rule] = rule_func(
                    rule=rule,
                    view=view,
                    deps={rule_dep: rule_tasks[rule_dep] for rule_dep in rule_deps}
                )
        # Compute tasks
        rule_results = compute(*list(rule_tasks.values()))
        # Create characteristics
        characteristics = {}
        for result in rule_results:
            characteristics[result.rule] = result
        # Return characteristics
        return characteristics

    def process_bottlenecks(
        self,
        bottlenecks: Dict[str, Dict[ViewKey, Dict[str, dd.DataFrame]]],
        characteristics: Dict[Rule, RuleResult],
        threshold=0.5
    ) -> Dict[ViewKey, object]:
        # Keep rule tasks
        rule_tasks = []
        # Run through bottlenecks
        for metric, bottleneck_views in bottlenecks.items():
            for view_key, bottleneck_view in bottleneck_views.items():
                # Get view type
                view_type = view_key[-1]

                # Check if rule is defined for view type
                if view_type not in self.rules:
                    continue

                # If so, run through rules
                for rule in self.rules[view_type]:
                    rule_func, rule_deps = BOTTLENECK_FUNCTIONS[rule]
                    if rule_deps is None:
                        rule_tasks.append(rule_func(
                            metric=metric,
                            rule=rule,
                            view_key=view_key,
                            high_level_view=bottleneck_view['high_level_view'],
                            mid_level_view=bottleneck_view['mid_level_view'],
                            low_level_view=bottleneck_view['low_level_view'],
                            threshold=threshold,
                        ))
                    else:
                        rule_tasks.append(rule_func(
                            metric=metric,
                            rule=rule,
                            view_key=view_key,
                            high_level_view=bottleneck_view['high_level_view'],
                            mid_level_view=bottleneck_view['mid_level_view'],
                            low_level_view=bottleneck_view['low_level_view'],
                            threshold=threshold,
                            deps={rule_dep: characteristics[rule_dep] for rule_dep in rule_deps}
                        ))
        # Compute tasks
        rule_results = compute(*rule_tasks)
        # Create bottlenecks
        bottlenecks = {}
        for metric, rule, view_key, result in rule_results:
            bottlenecks[metric] = bottlenecks[metric] if metric in bottlenecks else {}
            bottlenecks[metric][view_key] = bottlenecks[metric][view_key] if view_key in bottlenecks[metric] else {}
            bottlenecks[metric][view_key][rule] = result
        # Return bottlenecks
        return bottlenecks
