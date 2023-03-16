import dask.dataframe as dd
import pandas as pd
from dask import compute, delayed
from typing import Dict, List
from ..base import ViewKey, ViewType
from ..rules import Rule, RuleEngine, RuleReason, RuleResult
from ..utils.collection_utils import get_intervals, join_with_and
from .analysis import DERIVED_MD_OPS


METADATA_ACCESS_RATIO_THRESHOLD = 0.5
SMALL_ACCESS_SIZE_THRESHOLD = 256 * 1024
SMALL_ACCESS_RATIO_THRESHOLD = 0.75


@delayed
def _process_metadata_access_issue(
    rule: Rule,
    view_key: ViewKey,
    high_level_view: pd.DataFrame,
    mid_level_view: pd.DataFrame,
    low_level_view: pd.DataFrame,
    threshold: float,
):
    view_type = view_key[-1]
    results = {}
    # Run through high level view
    for ix, row in high_level_view.query(f"duration_th >= {threshold}").iterrows():

        total_time, metadata_time = row['duration_sum'], row['metadata_time']
        md_time_ratio = metadata_time / total_time

        # Check metadata time ratio
        if md_time_ratio > METADATA_ACCESS_RATIO_THRESHOLD:

            if view_type == 'file_name':

                processes = list(low_level_view.loc[[ix]].index.unique(level='proc_name'))
                tranges = list(low_level_view.loc[[ix]].index.unique(level='trange'))
                trange_intervals = get_intervals(values=tranges)

                md_ops = {}
                for md_op in DERIVED_MD_OPS:
                    md_op_key = f"{md_op}_time"
                    md_ops[md_op_key] = row[md_op_key]

                max_md_op = max(md_ops, key=md_ops.get)
                max_md_op_time = md_ops[max_md_op]
                max_md_op_ratio = max_md_op_time / total_time

                description = (
                    f"'{ix}' is accessed by {len(processes)} process(es) "
                    f"during the {join_with_and(values=trange_intervals)}th second(s) "
                    f"and spent {max_md_op_ratio * 100:.2f}% ({max_md_op_time:.2f} seconds) of its I/O time "
                    f"on the '{max_md_op.replace('_time', '')}' operation(s)."
                )

                results[ix] = RuleResult(
                    rule=rule,
                    description=description,
                    reasons=[
                        RuleReason(
                            description=f"Metadata time is {md_time_ratio * 100:.2f}% ({metadata_time:.2f} seconds) of I/O time",
                            value=md_time_ratio,
                        )
                    ]
                )

            elif view_type == 'proc_name':

                files = list(low_level_view.loc[[ix]].index.unique(level='file_name'))
                tranges = list(low_level_view.loc[[ix]].index.unique(level='trange'))
                trange_intervals = get_intervals(values=tranges)

                open_time = row['open_time']

                if open_time == total_time:

                    description = (
                        f"'{ix}' accesses {len(files)} file(s) "
                        f"during the {join_with_and(values=trange_intervals)}th second(s) "
                        f"and spent its all I/O time on the 'open' operation(s)."
                    )

                    reasons = []
                    if all(['/gpfs' in file for file in files]):
                        reasons.append(RuleReason(
                            description=f"All files are stored on GPFS and 'open's become costly without other I/O operations.",
                            value=None,
                        ))

                    results[ix] = RuleResult(
                        rule=rule,
                        description=description,
                        reasons=reasons,
                    )

    return rule, view_key, results


@delayed
def _process_small_io_access(
    rule: Rule,
    view_key: ViewKey,
    high_level_view: pd.DataFrame,
    mid_level_view: pd.DataFrame,
    low_level_view: pd.DataFrame,
    threshold: float,
):
    view_type = view_key[-1]
    results = {}
    # Run through high level view
    for ix, row in high_level_view.query(f"duration_th >= {threshold}").iterrows():

        total_time, read_time, write_time = row['duration_sum'], row['read_time'], row['write_time']
        read_time_ratio = read_time / total_time
        write_time_ratio = write_time / total_time

        if write_time_ratio > SMALL_ACCESS_RATIO_THRESHOLD or read_time_ratio > SMALL_ACCESS_RATIO_THRESHOLD:

            low_level_rows = low_level_view.loc[[ix]].query(f'size_max < {SMALL_ACCESS_SIZE_THRESHOLD}')
            max_xfer_size = low_level_rows['size_max'].max()

            if view_type == 'file_name':

                processes = list(low_level_rows.index.unique(level='proc_name'))
                processes_fmt = ["'{}'".format(p) for p in processes]
                tranges = list(low_level_rows.index.unique(level='trange'))
                trange_intervals = get_intervals(values=tranges)

                description = (
                    f"'{ix}' is accessed by process(es) {join_with_and(processes_fmt)} "
                    f"during the {join_with_and(values=trange_intervals)}th second(s) "
                    f"with a transfer size smaller than 256KB ({max_xfer_size / 1024:.2f}KB)."
                )

                reasons = []
                if read_time_ratio > SMALL_ACCESS_RATIO_THRESHOLD:
                    reasons.append(RuleReason(
                        description=f"'read' time is {read_time_ratio * 100:.2f}% ({read_time:.2f} seconds) of I/O time.",
                        value=read_time_ratio,
                    ))
                elif write_time_ratio > SMALL_ACCESS_RATIO_THRESHOLD:
                    reasons.append(RuleReason(
                        description=f"'write' time is {write_time_ratio * 100:.2f}% ({write_time:.2f} seconds) of I/O time.",
                        value=write_time_ratio,
                    ))

                results[ix] = RuleResult(
                    rule=rule,
                    description=description,
                    reasons=reasons,
                )

    return rule, view_key, results


class RecorderRuleEngine(RuleEngine):

    def __init__(self, rules: Dict[ViewType, List[Rule]]) -> None:
        super().__init__(rules)

    def process_bottlenecks(self, bottlenecks: Dict[ViewKey, Dict[str, dd.DataFrame]], threshold=0.5) -> Dict[ViewKey, object]:
        # Keep rule tasks
        rule_tasks = []
        # Run through bottlenecks
        for view_key, bottleneck in bottlenecks.items():
            # Get view type
            view_type = view_key[-1]
            # Run through rules
            for rule in self.rules[view_type]:
                if rule is Rule.METADATA_ACCESS_ISSUE:
                    rule_tasks.append(_process_metadata_access_issue(
                        rule=rule,
                        view_key=view_key,
                        high_level_view=bottleneck['high_level_view'],
                        mid_level_view=bottleneck['mid_level_view'],
                        low_level_view=bottleneck['low_level_view'],
                        threshold=threshold,
                    ))
                elif rule is Rule.SMALL_IO_ACCESS:
                    rule_tasks.append(_process_small_io_access(
                        rule=rule,
                        view_key=view_key,
                        high_level_view=bottleneck['high_level_view'],
                        mid_level_view=bottleneck['mid_level_view'],
                        low_level_view=bottleneck['low_level_view'],
                        threshold=threshold,
                    ))

        rule_results = compute(*rule_tasks)

        return rule_results
