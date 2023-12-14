import abc
import dask.dataframe as dd
import inflect
import jinja2
import numpy as np
import pandas as pd
from dask.delayed import Delayed
from enum import Enum
from scipy.cluster.hierarchy import linkage, fcluster
from typing import Dict, List, Union

from .analysis_utils import set_proc_name_parts
from .bottlenecks import BOTTLENECK_ORDER
from .constants import (
    ACC_PAT_SUFFIXES,
    COL_APP_NAME,
    COL_FILE_DIR,
    COL_FILE_NAME,
    COL_FILE_PATTERN,
    COL_NODE_NAME,
    COL_PROC_NAME,
    COL_RANK,
    COL_TIME_RANGE,
    HUMANIZED_VIEW_TYPES,
    IO_TYPES,
    XFER_SIZE_BINS,
    XFER_SIZE_BIN_LABELS,
    XFER_SIZE_BIN_NAMES,
    AccessPattern,
)
from .types import (
    BottleneckResult,
    Metric,
    Rule,
    RuleReason,
    RuleResult,
    RuleResultReason,
    ViewKey,
)
from .utils.collection_utils import get_intervals
from .utils.common_utils import convert_bytes_to_unit


METADATA_ACCESS_RATIO_THRESHOLD = 0.5


class KnownCharacteristics(Enum):
    ACCESS_PATTERN = 'access_pattern'
    APP_COUNT = 'app_count'
    FILE_COUNT = 'file_count'
    IO_COUNT = 'io_count'
    IO_SIZE = 'io_size'
    IO_TIME = 'io_time'
    NODE_COUNT = 'node_count'
    PROC_COUNT = 'proc_count'
    READ_XFER_SIZE = 'read_xfer_size'
    WRITE_XFER_SIZE = 'write_xfer_size'


class KnownRules(Enum):
    EXCESSIVE_METADATA_ACCESS = 'excessive_metadata_access'
    LOW_BANDWIDTH = 'low_bandwidth'
    OPERATION_IMBALANCE = 'operation_imbalance'
    RANDOM_OPERATIONS = 'random_operations'
    SIZE_IMBALANCE = 'size_imbalance'
    SMALL_READS = 'small_reads'
    SMALL_WRITES = 'small_writes'


KNOWN_RULES = {
    KnownRules.EXCESSIVE_METADATA_ACCESS.value: Rule(
        name='Excessive metadata access',
        condition='{metric}_threshold >= {threshold} & (metadata_time / time) >= 0.5',
        reasons=[
            RuleReason(
                condition='(open_time > close_time) & (open_time > seek_time)',
                message='''
Overall {{ "%.2f" | format((metadata_time / time) * 100) }}% ({{ "%.2f" | format(metadata_time) }} seconds) of I/O time is spent on metadata access, \
specifically {{ "%.2f" | format((open_time / time) * 100) }}% ({{ "%.2f" | format(open_time) }} seconds) on the 'open' operation.
                '''
            ),
            RuleReason(
                condition='(close_time > open_time) & (close_time > seek_time)',
                message='''
Overall {{ "%.2f" | format((metadata_time / time) * 100) }}% ({{ "%.2f" | format(metadata_time) }} seconds) of I/O time is spent on metadata access, \
specifically {{ "%.2f" | format((open_time / time) * 100) }}% ({{ "%.2f" | format(open_time) }} seconds) on the 'close' operation.
                '''
            ),
            RuleReason(
                condition='(seek_time > open_time) & (seek_time > close_time)',
                message='''
Overall {{ "%.2f" | format((metadata_time / time) * 100) }}% ({{ "%.2f" | format(metadata_time) }} seconds) of I/O time is spent on metadata access, \
specifically {{ "%.2f" | format((open_time / time) * 100) }}% ({{ "%.2f" | format(open_time) }} seconds) on the 'seek' operation.
                '''
            ),
        ]
    ),
    KnownRules.OPERATION_IMBALANCE.value: Rule(
        name='Operation imbalance',
        condition='{metric}_threshold >= {threshold} & (abs(write_count - read_count) / count) > 0.1',
        reasons=[
            RuleReason(
                condition='read_count > write_count',
                message='''
'read' operations are {{ "%.2f" | format((read_count / count) * 100) }}% ({{read_count}} operations) of total I/O operations.
                '''
            ),
            RuleReason(
                condition='write_count > read_count',
                message='''
'write' operations are {{ "%.2f" | format((write_count / count) * 100) }}% ({{write_count}} operations) of total I/O operations.
                '''
            ),
        ]
    ),
    KnownRules.RANDOM_OPERATIONS.value: Rule(
        name='Random operations',
        condition='{metric}_threshold >= {threshold} & random_count / count > 0.5',
        reasons=[
            RuleReason(
                condition='random_count / count > 0.5',
                message='''
Issued high number of random operations, specifically {{ "%.2f" | format((random_count / count) * 100) }}% \
({{ random_count }} operations) of total I/O operations.
                '''
            ),
        ]
    ),
    KnownRules.SIZE_IMBALANCE.value: Rule(
        name='Size imbalance',
        condition='{metric}_threshold >= {threshold} & size > 0 & (abs(write_size - read_size) / size) > 0.1',
        reasons=[
            RuleReason(
                condition='read_size > write_size',
                message='''
'read' size is {{ "%.2f" | format((read_size / size) * 100) }}% ({{ "%.2f" | format(read_size / 1024 / 1024) }} MB) of total I/O size.
                '''
            ),
            RuleReason(
                condition='write_size > read_size',
                message='''
'write' size is {{ "%.2f" | format((write_size / size) * 100) }}% ({{ "%.2f" | format(write_size / 1024 / 1024) }} MB) of total I/O size.
                '''
            ),
        ]
    ),
    KnownRules.SMALL_READS.value: Rule(
        name='Small reads',
        condition='{metric}_threshold >= {threshold} & (read_time / time) > 0.5 & (read_size / count) < 262144',
        reasons=[
            RuleReason(
                condition='(read_time / time) > 0.5',
                message='''
'read' time is {{ "%.2f" | format((read_time / time) * 100) }}% ({{ "%.2f" | format(read_time) }} seconds) of I/O time.
                '''
            ),
            RuleReason(
                condition='(read_size / count) < 262144',
                message='''
Average 'read's are {{ "%.2f" | format(read_size / count / 1024) }} KB, which is smaller than 256 KB.
                '''
            )
        ]
    ),
    KnownRules.SMALL_WRITES.value: Rule(
        name='Small writes',
        condition='{metric}_threshold >= {threshold} & (write_time / time) > 0.5 & (write_size / count) < 262144',
        reasons=[
            RuleReason(
                condition='(write_time / time) > 0.5',
                message='''
'write' time is {{ "%.2f" | format((write_time / time) * 100) }}% ({{ "%.2f" | format(write_time) }} seconds) of I/O time.
                '''
            ),
            RuleReason(
                condition='(write_size / count) < 262144',
                message='''
Average 'write's are {{ "%.2f" | format(write_size / count / 1024) }} KB, which is smaller than 256 KB
                '''
            )
        ]
    ),
}


class RuleHandler(abc.ABC):

    def __init__(self, rule_key: str) -> None:
        super().__init__()
        self.rule_key = rule_key


class BottleneckRule(RuleHandler):

    def __init__(self, rule_key: str, rule: Rule) -> None:
        super().__init__(rule_key=rule_key)
        self.rule = rule
        self.pluralize = inflect.engine()

    def define_tasks(
        self,
        metric: Metric,
        metric_boundary: dd.core.Scalar,
        view_key: ViewKey,
        bottleneck_result: BottleneckResult,
        characteristics: Dict[str, RuleResult] = None,
        threshold: float = 0.5,
    ) -> Dict[str, Delayed]:

        view_type = view_key[-1]

        # takes around 0.015 seconds
        bottlenecks = bottleneck_result.bottlenecks \
            .query(self.rule.condition.format(metric=metric, threshold=threshold))

        # takes around 0.02 seconds
        details = bottleneck_result.details \
            .query(f"{view_type} in @indices", local_dict={'indices': bottlenecks.index})

        tasks = {}
        tasks['bottlenecks'] = bottlenecks
        tasks['details'] = details
        tasks['metric_boundary'] = metric_boundary

        for i, reason in enumerate(self.rule.reasons):
            # takes around 0.005 seconds
            tasks[f"reason{i}"] = bottlenecks.eval(reason.condition)

        return tasks

    def handle_task_results(
        self,
        metric: Metric,
        view_key: ViewKey,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> List[RuleResult]:
        view_type = view_key[-1]

        bottlenecks = result['bottlenecks']
        details = result['details']
        metric_boundary = result['metric_boundary']

        reasoning = {}
        reasoning_templates = {}
        for i, reason in enumerate(self.rule.reasons):
            reasoning[i] = result[f"reason{i}"].to_dict()
            reasoning_templates[i] = jinja2.Template(reason.message)

        results = []

        if len(bottlenecks) == 0:
            return {}

        # similar_bottlenecks = self._group_similar_behavior(
        #     bottlenecks=bottlenecks,
        #     metric=metric,
        #     view_type=view_type,
        # )

        for ix, row in bottlenecks.iterrows():

            unioned_details = self._union_details(
                details=details,
                behavior=1,
                indices=[ix],
                view_type=view_type,
            )

            description, times, processes, files = self._describe_bottleneck(
                metric=metric,
                metric_boundary=metric_boundary,
                view_type=view_type,
                row=row,
                unioned_details=unioned_details,
            )

            extra_data = dict(row)
            extra_data['file_name'] = files
            extra_data['proc_name'] = processes
            extra_data['time_range'] = times

            reasons = []
            for i, reason in enumerate(self.rule.reasons):
                if reasoning[i][ix]:
                    reasons.append(RuleResultReason(
                        description=reasoning_templates[i].render(row).strip()
                    ))

            result = RuleResult(
                description=description,
                detail_list=None,
                extra_data=extra_data,
                reasons=reasons,
                value=0,
                value_fmt='',
            )

            results.append(result)

        return results

    def _describe_bottleneck(
        self,
        metric: Metric,
        metric_boundary: Union[int, float],
        view_type: str,
        row: dict,
        unioned_details: pd.DataFrame,
    ) -> str:

        files = []
        processes = []
        times = []

        unioned_dict = unioned_details.to_dict(orient='index')[0]
        unioned_dict[COL_FILE_NAME] = unioned_dict.get(COL_FILE_NAME, set())
        unioned_dict[COL_PROC_NAME] = unioned_dict.get(COL_PROC_NAME, set())
        unioned_dict[COL_TIME_RANGE] = unioned_dict.get(COL_TIME_RANGE, set())

        if view_type in [COL_FILE_NAME, COL_FILE_DIR, COL_FILE_PATTERN]:
            files = list(sorted(unioned_dict[view_type]))
            processes = list(sorted(unioned_dict[COL_PROC_NAME]))
            times = list(sorted(unioned_dict[COL_TIME_RANGE]))
        elif view_type in [COL_APP_NAME, COL_NODE_NAME, COL_PROC_NAME, COL_RANK]:
            files = list(sorted(unioned_dict[COL_FILE_NAME]))
            processes = list(sorted(unioned_dict[view_type]))
            times = list(sorted(unioned_dict[COL_TIME_RANGE]))
        else:
            files = list(sorted(unioned_dict[COL_FILE_NAME]))
            processes = list(sorted(unioned_dict[COL_PROC_NAME]))
            times = list(sorted(unioned_dict[view_type]))

        value = getattr(row, metric)

        if len(files) > 0 and len(processes) > 0 and len(times) > 0:

            time_intervals = get_intervals(values=times)

            accessor = HUMANIZED_VIEW_TYPES['proc_name'].lower()
            accessed = HUMANIZED_VIEW_TYPES['file_name'].lower()
            if view_type in [COL_FILE_NAME, COL_FILE_DIR, COL_FILE_PATTERN]:
                accessed = HUMANIZED_VIEW_TYPES[view_type].lower()
            if view_type in [COL_APP_NAME, COL_NODE_NAME, COL_PROC_NAME, COL_RANK]:
                accessor = HUMANIZED_VIEW_TYPES[view_type].lower()

            description = (
                f"{len(processes)} {self.pluralize.plural_noun(accessor, len(processes))} "
                f"{self.pluralize.plural_verb('accesses', len(processes))} {len(files)} {self.pluralize.plural_noun(accessed, len(files))} "
                f"within {len(time_intervals)} {self.pluralize.plural_noun('time period', len(time_intervals))} "
                f"and {self.pluralize.plural_verb('has', len(processes))} an I/O time of {value:.2f} seconds which is "
                f"{value/metric_boundary*100:.2f}% of overall I/O time of the workload."
            )

            # description = (
            #     f"{self.pluralize.join(processes)} {self.pluralize.plural_verb('access', len(processes))} "
            #     f"{self.pluralize.plural_noun('file', len(files))} {self.pluralize.join(files)} "
            #     f"during the {join_with_and(values=time_intervals)}th {self.pluralize.plural_noun('second', len(time_intervals))} "
            #     f"and {self.pluralize.plural_verb('has', len(processes))} an I/O time of {value:.2f} seconds which is "
            #     f"{value/metric_boundary*100:.2f}% of overall I/O time of the workload."
            # )

        else:

            accessor = HUMANIZED_VIEW_TYPES[view_type].lower()
            count = len(unioned_dict[view_type])

            description = (
                f"{count} {self.pluralize.plural_noun(accessor, count)} "
                f"{self.pluralize.plural_verb('has', count)} an I/O time of "
                f"{value:.2f} seconds which is {value/metric_boundary*100:.2f}% of overall I/O time of the workload."
            )

        return description, times, processes, files

    @staticmethod
    def _group_similar_behavior(bottlenecks: pd.DataFrame, metric: str, view_type: str):

        behavior_col = 'behavior'
        cols = bottlenecks.columns

        if len(bottlenecks) > 1:
            behavior_cols = cols[cols.str.contains('_min|_max|_count|_size')]

            link_mat = linkage(bottlenecks[behavior_cols], method='single')
            behavior_labels = fcluster(link_mat, t=10, criterion='distance')

            bottlenecks[behavior_col] = behavior_labels
        else:
            bottlenecks[behavior_col] = 1

        agg_dict = {col: 'mean' for col in cols}
        agg_dict[view_type] = list
        agg_dict.pop(f"{metric}_score")

        return bottlenecks.reset_index().groupby(behavior_col).agg(agg_dict)

    @staticmethod
    def _union_details(details: pd.DataFrame, behavior: int, indices: list, view_type: str):
        view_types = details.index.names

        filtered_details = details.query(
            f"{view_type} in @indices", local_dict={'indices': indices})

        if len(view_types) == 1:
            # This means there is only one view type
            detail_groups = filtered_details

            # So override aggregations accordingly
            agg_dict = {}
            agg_dict[view_type] = set
        else:
            agg_dict = {col: set for col in BOTTLENECK_ORDER[view_type]}
            agg_dict.pop(view_type)

            detail_groups = filtered_details \
                .reset_index() \
                .groupby(view_type) \
                .agg(agg_dict)

            # Then create unions for other view types
            agg_dict = {col: lambda x: set.union(*x) for col in agg_dict}
            agg_dict[view_type] = set

        behavior_col = 'behavior'
        detail_groups[behavior_col] = behavior

        return detail_groups \
            .reset_index() \
            .groupby([behavior_col]) \
            .agg(agg_dict) \
            .reset_index(drop=True)


class CharacteristicRule(RuleHandler):

    @abc.abstractmethod
    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:
        raise NotImplementedError

    @abc.abstractmethod
    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:
        raise NotImplementedError


class CharacteristicAccessPatternRule(CharacteristicRule):

    def __init__(self) -> None:
        super().__init__(rule_key=KnownCharacteristics.ACCESS_PATTERN.value)

    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:

        acc_pat_cols = []
        for acc_pat in list(AccessPattern):
            for col_suffix in ACC_PAT_SUFFIXES:
                col_name = f"{acc_pat.name.lower()}_{col_suffix}"
                acc_pat_cols.append(col_name)

        return {
            'acc_pat_sum': main_view[acc_pat_cols].sum()
        }

    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:
        acc_pat_sum = result['acc_pat_sum']

        sequential_count = int(acc_pat_sum['sequential_count'])
        random_count = int(acc_pat_sum['random_count'])
        total_count = sequential_count + random_count

        return RuleResult(
            description='Access Pattern',
            detail_list=None,
            extra_data=dict(acc_pat_sum),
            reasons=None,
            value=None,
            value_fmt=(
                f"{sequential_count/total_count*100:.2f}% Sequential - "
                f"{random_count/total_count*100:.2f}% Random"
            )
        )


class CharacteristicFileCountRule(CharacteristicRule):

    def __init__(self) -> None:
        super().__init__(rule_key=KnownCharacteristics.FILE_COUNT.value)

    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:

        x = main_view.reset_index()

        tasks = {}

        if 'file_name' in x.columns:
            tasks['total_count'] = x['file_name'].nunique()

            fpp = x \
                .groupby(['file_name'])['proc_name'] \
                .nunique() \
                .to_frame()

            fpp_count = fpp[fpp['proc_name'] == 1].count()

            tasks['fpp_count'] = fpp_count
        else:
            tasks['total_count'] = 0
            tasks['fpp_count'] = 0

        return tasks

    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:
        total_count = int(result['total_count'])
        fpp_count = int(result['fpp_count'])

        detail_list = []

        if total_count == 0 or fpp_count == 0:
            detail_list.append(f"Shared: N/A")
            detail_list.append(f"FPP: N/A")
        else:
            fpp_per = f"{fpp_count/total_count*100:.2f}%"

            shared_count = total_count - fpp_count
            shared_per = f"{shared_count/total_count*100:.2f}%"

            detail_list.append(f"Shared: {shared_count} files ({shared_per})")
            detail_list.append(f"FPP: {fpp_count} files ({fpp_per})")

        return RuleResult(
            description='Files',
            detail_list=detail_list,
            extra_data=None,
            reasons=None,
            value=total_count,
            value_fmt=f"{total_count} files",
        )


class CharacteristicIOOpsRule(CharacteristicRule):

    def __init__(self) -> None:
        super().__init__(rule_key=KnownCharacteristics.IO_COUNT.value)

    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:
        tasks = {}
        tasks['total_count'] = main_view['count'].sum()
        for io_type in IO_TYPES:
            count_col = f"{io_type}_count"
            tasks[count_col] = main_view[count_col].sum()
        return tasks

    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:
        total_count = int(result['total_count'])

        detail_list = []
        for io_type in IO_TYPES:
            count_col = f"{io_type}_count"
            count = int(result[count_col])
            percent = f"{count/total_count*100:.2f}%"
            detail_list.append(
                f"{io_type.capitalize()} - {count:,} ops ({percent})")

        return RuleResult(
            description='I/O Ops',
            detail_list=detail_list,
            extra_data=None,
            reasons=None,
            # rule=rule,
            value=total_count,
            value_fmt=f"{total_count:,} ops",
        )


class CharacteristicIOSizeRule(CharacteristicRule):

    def __init__(self) -> None:
        super().__init__(rule_key=KnownCharacteristics.IO_SIZE.value)

    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:
        tasks = {}
        tasks['total_size'] = main_view['size'].sum()
        for io_type in IO_TYPES:
            if io_type != 'metadata':
                size_col = f"{io_type}_size"
                tasks[size_col] = main_view[size_col].sum()
        return tasks

    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:
        total_size = int(result['total_size'])

        detail_list = []
        for io_type in IO_TYPES:
            if io_type != 'metadata':
                size_col = f"{io_type}_size"
                size = int(result[size_col])
                detail_list.append((
                    f"{io_type.capitalize()} - "
                    f"{convert_bytes_to_unit(size, 'GB'):.2f} GB "
                    f"({size/total_size*100:.2f}%)"
                ))

        return RuleResult(
            description='I/O Size',
            detail_list=detail_list,
            extra_data=None,
            reasons=None,
            value=total_size,
            value_fmt=f"{convert_bytes_to_unit(total_size, 'GB'):.2f} GB",
        )


class CharacteristicIOTimeRule(CharacteristicRule):

    def __init__(self) -> None:
        super().__init__(rule_key=KnownCharacteristics.IO_TIME.value)

    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:
        tasks = {}

        tasks['total_time'] = main_view \
            .groupby(['proc_name']) \
            .sum()['time'] \
            .max()

        for io_type in IO_TYPES:
            time_col = f"{io_type}_time"
            tasks[time_col] = main_view \
                .groupby(['proc_name']) \
                .sum()[time_col] \
                .max()

        return tasks

    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:
        total_time = result['total_time']

        detail_list = []
        for io_type in IO_TYPES:
            time_col = f"{io_type}_time"
            time = result[time_col]
            time_per = f"{time/total_time*100:.2f}%"
            detail_list.append(
                f"{io_type.capitalize()} - {time:.2f} seconds ({time_per})")

        return RuleResult(
            description='I/O Time',
            detail_list=detail_list,
            extra_data=None,
            reasons=None,
            # rule=rule,
            value=total_time,
            value_fmt=f"{total_time:.2f} seconds",
        )


class CharacteristicProcessCount(CharacteristicRule):

    def __init__(self, rule_key: str) -> None:
        super().__init__(rule_key=rule_key)
        self.col = 'proc_name'
        self.description = 'Process(es)/Rank(s)'
        if rule_key is KnownCharacteristics.APP_COUNT.value:
            self.col = 'app_name'
            self.description = 'App(s)'
        elif rule_key is KnownCharacteristics.NODE_COUNT.value:
            self.col = 'node_name'
            self.description = 'Node(s)'

    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:
        tasks = {}

        if self.col == 'proc_name':
            tasks[f"{self.col}s"] = main_view \
                .map_partitions(lambda df: df.index.get_level_values(COL_PROC_NAME)) \
                .nunique() \
                .compute()
        else:
            tasks[f"{self.col}s"] = main_view \
                .map_partitions(set_proc_name_parts) \
                .reset_index() \
                .groupby([self.col, COL_PROC_NAME]) \
                .agg({
                    'count': sum,
                    'time': sum,
                    'read_size': sum,
                    'write_size': sum,
                }) \
                .groupby([self.col]) \
                .agg({
                    'count': sum,
                    'time': max,
                    'read_size': sum,
                    'write_size': sum,
                }) \
                .sort_values('time', ascending=False)

        return tasks

    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:

        if self.col == 'proc_name':
            value = int(result[f"{self.col}s"])

            return RuleResult(
                description=self.description,
                detail_list=None,
                extra_data=None,
                reasons=None,
                value=value,
                value_fmt=f"{value} {self.description.lower()}",
            )

        max_io_time = characteristics[KnownCharacteristics.IO_TIME.value].value
        total_ops = characteristics[KnownCharacteristics.IO_COUNT.value].value
        total_size = characteristics[KnownCharacteristics.IO_SIZE.value].value

        detail_list = []
        for node, row in pd.DataFrame(result[f"{self.col}s"]).iterrows():
            read_size = row['read_size']
            write_size = row['write_size']
            read_size_gb = convert_bytes_to_unit(read_size, 'GB')
            write_size_gb = convert_bytes_to_unit(write_size, 'GB')
            read_size_per = read_size / total_size * 100
            write_size_per = write_size / total_size * 100
            detail_list.append(' - '.join([
                node,
                f"{row['time']:.2f} s ({row['time'] / max_io_time * 100:.2f}%)",
                f"{read_size_gb:.2f}/{write_size_gb:.2f} GB R/W ({read_size_per:.2f}/{write_size_per:.2f}%)",
                f"{int(row['count']):,} ops ({row['count'] / total_ops * 100:.2f}%)"
            ]))

        value = len(result[f"{self.col}s"])

        return RuleResult(
            description=self.description,
            detail_list=detail_list,
            extra_data=None,
            reasons=None,
            value=value,
            value_fmt=f"{value} {self.description.lower()}",
        )


class CharacteristicXferSizeRule(CharacteristicRule):

    def __init__(self, rule_key: str) -> None:
        super().__init__(rule_key)
        self.io_op = 'write' if rule_key is KnownCharacteristics.WRITE_XFER_SIZE.value else 'read'

    def define_tasks(self, main_view: dd.DataFrame) -> Dict[str, Delayed]:
        tasks = {}

        count_col, min_col, max_col, per_col, xfer_col = (
            'count',
            f"{self.io_op}_min",
            f"{self.io_op}_max",
            'per',
            'xfer',
        )

        min_view = main_view[main_view[min_col] > 0]
        max_view = main_view[main_view[max_col] > 0]

        tasks['min_xfer_size'] = min_view[min_col].min()
        tasks['max_xfer_size'] = max_view[max_col].max()

        tasks['xfer_sizes'] = max_view[max_col].value_counts()

        return tasks

    def handle_task_results(
        self,
        result: Dict[str, Union[str, int, pd.DataFrame, pd.Series]],
        characteristics: Dict[str, RuleResult] = None,
    ) -> RuleResult:

        count_col, min_col, max_col, per_col, xfer_col = (
            'count',
            f"{self.io_op}_min",
            f"{self.io_op}_max",
            'per',
            'xfer',
        )

        min_xfer_size = 0
        max_xfer_size = 0
        if not np.isnan(result['min_xfer_size']):
            min_xfer_size = int(result['min_xfer_size'])
        if not np.isnan(result['max_xfer_size']):
            max_xfer_size = int(result['max_xfer_size'])

        xfer_sizes = pd.DataFrame(result['xfer_sizes']) \
            .rename(columns={max_col: count_col})
        xfer_sizes[xfer_col] = pd.cut(
            xfer_sizes.index, bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS, right=True)
        xfer_bins = xfer_sizes \
            .groupby([xfer_col]) \
            .sum() \
            .replace(0, np.nan) \
            .dropna()
        xfer_bins[per_col] = xfer_bins[count_col] / xfer_bins[count_col].sum()

        total_ops = int(xfer_bins[count_col].sum())

        value_fmt = self._get_xfer_size(max_xfer_size)
        if min_xfer_size > 0 and max_xfer_size > 0 and min_xfer_size != max_xfer_size:
            value_fmt = f"{self._get_xfer_size(min_xfer_size)}-{self._get_xfer_size(max_xfer_size)}"
        value_fmt = f"{value_fmt} - {total_ops:,} ops"

        detail_list = []
        for xfer, row in xfer_bins.iterrows():
            detail_list.append(
                f"{xfer} - {int(row[count_col]):,} ops ({row['per'] * 100:.2f}%)")

        result = RuleResult(
            extra_data=None,
            description='Write Xfer' if self.io_op == 'write' else 'Read Xfer',
            detail_list=detail_list,
            reasons=None,
            value=(min_xfer_size, max_xfer_size),
            value_fmt=value_fmt,
        )

        return result

    @staticmethod
    def _get_xfer_size(size: float):
        size_bin = np.digitize(size, bins=XFER_SIZE_BINS, right=True)
        size_label = np.choose(
            size_bin, choices=XFER_SIZE_BIN_NAMES, mode='clip')
        return size_label
