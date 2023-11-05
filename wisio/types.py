from dask.dataframe import DataFrame
from dataclasses import dataclass
from typing import Dict, Literal, Union, Tuple


COL_APP_NAME = 'app_name'
COL_FILE_DIR = 'file_dir'
COL_FILE_NAME = 'file_name'
COL_FILE_REGEX = 'file_regex'
COL_HOST_NAME = 'host_name'
COL_NODE_NAME = 'node_name'
COL_PROC_NAME = 'proc_name'
COL_RANK = 'rank'
COL_TIME_RANGE = 'time_range'


AnalysisAccuracy = Literal['accurate', 'optimistic', 'pessimistic']
BottleneckType = Literal[
    'high_level_view',
    'mid_level_view',
    'low_level_view',
]
Metric = Literal[
    'att_perf',
    'bw',
    'duration',
    'intensity',
    'iops',
]
ViewType = Literal[
    'file_name',
    'proc_name',
    'time_range',
]
ViewKey = Union[Tuple[ViewType], Tuple[ViewType, ViewType],
                Tuple[ViewType, ViewType, ViewType]]


@dataclass
class BottleneckViews:
    low_level_view: DataFrame
    mid_level_view: DataFrame
    high_level_view: DataFrame


@dataclass
class ViewNormalizationData:
    index_sum: int
    metric_max: float


@dataclass
class ViewResult:
    group_view: DataFrame
    metric_col: str
    norm_data: ViewNormalizationData
    view: DataFrame
    view_type: ViewType


MainView = DataFrame

ViewResultsPerView = Dict[ViewKey, ViewResult]
ViewResultsPerViewPerMetric = Dict[Metric, ViewResultsPerView]

BottlenecksPerView = Dict[ViewKey, BottleneckViews]
BottlenecksPerViewPerMetric = Dict[Metric, BottlenecksPerView]


def view_name(view_key_type: Union[ViewKey, ViewType]):
    return '_'.join(view_key_type) if isinstance(view_key_type, tuple) else view_key_type
