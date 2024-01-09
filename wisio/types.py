import dask.dataframe as dd
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Union, Tuple
from .constants import HUMANIZED_VIEW_TYPES


AnalysisAccuracy = Literal['accurate', 'optimistic', 'pessimistic']
Metric = Literal[
    'att_perf',
    'bw',
    'intensity',
    'iops',
    'time',
]
OutputType = Literal['console', 'csv', 'html', 'json']
ViewType = Literal[
    'file_name',
    'proc_name',
    'time_range',
]
ViewKey = Union[Tuple[ViewType], Tuple[ViewType, ViewType],
                Tuple[ViewType, ViewType, ViewType]]


@dataclass
class AnalysisSetup:
    accuracy: AnalysisAccuracy
    checkpoint: bool
    cluster_type: str
    debug: bool
    memory: int
    metric_threshold: float
    num_threads_per_worker: int
    num_workers: int
    processes: bool
    slope_threshold: float
    verbose: bool


@dataclass
class ScoringResult:
    attached_records: dd.DataFrame
    evaluated_groups: dd.DataFrame
    potential_bottlenecks: dd.DataFrame


@dataclass
class RawStats:
    job_time: dd.core.Scalar
    time_granularity: int
    total_count: dd.core.Scalar


@dataclass
class RuleReason:
    condition: str
    message: str


@dataclass
class Rule:
    name: str
    condition: str
    reasons: Optional[List[RuleReason]]
    # source: Optional[str]


@dataclass
class RuleResultReason:
    description: str
    # value: Optional[float]


@dataclass
class RuleResult:
    description: str
    detail_list: Optional[List[str]]
    extra_data: Optional[dict]
    reasons: Optional[List[RuleResultReason]]
    value: Optional[Union[float, int, tuple]]
    value_fmt: Optional[str]


@dataclass
class ViewResult:
    group_view: dd.DataFrame
    metric: str
    slope_view: dd.DataFrame
    view: dd.DataFrame
    view_type: ViewType


MainView = dd.DataFrame


Characteristics = Dict[str, RuleResult]

RuleResultsPerView = Dict[ViewKey, List[RuleResult]]
RuleResultsPerViewPerMetric = Dict[Metric, RuleResultsPerView]
RuleResultsPerViewPerMetricPerRule = Dict[str, RuleResultsPerViewPerMetric]

ScoringPerView = Dict[ViewKey, ScoringResult]
ScoringPerViewPerMetric = Dict[Metric, ScoringPerView]

ViewResultsPerView = Dict[ViewKey, ViewResult]
ViewResultsPerViewPerMetric = Dict[Metric, ViewResultsPerView]


def humanized_view_name(view_key_type: Union[ViewKey, ViewType], separator='_'):
    if isinstance(view_key_type, tuple):
        return separator.join([HUMANIZED_VIEW_TYPES[view_type] for view_type in view_key_type])
    return HUMANIZED_VIEW_TYPES[view_key_type]


def view_name(view_key_type: Union[ViewKey, ViewType], separator='_'):
    return separator.join(view_key_type) if isinstance(view_key_type, tuple) else view_key_type
