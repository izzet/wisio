import dask.dataframe as dd
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Literal, Optional, Union, Tuple
from .constants import HUMANIZED_VIEW_TYPES


class Score(Enum):
    NONE = 'none'
    TRIVIAL = 'trivial'
    VERY_LOW = 'very low'
    LOW = 'low'
    MEDIUM = 'medium'
    HIGH = 'high'
    VERY_HIGH = 'very high'
    CRITICAL = 'critical'


AnalysisAccuracy = Literal['accurate', 'optimistic', 'pessimistic']
Metric = Literal[
    'att_perf',
    'bw',
    'intensity',
    'iops',
    'time',
]
OutputType = Literal['console', 'csv', 'html', 'json', 'sqlite']
ViewType = Literal[
    'file_name',
    'proc_name',
    'time_range',
]
ViewKey = Union[Tuple[ViewType], Tuple[ViewType, ViewType],
                Tuple[ViewType, ViewType, ViewType]]


@dataclass
class AnalysisRuntimeConfig:
    accuracy: AnalysisAccuracy
    checkpoint: bool
    cluster_type: str
    debug: bool
    memory: int
    num_threads_per_worker: int
    num_workers: int
    processes: bool
    run_id: str
    slope_threshold: float
    verbose: bool
    working_dir: str


@dataclass
class ScoringResult:
    critical_view: dd.DataFrame
    records_index: dd.Index
    scored_view: dd.DataFrame


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
    compact_desc: Optional[str]
    description: str
    detail_list: Optional[List[str]]
    extra_data: Optional[dict]
    object_hash: Optional[int]
    reasons: Optional[List[RuleResultReason]]
    value: Optional[Union[float, int, tuple]]
    value_fmt: Optional[str]


@dataclass
class BottleneckOutput:
    description: str
    id: int
    metric: str
    num_files: int
    num_processes: int
    num_time_periods: int
    object_hash: int
    reasons: List[RuleResultReason]
    rule: str
    score: str
    view_name: str


@dataclass
class BottleneckResult:
    results: List[RuleResult]
    severities: Dict[str, int]


@dataclass
class ViewResult:
    critical_view: dd.DataFrame
    metric: str
    records: dd.DataFrame
    view: dd.DataFrame
    view_type: ViewType


MainView = dd.DataFrame


Characteristics = Dict[str, RuleResult]

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
