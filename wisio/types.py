import dask.dataframe as dd
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Union, Tuple


AnalysisAccuracy = Literal['accurate', 'optimistic', 'pessimistic']
Metric = Literal[
    'att_perf',
    'bw',
    'intensity',
    'iops',
    'time',
]
OutputType = Literal['console', 'html', 'json']
ViewType = Literal[
    'file_name',
    'proc_name',
    'time_range',
]
ViewKey = Union[Tuple[ViewType], Tuple[ViewType, ViewType],
                Tuple[ViewType, ViewType, ViewType]]


@dataclass
class BottleneckResult:
    bottlenecks: dd.DataFrame
    details: dd.DataFrame


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
    view: dd.DataFrame
    view_type: ViewType


MainView = dd.DataFrame

BottlenecksPerView = Dict[ViewKey, BottleneckResult]
BottlenecksPerViewPerMetric = Dict[Metric, BottlenecksPerView]

Characteristics = Dict[str, RuleResult]

RuleResultsPerView = Dict[ViewKey, List[RuleResult]]
RuleResultsPerViewPerMetric = Dict[Metric, RuleResultsPerView]

ViewResultsPerView = Dict[ViewKey, ViewResult]
ViewResultsPerViewPerMetric = Dict[Metric, ViewResultsPerView]


def view_name(view_key_type: Union[ViewKey, ViewType], separator='_'):
    return separator.join(view_key_type) if isinstance(view_key_type, tuple) else view_key_type
