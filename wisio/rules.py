import abc
import dask.dataframe as dd
import dataclasses
from enum import Enum
from typing import Dict, List, Optional
from .base import ViewKey, ViewType


class Rule(Enum):
    EXCESSIVE_IO_TIME = 10
    WORKLOAD_IMBALANCE = 11
    COLLECTIVE_IO_CONTENTION = 12
    FILE_SHARING_IMBALANCE = 13
    ACCESS_PATTERN_ISSUE = 20
    METADATA_ACCESS_ISSUE = 21
    SMALL_IO_ACCESS = 30


class RuleEngine(abc.ABC):

    def __init__(self, rules: Dict[ViewType, List[Rule]]) -> None:
        self.rules = rules

    @abc.abstractmethod
    def process_bottlenecks(self, bottlenecks: Dict[ViewKey, Dict[str, dd.DataFrame]], threshold=0.5) -> Dict[ViewKey, object]:
        raise NotImplementedError


@dataclasses.dataclass
class RuleReason(object):
    description: str
    value: Optional[float]


@dataclasses.dataclass
class RuleResult(object):
    rule: Rule
    description: str
    reasons: Optional[List[RuleReason]]
