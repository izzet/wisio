import abc
import dask.dataframe as dd
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Union
from .base import ViewKey, ViewType


class Rule(Enum):
    CHAR_ACCESS_PATTERN = 100
    CHAR_APP_COUNT = 101
    CHAR_BUFFERED_BANDWIDTH = 102
    CHAR_CORE_COUNT = 103
    CHAR_DATA_DISTRIBUTION = 104
    CHAR_DATA_METADATA_RATIO = 105
    CHAR_EPOCH_COUNT = 106
    CHAR_EVERY_X_NODE = 107
    CHAR_FILE_COUNT = 108
    CHAR_FILE_STATS = 109
    CHAR_FPP_COUNT = 110
    CHAR_IO_COUNT = 111
    CHAR_IO_COUNT_PER_APP = 112
    CHAR_IO_COUNT_XFER_DIST = 113
    CHAR_IO_INTERFACE = 114
    CHAR_IO_PHASES = 115
    CHAR_IO_SIZE = 116
    CHAR_IO_SIZE_PER_APP = 117
    CHAR_IO_TIME = 118
    CHAR_JOB_TIME = 119
    # CHAR_LOW_BANDWIDTH = 112
    CHAR_MPI_PROC_COUNT = 121
    CHAR_NODE_COUNT = 122
    CHAR_PER_FILE_BANDWIDTH = 123
    CHAR_READ_IO_COUNT = 124
    CHAR_READ_IO_SIZE = 125
    CHAR_READ_WRITE_RATIO = 126
    CHAR_SHARED_FILE_COUNT = 127
    CHAR_STEP_COUNT = 128
    CHAR_WRITE_IO_COUNT = 129
    CHAR_WRITE_IO_SIZE = 130
    CHAR_METADATA_IO_COUNT = 131
    CHAR_READ_XFER_SIZE = 132
    CHAR_WRITE_XFER_SIZE = 133

    BOTT_COL_IO_CONTENTION = 201
    BOTT_DATA_XFER_IMBALANCE = 202
    BOTT_HIGH_PARALLELISM = 203
    BOTT_IO_TIME_IMBALANCE = 204
    BOTT_JOB_IO_CONTENTION = 205
    BOTT_LOW_BANDWIDTH = 206
    BOTT_METADATA_ACCESS = 207
    BOTT_NO_COL_IO = 208
    BOTT_RANDOM_ACCESS = 209
    BOTT_REDUNDANT_ACCESS = 210
    BOTT_SMALL_READS = 211
    BOTT_SMALL_WRITES = 212


@dataclass
class RuleReason(object):
    description: str
    value: Optional[float]


@dataclass
class RuleResult(object):
    data_dict: Optional[dict]    
    description: str
    detail_list: Optional[List[str]]
    reasons: Optional[List[RuleReason]]
    rule: Rule
    value: Optional[Union[float, int, tuple]]
    value_fmt: Optional[str]


class RuleEngine(abc.ABC):

    def __init__(self, rules: Dict[ViewType, List[Rule]]) -> None:
        self.rules = rules

    @abc.abstractmethod
    def process_characteristics(self, main_view: dd.DataFrame) -> Dict[Rule, RuleResult]:
        raise NotImplementedError

    @abc.abstractmethod
    def process_bottlenecks(self, bottlenecks: Dict[ViewKey, Dict[str, dd.DataFrame]], threshold=0.5) -> Dict[ViewKey, object]:
        raise NotImplementedError
