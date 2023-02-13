import dask.dataframe as dd
import pandas as pd
from enum import Enum
from typing import Dict, List


class Rule(Enum):
    # EXCESSIVE_IO_TIME = 10
    # PROBLEMATIC_ACCESS_PATTERN = 11
    TIME_RANGE_EXCESSIVE_IO_TIME = 20
    FILE_EXCESSIVE_IO_TIME = 30
    PROCESS_EXCESSIVE_IO_TIME = 40
    PROCESS_WORKLOAD_IMBALANCE = 41
    PROCESS_COLLECTIVE_IO_CONTENTION = 42
    PROCESS_FILE_SHARING_IMBALANCE = 43


class RuleEngine(object):

    def __init__(
        self,
        rules: List[Rule],
        views: Dict[tuple, dd.DataFrame]
    ):
        self.rules = rules
        self.views = views

    def process_rules(self, cut=0.5) -> Dict[tuple, pd.DataFrame]:
        raise NotImplementedError
