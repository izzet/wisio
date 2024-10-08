import dask.dataframe as dd
import json
import numpy as np
import pandas as pd
from dask.distributed import Future, get_client
from typing import Union

from .analyzer import Analyzer
from .constants import IO_CATS


CAT_POSIX = 0
DROPPED_COLS = [
    'app',
    'bandwidth',
    'file_id',
    'hostname',
    'index',
    'level',
    'proc',
    'proc_id',
    'rank',
    'tend',
    'thread_id',
    'tmid',
    'tstart',
]
RENAMED_COLS = {'duration': 'time'}


class RecorderAnalyzer(Analyzer):
    def read_trace(self, trace_path: str) -> dd.DataFrame:
        self.global_min_max = self._load_global_min_max(trace_path=trace_path)
        return dd.read_parquet(trace_path)

    def postread_trace(self, traces: dd.DataFrame) -> dd.DataFrame:
        traces['acc_pat'] = traces['acc_pat'].astype(np.uint8)
        traces['count'] = 1
        traces['duration'] = traces['duration'].astype(np.float64)
        traces['io_cat'] = traces['io_cat'].astype(np.uint8)
        time_ranges = self._compute_time_ranges(
            global_min_max=self.global_min_max,
            time_granularity=self.time_granularity,
        )
        traces = (
            traces[(traces['cat'] == CAT_POSIX) & (traces['io_cat'].isin(IO_CATS))]
            .map_partitions(self._set_time_ranges, time_ranges=time_ranges)
            .rename(columns=RENAMED_COLS)
            .drop(columns=DROPPED_COLS, errors='ignore')
        )
        return traces

    def compute_total_count(self, traces: dd.DataFrame) -> int:
        return (
            traces[(traces['cat'] == CAT_POSIX) & (traces['io_cat'].isin(IO_CATS))]
            .index.count()
            .persist()
        )

    @staticmethod
    def _compute_time_ranges(global_min_max: dict, time_granularity: int):
        tmid_min, tmid_max = global_min_max['tmid']
        time_ranges = np.arange(tmid_min, tmid_max, time_granularity)
        return get_client().scatter(time_ranges)

    @staticmethod
    def _load_global_min_max(trace_path: str) -> dict:
        with open(f"{trace_path}/global.json") as file:
            global_min_max = json.load(file)
        return global_min_max

    @staticmethod
    def _set_time_ranges(df: pd.DataFrame, time_ranges: Union[Future, np.ndarray]):
        if isinstance(time_ranges, Future):
            time_ranges = time_ranges.result()
        return df.assign(
            time_range=np.digitize(df['tmid'], bins=time_ranges, right=True)
        )
