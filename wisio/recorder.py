import dask.dataframe as dd
import json
import numpy as np
import pandas as pd
from dask.distributed import Future, get_client
from typing import List, Union
from .analyzer import Analyzer
from .cluster_management import ClusterConfig
from .constants import IO_CATS
from .types import AnalysisAccuracy, Metric, OutputType, ViewType
from .utils.logger import ElapsedTimeLogger


CAT_POSIX = 0
CHECKPOINT_MAIN_VIEW = '_main_view'
CHECKPOINT_HLM = '_hlm'
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

    def __init__(
        self,
        working_dir: str,
        checkpoint: bool = False,
        checkpoint_dir: str = '',
        cluster_config: ClusterConfig = None,
        debug=False,
        output_type: OutputType = 'console',
    ):
        super().__init__(
            name='Recorder',
            checkpoint=checkpoint,
            checkpoint_dir=checkpoint_dir,
            cluster_config=cluster_config,
            debug=debug,
            output_type=output_type,
            working_dir=working_dir,
        )

    def analyze_parquet(
        self,
        trace_path: str,
        metrics: List[Metric] = ['time'],
        accuracy: AnalysisAccuracy = 'pessimistic',
        slope_threshold: int = 45,
        time_granularity: int = 1e7,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ):
        # Read traces
        with ElapsedTimeLogger(message='Read traces'):
            traces = self.read_parquet_files(
                trace_path=trace_path,
                time_granularity=time_granularity,
            )

        # Analyze traces
        return self.analyze_traces(
            traces=traces,
            metrics=metrics,
            accuracy=accuracy,
            slope_threshold=slope_threshold,
            view_types=view_types,
        )

    def read_parquet_files(self, trace_path: str, time_granularity: int):
        traces = dd.read_parquet(f"{trace_path}/*.parquet")

        traces['acc_pat'] = traces['acc_pat'].astype(np.uint8)
        traces['count'] = 1
        traces['duration'] = traces['duration'].astype(np.float64)
        traces['io_cat'] = traces['io_cat'].astype(np.uint8)

        global_min_max = self._load_global_min_max(trace_path=trace_path)
        time_ranges = self._compute_time_ranges(
            global_min_max=global_min_max,
            time_granularity=time_granularity,
        )

        traces = traces[(traces['cat'] == CAT_POSIX) & (traces['io_cat'].isin(IO_CATS))] \
            .map_partitions(self._set_time_ranges, time_ranges=time_ranges) \
            .rename(columns=RENAMED_COLS) \
            .drop(columns=DROPPED_COLS, errors='ignore')

        return traces

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
        return df.assign(time_range=np.digitize(df['tmid'], bins=time_ranges, right=True))
