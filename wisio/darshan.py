# import darshan as d
import dask.dataframe as dd
import pandas as pd
from dask import delayed
from glob import glob
from typing import List

from ._darshan.analysis import create_dxt_dataframe
from .cluster_management import ClusterConfig
from .analyzer import Analyzer
from .types import AnalysisAccuracy, OutputType, RawStats, ViewType
from .utils.logger import ElapsedTimeLogger


DXT_COLS = {
    'acc_pat': "uint64",
    'cat': "string",
    'count': "uint64",
    'file_name': "string",
    'func_id': "string",
    'io_cat': "uint64",
    'proc_name': "string",
    'size': "uint64",
    'time': "float64",
    # 'time': "uint64",
    'time_range': "uint64",
}


class DarshanAnalyzer(Analyzer):

    def __init__(
        self,
        working_dir: str,
        checkpoint: bool = False,
        checkpoint_dir: str = '',
        cluster_config: ClusterConfig = None,
        debug=False,
    ):
        super().__init__(
            name='Darshan',
            checkpoint=checkpoint,
            checkpoint_dir=checkpoint_dir,
            cluster_config=cluster_config,
            debug=debug,
            working_dir=working_dir,
        )

    def analyze_dxt(
        self,
        trace_path_pattern: str,
        metrics=['duration'],
        accuracy: AnalysisAccuracy = 'pessimistic',
        metric_threshold: float = 0.5,
        slope_threshold: int = 45,
        time_granularity: int = 1e3,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ):
        # Read traces
        with ElapsedTimeLogger(message='Read traces'):
            traces, job_time = self.read_dxt(
                trace_path_pattern=trace_path_pattern,
                time_granularity=time_granularity,
            )

        # Prepare raw stats
        raw_stats = RawStats(
            job_time=delayed(job_time),
            time_granularity=time_granularity,
            total_count=traces.index.count().persist(),
        )

        # Analyze traces
        return self.analyze_traces(
            accuracy=accuracy,
            metric_threshold=metric_threshold,
            metrics=metrics,
            raw_stats=raw_stats,
            slope_threshold=slope_threshold,
            traces=traces,
            view_types=view_types,
        )

    def read_dxt(self, trace_path_pattern: str, time_granularity: int):
        trace_paths = glob(trace_path_pattern)

        df = None

        for trace_path in trace_paths:
            if df is None:
                df, job_time = create_dxt_dataframe(
                    trace_path, time_granularity)
            else:
                df = pd.concat(
                    [df, create_dxt_dataframe(trace_path, time_granularity)])

        return dd.from_pandas(df, npartitions=max(1, self.cluster_config.n_workers)), job_time
