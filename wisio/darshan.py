# import darshan as d
import dask.dataframe as dd
import pandas as pd
from dask import delayed
from glob import glob
from typing import List

from ._darshan.analysis import create_dxt_dataframe
from .cluster_management import ClusterConfig
from .constants import EVENT_READ_TRACES
from .analyzer import CHECKPOINT_MAIN_VIEW, Analyzer
from .types import AnalysisAccuracy, RawStats, ViewType
from .utils.dask_utils import EventLogger


CHECKPOINT_RAW_STATS = '_raw_stats'
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
        bottleneck_dir: str = '',
        checkpoint: bool = False,
        checkpoint_dir: str = '',
        cluster_config: ClusterConfig = None,
        debug=False,
        verbose=False,
    ):
        super().__init__(
            name='Darshan',
            bottleneck_dir=bottleneck_dir,
            checkpoint=checkpoint,
            checkpoint_dir=checkpoint_dir,
            cluster_config=cluster_config,
            debug=debug,
            verbose=verbose,
            working_dir=working_dir,
        )

    def analyze_dxt(
        self,
        trace_path_pattern: str,
        accuracy: AnalysisAccuracy = 'pessimistic',
        exclude_bottlenecks: List[str] = [],
        exclude_characteristics: List[str] = [],
        logical_view_types: bool = False,
        metrics=['time'],
        slope_threshold: int = 45,
        time_granularity: int = 1e3,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ):
        # Init traces
        traces = pd.DataFrame()

        # Check checkpoint status
        main_view_name = self.get_checkpoint_name(CHECKPOINT_MAIN_VIEW, *sorted(view_types))
        if not self.checkpoint or not self.has_checkpoint(name=main_view_name):
            # Read traces
            with EventLogger(key=EVENT_READ_TRACES, message='Read traces'):
                traces, job_time = self.read_dxt(
                    trace_path_pattern=trace_path_pattern,
                    time_granularity=time_granularity,
                )

        # Prepare raw stats
        raw_stats = self.restore_extra_data(
            name=CHECKPOINT_RAW_STATS,
            fallback=lambda: dict(
                job_time=delayed(job_time),
                time_granularity=time_granularity,
                total_count=traces.index.count().persist(),
            )
        )

        # Analyze traces
        return self.analyze_traces(
            accuracy=accuracy,
            exclude_bottlenecks=exclude_bottlenecks,
            exclude_characteristics=exclude_characteristics,
            logical_view_types=logical_view_types,
            metrics=metrics,
            raw_stats=RawStats(**raw_stats),
            slope_threshold=slope_threshold,
            traces=traces,
            view_types=view_types,
        )

    def read_dxt(self, trace_path_pattern: str, time_granularity: int):
        trace_paths = glob(trace_path_pattern)

        df = None
        for trace_path in trace_paths:
            if df is None:
                df, job_time = create_dxt_dataframe(trace_path, time_granularity)
            else:
                df2, _ = create_dxt_dataframe(trace_path, time_granularity)
                df = pd.concat([df, df2])

        return dd.from_pandas(df, npartitions=max(1, self.cluster_config.n_workers)), job_time
