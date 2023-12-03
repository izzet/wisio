# import darshan as d
import dask
import dask.dataframe as dd
import pandas as pd 
from datetime import datetime
from glob import glob
from typing import List
from ._darshan.analysis import create_dxt_dataframe, generate_dxt_records
from .cluster_management import ClusterConfig
from .analyzer import Analyzer
from .types import AnalysisAccuracy, ViewType
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
        debug=False
    ):
        super().__init__(
            name='Darshan',
            working_dir=working_dir,
            checkpoint=checkpoint,
            checkpoint_dir=checkpoint_dir,
            cluster_config=cluster_config,
            debug=debug,
        )

    def analyze_dxt(
        self,
        trace_path_pattern: str,
        metrics=['duration'],
        accuracy: AnalysisAccuracy = 'pessimistic',
        slope_threshold: int = 45,
        time_granularity: int = 1e3,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ):
        # Read traces
        with ElapsedTimeLogger(message='Read traces'):
            traces = self.read_dxt(
                trace_path_pattern=trace_path_pattern,
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

    def read_dxt(self, trace_path_pattern: str, time_granularity: int):
        trace_paths = glob(trace_path_pattern)

        rows = []


        df = None

        for trace_path in trace_paths:
            t0 = datetime.now()
            if df is None:
                df = create_dxt_dataframe(trace_path, time_granularity)
            else:
                df = pd.concat([df, create_dxt_dataframe(trace_path, time_granularity)])
            

        return dd.from_pandas(df, npartitions=1)

        trace_bag = dask.bag.from_delayed([
            dask.delayed(generate_dxt_records)(trace_path, time_granularity)
            for trace_path in trace_paths
        ])
        traces = trace_bag.to_dataframe(meta=DXT_COLS)
        npartitions = 1
        traces = traces.repartition(npartitions=npartitions)
        return traces
