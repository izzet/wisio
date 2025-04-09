import dask.dataframe as dd
import os
import pandas as pd
from glob import glob


from ._darshan.analysis import create_dxt_dataframe
from .analyzer import Analyzer


class DarshanAnalyzer(Analyzer):
    job_time: float = 0.0

    def read_trace(self, trace_path: str) -> dd.DataFrame:
        if not trace_path.endswith('.darshan') and not os.path.isdir(trace_path):
            raise ValueError(
                f"Invalid trace path: {trace_path}. Must be a directory or a .darshan file."
            )
        if os.path.isdir(trace_path):
            trace_path = os.path.join(trace_path, '*.darshan')
            trace_paths = glob(trace_path)
            df = None
            for trace_path in trace_paths:
                if df is None:
                    df, job_time = create_dxt_dataframe(
                        trace_path, self.time_granularity
                    )
                else:
                    df2, _ = create_dxt_dataframe(trace_path, self.time_granularity)
                    df = pd.concat([df, df2])
        else:
            df, job_time = create_dxt_dataframe(trace_path, self.time_granularity)
        self.job_time = job_time
        return dd.from_pandas(df, npartitions=1)

    def compute_job_time(self, traces: dd.DataFrame) -> float:
        return self.job_time
