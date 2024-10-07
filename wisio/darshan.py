import dask.dataframe as dd
import pandas as pd
from glob import glob

from ._darshan.analysis import create_dxt_dataframe
from .analyzer import Analyzer


class DarshanAnalyzer(Analyzer):
    def read_trace(self, trace_path: str) -> dd.DataFrame:
        if '*' in trace_path:
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
        return dd.from_pandas(df, npartitions=1)
