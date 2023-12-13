import logging
import dask.dataframe as dd
from distributed import get_client
from .logger import ElapsedTimeLogger


class EventLogger(ElapsedTimeLogger):

    def __init__(self, key: str, message: str, level=logging.INFO):
        super().__init__(message, level, stacklevel=4)
        self.key = key

    def __enter__(self):
        super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        get_client().log_event('elapsed_times', dict(
            elapsed_time=self.elapsed_time,
            key=self.key,
            message=self.message,
            start_time=self.start_time,
            end_time=self.end_time,
        ))


def flatten_column_names(ddf: dd.DataFrame):
    ddf.columns = ['_'.join(tup).rstrip('_') for tup in ddf.columns.values]
    return ddf
