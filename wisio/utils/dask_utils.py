import logging
import math
import dask
import dask.dataframe as dd
from dask.delayed import delayed
from dask.base import compute
from dask.utils import parse_bytes
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


def as_single_dask_partition(frame):
    """Wrap a pandas frame as a one-partition Dask frame, ready to write.

    Views and bottlenecks small enough to materialise are computed in pandas,
    but checkpoints and the bottleneck store keep one on-disk shape: a Parquet
    directory with the `_metadata` file the readers look for. So a pandas
    result is put back on Dask rather than written directly.

    `from_delayed` rather than `from_pandas` because these frames are indexed
    by their view type -- a MultiIndex whenever there is more than one -- and
    `from_pandas` rejects a MultiIndex.

    `dataframe.convert-string` is turned off for the wrap. Left on, Dask
    rewrites the object columns of an incoming pandas frame to pyarrow strings,
    which pyarrow then writes as `large_string` and reads back as pandas
    `string`. The Dask path writes plain `string` and reads back `object`, so
    the same analysis would land a different Parquet schema and a different
    dtype depending only on how large the trace was. Disabling the conversion
    reproduces the Dask path exactly, and leaves the frame's own dtypes alone
    -- which matters because an object column here can hold sets, not just
    strings.

    Args:
        frame: A pandas DataFrame.

    Returns:
        A single-partition Dask DataFrame.
    """
    with dask.config.set({'dataframe.convert-string': False}):
        return dd.from_delayed([delayed(frame)], meta=frame.iloc[:0])


def row_count(frame):
    """Number of rows, lazily on a Dask frame and immediately on a pandas one.

    Views small enough to materialise are computed in pandas, so a frame here
    may be either. `reduction` is Dask-only; `len` on a lazy frame would
    execute the whole chain, which is the opposite of what the Dask branch
    wants. Both results survive `dask.compute`, which passes an int through
    unchanged.
    """
    if hasattr(frame, 'reduction'):
        return frame.reduction(len, sum)
    return len(frame)


def repartition_to_size(ddf: dd.DataFrame, partition_size: str) -> dd.DataFrame:
    """Repartitions so each partition holds roughly `partition_size` of data.

    Equivalent in intent to `ddf.repartition(partition_size=...)`, but measures
    the frame here instead of letting dask do it inside the query optimizer.
    dask-expr defers that measurement until the expression is lowered, which
    happens during graph construction -- `to_parquet` asks for
    `known_divisions`, that lowers the repartition, and the lowering computes
    memory usage while the graph is still being built. On Python 3.10 and 3.11
    that nested compute deadlocks outright; on 3.12 it merely costs several
    times what an explicit measurement does.

    The measurement itself is the same work dask would have done, just hoisted
    out of the optimizer, so this is not an extra pass over the data.

    Args:
        ddf: The Dask DataFrame to repartition.
        partition_size: Target size per partition, e.g. '256MB'.

    Returns:
        The repartitioned Dask DataFrame.
    """
    (total_bytes,) = compute(ddf.memory_usage(deep=True).sum())
    npartitions = max(1, math.ceil(int(total_bytes) / parse_bytes(partition_size)))
    return ddf.repartition(npartitions=npartitions)
