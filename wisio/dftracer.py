import dask.dataframe as dd
import logging
import math
import os
import pandas as pd
import pyarrow as pa
from dask.delayed import delayed
from dask.distributed import wait
from glob import glob
from typing import List

from dftracer.utils import TraceReader

from .analyzer import Analyzer
from .constants import (
    COL_ACC_PAT,
    COL_COUNT,
    COL_FILE_NAME,
    COL_FUNC_ID,
    COL_HOST_NAME,
    COL_IO_CAT,
    COL_PROC_NAME,
    COL_TIME,
    COL_TIME_RANGE,
    IOCategory,
)


CAT_POSIX = 'POSIX'
DFTRACER_TIME_RESOLUTION = 1e6
PFW_COL_MAPPING = {
    'name': COL_FUNC_ID,
    'dur': COL_TIME,
    'hhash': COL_HOST_NAME,
    'fhash': COL_FILE_NAME,
    'trange': COL_TIME_RANGE,
}

# Record kinds in a DFTracer (.pfw) stream. Hash records map an id to a
# file/host/string name; events reference those ids via `fhash`/`hhash`.
TYPE_EVENT = 0
TYPE_FILE_HASH = 1
TYPE_HOST_HASH = 2
TYPE_STRING_HASH = 3
TYPE_METADATA = 4

# Metadata records are phase-'M' entries tagged by `name` in the raw stream.
HASH_RECORD_TYPES = {
    'FH': TYPE_FILE_HASH,
    'HH': TYPE_HOST_HASH,
    'SH': TYPE_STRING_HASH,
}

TRACE_SUFFIXES = ('.pfw', '.pfw.gz')

PARTITION_SIZE_BYTES = 128 * 1024**2

# Schema handed to the analyzer, before PFW_COL_MAPPING is applied.
RECORD_COLUMNS = {
    'name': 'string',
    'cat': 'string',
    'type': 'Int8',
    'pid': 'Int64',
    'tid': 'Int64',
    'ts': 'Int64',
    'dur': 'Int64',
    'hash': 'Int64',
    'fhash': 'Int64',
    'hhash': 'Int64',
    'size': 'Int64',
    'value': 'string',
}

HASH_TABLE_COLUMNS = ['name', 'hash', 'pid', 'tid', 'hhash']
METADATA_COLUMNS = ['name', 'value', 'pid', 'tid', 'hhash']


def resolve_trace_files(trace_path: str) -> List[str]:
    """Expand `trace_path` into the .pfw/.pfw.gz files it refers to."""
    if os.path.isdir(trace_path) and '*' not in trace_path:
        trace_path = f"{trace_path}/*.pfw*"
    files = []
    for path in sorted(glob(trace_path)):
        if path.endswith(TRACE_SUFFIXES):
            files.append(path)
        else:
            logging.warning(f"Ignoring unsupported file {path}")
    return files


def empty_records() -> pd.DataFrame:
    return pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in RECORD_COLUMNS.items()})


def _column(df: pd.DataFrame, name: str):
    """Return a column, or an all-null placeholder when the file lacks it.

    `args.*` columns only materialize when some record in the file carried that
    key, so a trace with no reads has no `args.size` at all.
    """
    if name in df.columns:
        return df[name]
    return pd.Series([pd.NA] * len(df), index=df.index)


def derive_size(df: pd.DataFrame) -> pd.Series:
    """Bytes transferred per event.

    DFTracer reports this as the syscall return value, so it is only meaningful
    for POSIX reads and writes that returned a positive count. `readdir` matches
    'read' textually but transfers no bytes. Non-POSIX events fall back to
    `image_size`, which is what dataloader traces report.
    """
    names = _column(df, 'name').astype('string')
    is_posix = _column(df, 'cat').astype('string') == CAT_POSIX
    is_write = names.str.contains('write', na=False)
    is_read = names.str.contains('read', na=False) & ~names.str.contains('readdir', na=False)

    returned = pd.to_numeric(_column(df, 'args.ret'), errors='coerce')
    size = returned.where(is_posix & (is_write | is_read) & (returned > 0))

    image_size = pd.to_numeric(_column(df, 'args.image_size'), errors='coerce')
    return size.fillna(image_size.where(~is_posix & (image_size > 0)))


def classify_records(df: pd.DataFrame) -> pd.Series:
    """Tag each row with its TYPE_*."""
    types = pd.Series(TYPE_EVENT, index=df.index, dtype='object')
    # fillna is load-bearing: a file with no metadata records has no `ph`
    # column, and NA propagating into `mask` would tag every row as metadata,
    # leaving zero events.
    is_meta = (_column(df, 'ph').astype('string') == 'M').fillna(False)
    names = _column(df, 'name').astype('string')
    for record_name, record_type in HASH_RECORD_TYPES.items():
        types = types.mask(is_meta & (names == record_name), record_type)
    types = types.mask(is_meta & ~names.isin(HASH_RECORD_TYPES), TYPE_METADATA)
    return types


def read_pfw_file(filename: str) -> pd.DataFrame:
    """Read one trace file into wisio's record schema.

    dftracer-utils builds and caches its own index on first read, so gzipped
    traces are seekable without a separate indexing pass.
    """
    reader = TraceReader(filename, auto_build_index=True)
    frames = [
        pa.Table.from_batches([pa.record_batch(batch)]).to_pandas()
        for batch in reader.iter_arrow(flatten_objects=True)
    ]
    frames = [frame for frame in frames if len(frame)]
    if not frames:
        return empty_records()

    df = pd.concat(frames, ignore_index=True)
    record_type = classify_records(df)
    is_event = record_type == TYPE_EVENT

    records = pd.DataFrame(index=df.index)
    records['type'] = record_type
    records['cat'] = _column(df, 'cat')
    # Hash/metadata records carry their payload under `args`; events use `name`.
    records['name'] = _column(df, 'name').where(is_event, _column(df, 'args.name'))
    records['value'] = _column(df, 'args.value')
    records['hash'] = _column(df, 'args.value').where(~is_event)
    records['pid'] = _column(df, 'pid')
    records['tid'] = _column(df, 'tid')
    records['ts'] = _column(df, 'ts')
    records['dur'] = _column(df, 'dur')
    records['fhash'] = _column(df, 'args.fhash')
    records['hhash'] = _column(df, 'args.hhash')
    records['size'] = derive_size(df)

    # Reindex before astype: dask matches partition schemas positionally.
    return records[list(RECORD_COLUMNS)].astype(RECORD_COLUMNS)


class DFTracerAnalyzer(Analyzer):
    def read_trace(self, trace_path: str) -> dd.DataFrame:
        files = resolve_trace_files(trace_path)
        if not files:
            raise ValueError(f"No .pfw or .pfw.gz files found in '{trace_path}'")

        total_size = sum(os.path.getsize(filename) for filename in files)
        self.n_partition = max(1, math.ceil(total_size / PARTITION_SIZE_BYTES))
        logging.info(
            f"Reading {len(files)} DFTracer files ({total_size} bytes) "
            f"into {self.n_partition} partitions"
        )

        self.all_events = dd.from_delayed(
            [delayed(read_pfw_file)(filename) for filename in files],
            meta=empty_records(),
        ).persist()
        _ = wait(self.all_events)

        self.file_hash = self._hash_table(TYPE_FILE_HASH)
        self.host_hash = self._hash_table(TYPE_HOST_HASH)
        self.string_hash = self._hash_table(TYPE_STRING_HASH)
        self.metadata = self.all_events[self.all_events['type'] == TYPE_METADATA][
            METADATA_COLUMNS
        ].persist()

        events = self.all_events[self.all_events['type'] == TYPE_EVENT]
        events = events.repartition(npartitions=self.n_partition).persist()
        _ = wait(events)

        events['ts'] = events['ts'] - events['ts'].min()
        events['te'] = events['ts'] + events['dur']
        events['trange'] = events['ts'] // self.time_granularity
        events['ts'] = events['ts'].astype('Int64')
        events['te'] = events['te'].astype('Int64')
        events['trange'] = events['trange'].astype('Int16')
        events['dur'] = events['dur'] / DFTRACER_TIME_RESOLUTION

        self.events = events.persist()
        _ = wait(
            [
                self.file_hash,
                self.host_hash,
                self.string_hash,
                self.metadata,
                self.events,
            ]
        )

        return self.events.rename(columns=PFW_COL_MAPPING)

    def _hash_table(self, record_type: int) -> dd.DataFrame:
        """One row per hash id, mapping it to the name it stands for."""
        table = self.all_events[self.all_events['type'] == record_type]
        return table[HASH_TABLE_COLUMNS].groupby('hash').first().persist()

    def postread_trace(self, traces: dd.DataFrame) -> dd.DataFrame:
        traces = traces[(traces['cat'] == CAT_POSIX) & (traces['ts'] > 0)]
        traces[COL_PROC_NAME] = (
            'app#'
            + traces[COL_HOST_NAME].astype(str)
            + '#'
            + traces['pid'].astype(str)
            + '#'
            + traces['tid'].astype(str)
        )
        read_cond = 'read'
        write_cond = 'write'
        metadata_cond = 'readlink'
        traces[COL_ACC_PAT] = 0
        traces[COL_COUNT] = 1
        traces[COL_IO_CAT] = 0
        traces[COL_IO_CAT] = traces[COL_IO_CAT].mask(
            (traces['cat'] == CAT_POSIX)
            & ~traces[COL_FUNC_ID].str.contains(read_cond)
            & ~traces[COL_FUNC_ID].str.contains(write_cond),
            IOCategory.METADATA.value,
        )
        traces[COL_IO_CAT] = traces[COL_IO_CAT].mask(
            (traces['cat'] == CAT_POSIX)
            & traces[COL_FUNC_ID].str.contains(read_cond)
            & ~traces[COL_FUNC_ID].str.contains(metadata_cond),
            IOCategory.READ.value,
        )
        traces[COL_IO_CAT] = traces[COL_IO_CAT].mask(
            (traces['cat'] == CAT_POSIX)
            & traces[COL_FUNC_ID].str.contains(write_cond)
            & ~traces[COL_FUNC_ID].str.contains(metadata_cond),
            IOCategory.WRITE.value,
        )
        return traces

    def compute_job_time(self, traces: dd.DataFrame) -> float:
        return (traces['te'].max() - traces['ts'].min()) / DFTRACER_TIME_RESOLUTION

    def compute_total_count(self, traces: dd.DataFrame) -> int:
        return (
            traces[(traces['cat'] == CAT_POSIX) & (traces['ts'] > 0)]
            .reduction(len, sum)
            .persist()
        )
