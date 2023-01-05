import dask.dataframe as dd
import numpy as np
import pandas as pd
from copy import copy
from dask.distributed import Client

from ..utils.dask_agg import nunique
from .constants import TIME_PRECISION, IOCat


IO_CATS = [io_cat.value for io_cat in list(IOCat)]
LLC_AGG = {
    'acc_pat': [min, max],
    'app': [nunique()],
    'duration': [sum],
    'file_id': [nunique()],
    'hostname': [nunique()],
    'index': ['count'],
    'io_cat': [min, max],
    'proc_id': [nunique()],
    'rank': [min, max, nunique()],
    'size': [min, max, 'mean', sum],
}
XFER_SIZE_BINS = [
    -np.inf,
    4 * 1024.0,
    16 * 1024.0,
    64 * 1024.0,
    256 * 1024.0,
    1 * 1024.0 * 1024.0,
    4 * 1024.0 * 1024.0,
    16 * 1024.0 * 1024.0,
    64 * 1024.0 * 1024.0,
    np.inf
]
XFER_SIZE_BIN_LABELS = [
    '<4KB',
    '~16KB',
    '~64KB',
    '~256KB',
    '~1MB',
    '~4MB',
    '~16MB',
    '~64MB',
    '>64MB'
]
XFER_SIZE_BIN_NAMES = [
    '<4KB',
    '4KB',
    '16KB',
    '64KB',
    '256KB',
    '1MB',
    '4MB',
    '16MB',
    '64MB',
    '>64MB'
]


def compute_hlm(
    client: Client,
    log_dir: str,
    global_min_max: dict,
    filter_group: str,
    metric='duration',
    delta=0.0001,
):
    # Set client as current
    with client.as_current():
        # Read Parquet files
        # print(filter_group, get_client())
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns([filter_group]))
        # Set trange
        ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
        # Compute hlm
        hlm_df = ddf \
            .groupby([filter_group]) \
            .agg({metric: [sum]}) \
            .compute() \
            .sort_values((metric, 'sum'), ascending=False)
        # Filter by delta
        return filter_delta(df=hlm_df, delta=delta, metric=metric)


def compute_hlm_perm(
    client: Client,
    log_dir: str,
    global_min_max: dict,
    filter_perm: tuple,
    hlm_df: pd.DataFrame,
    metric='duration',
    delta=0.0001,
):
    # Set client as current
    with client.as_current():
        # Read filter permutation
        filter_group, perm_group = filter_perm
        # print(filter_perm, get_client())
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns([filter_group, perm_group]))
        # Set trange
        ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
        # Compute perm
        perm_ddf = ddf[ddf[filter_group].isin(hlm_df.index)] \
            .groupby([perm_group]) \
            .agg({metric: [sum]}) \
            .compute() \
            .sort_values((metric, 'sum'), ascending=False)
        # Filter by delta
        return filter_delta(df=perm_ddf, delta=delta, metric=metric)


def compute_llc(
    client: Client,
    log_dir: str,
    global_min_max: dict,
    filter_perm: tuple,
    hlm_perm_df: pd.DataFrame,
    metric='duration',
    delta=0.0001,
):
    # Set client as current
    with client.as_current():
        # Read filter permutation
        filter_group, perm_group = filter_perm
        # print(filter_perm, hlm_perm_df.index.name, get_client())
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns(list(LLC_AGG.keys())))
        # Set trange
        ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
        # print(ddf.columns)
        # Compute llc
        llc_df = ddf[ddf[perm_group].isin(hlm_perm_df.index) & ddf['io_cat'].isin(IO_CATS)] \
            .groupby([perm_group, 'io_cat']) \
            .agg(LLC_AGG) \
            .compute() \
            .dropna() \
            .sort_values((metric, 'sum'), ascending=False)
        # Filter by I/O category
        # llc_df = llc_df[llc_df['io_cat'].isin([IOCat.READ.value, IOCat.WRITE.value, IOCat.METADATA.value])] \
        #     .dropna() \
        #     .sort_values((metric, 'sum'), ascending=False)
        # Format df
        return format_df(df=llc_df)


def compute_llc_alt(
    client: Client,
    log_dir: str,
    global_min_max: dict,
    filter_perm: tuple,
    hlm_perm_df: pd.DataFrame,
    metric='duration',
    delta=0.0001,
):
    # Set client as current
    with client.as_current():
        # Read filter permutation
        filter_group, perm_group, final_group = filter_perm
        # print(filter_perm, hlm_perm_df.index.name, get_client())
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns(list(LLC_AGG.keys())))
        # Set trange
        ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
        # print(ddf.columns)
        # Compute llc
        llc_df = ddf[ddf[perm_group].isin(hlm_perm_df.index) & ddf['io_cat'].isin(IO_CATS)] \
            .groupby([final_group]) \
            .agg(LLC_AGG) \
            .compute() \
            .dropna() \
            .sort_values((metric, 'sum'), ascending=False)
        # Filter by I/O category
        # llc_df = llc_df[llc_df['io_cat'].isin([IOCat.READ.value, IOCat.WRITE.value, IOCat.METADATA.value])] \
        #     .dropna() \
        #     .sort_values((metric, 'sum'), ascending=False)
        # Format df
        return format_df(df=llc_df)


def filter_delta(df: pd.DataFrame, delta: float, metric='duration'):
    df[metric, 'csp'] = df[metric, 'sum'].cumsum() / df[metric, 'sum'].sum()
    df[metric, 'delta'] = df[metric, 'csp'].diff().fillna(df[metric, 'csp'])
    df_filtered = df[df[metric, 'delta'] > delta]
    df_filtered = df_filtered.reindex(sorted(df_filtered.columns), axis=1)
    return df_filtered


def format_df(df, add_xfer=True):
    if ('size', 'sum') in df.columns:
        df['bw', 'sum'] = df['size', 'sum'] / df['duration', 'sum']
        df['bw', 'per'] = df.div(df['bw', 'sum'].sum(), level=0)['bw', 'sum']
        df['size', 'per'] = df.div(df['size', 'sum'].sum(), level=0)['size', 'sum']
    df['duration', 'per'] = df.div(df['duration', 'sum'].sum(), level=0)['duration', 'sum']
    if ('index', 'count') in df.columns:
        df['index', 'per'] = df.div(df['index', 'count'].sum(), level=0)['index', 'count']
    if add_xfer:
        df['xfer', 'max'] = pd.cut(df['size', 'max'], bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS, right=True)
        df['xfer', 'min'] = pd.cut(df['size', 'min'], bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS, right=True)
        df['xfer', 'mean'] = pd.cut(df['size', 'mean'], bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS, right=True)
        df.drop(columns=[('size', 'max'), ('size', 'min'), ('size', 'mean')], inplace=True)
    if ('size', 'sum') in df.columns:
        df['bw', 'fmt'] = df['bw', 'sum'].apply(lambda x: format(float(x)/1024.0/1024.0/1024.0, ".2f") + "GB/s")
        df['size', 'fmt'] = df['size', 'sum'].apply(lambda x: format(float(x)/1024.0/1024.0/1024.0, ".2f") + "GB")
    df = df.reindex(sorted(df.columns), axis=1)
    return df


def get_parquet_columns(columns: list):
    columns = copy(columns)
    columns.extend(['duration', 'tmid'])
    return list(set(map(lambda x: x.replace('trange', 'tmid'), columns)))


def set_derived_ddf_fields(ddf: dd.DataFrame, global_min_max: dict):
    tmid_min, tmid_max = global_min_max['tmid']
    trange = np.arange(tmid_min, tmid_max, TIME_PRECISION)
    # ddf = ddf.map_partitions(lambda df: df.assign(trange=pd.cut(df['tmid'], trange, right=True)))
    ddf = ddf.map_partitions(lambda df: df.assign(trange=np.digitize(df['tmid'], trange, right=True)))
    return ddf
