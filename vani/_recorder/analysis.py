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
    'duration': [sum],
    'file_id': [nunique()],
    'index': ['count'],
    'io_cat': [min, max],
    'proc_id': [nunique()],
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
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns([filter_group, 'io_cat']))
        # Set trange
        ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
        # Compute hlm
        hlm_df = ddf \
            .groupby([filter_group, 'io_cat']) \
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
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns([filter_group, perm_group]))
        # Set trange
        ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
        # Compute perm
        perm_ddf = ddf[ddf[filter_group].isin(hlm_df.groupby(level=0).sum().index)] \
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
    stats_df: pd.DataFrame,
    metric='duration',
    delta=0.0001,
):
    # Set client as current
    with client.as_current():
        # Read filter permutation
        filter_group, perm_group, final_group = filter_perm
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns(list(LLC_AGG.keys())))
        # Set trange
        ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
        # Compute final perm
        final_df = ddf[ddf[perm_group].isin(hlm_perm_df.index)] \
            .groupby([final_group]) \
            .agg({metric: [sum]}) \
            .compute()
        # Filter delta
        final_ddf_filtered = filter_delta(df=final_df, delta=delta, metric=metric)
        # Compute llc
        llc_df = ddf[ddf[final_group].isin(final_ddf_filtered.index)] \
            .groupby([filter_group, perm_group, final_group]) \
            .agg(LLC_AGG) \
            .compute() \
            .dropna() \
            .sort_values((metric, 'sum'), ascending=False)
        # Format all
        llc_df_formatted = format_df(df=llc_df, stats_df=stats_df)
        # Filter formatted
        return filter_delta(df=llc_df_formatted, delta=delta, metric=metric)


def compute_stats(
    client: Client,
    log_dir: str,
):
    # Set client as current
    with client.as_current():
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet")
        # Compute stats
        stats_df = ddf \
            .groupby(['io_cat']) \
            .agg({
                'acc_pat': [min, max],
                'app': [nunique()],
                'duration': [sum],
                'file_id': [nunique()],
                'hostname': [nunique()],
                'index': ['count'],
                'proc_id': [nunique()],
                'rank': [nunique()],
                'size': [min, max, 'mean', sum],
            }) \
            .compute()
        # Format stats
        # stat_df = stat_df[stat_df.index.isin(IO_CATS)]
        return format_df(df=stats_df, stats_df=stats_df)


def compute_unique_filenames(
    client: Client,
    log_dir: str,
    unique_file_ids: list,
):
    # Set client as current
    with client.as_current():
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=['file_id', 'filename'])
        # Compute stats
        filenames_df = ddf[ddf['file_id'].isin(unique_file_ids)] \
            .groupby(['file_id']) \
            .agg({'filename': min}) \
            .compute()
        # Return as dict
        return filenames_df.T.to_dict()


def compute_unique_processes(
    client: Client,
    log_dir: str,
    unique_proc_ids: list,
):
    # Set client as current
    with client.as_current():
        # Read Parquet files
        ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=['proc_id', 'app', 'hostname', 'rank'])
        # Compute stats
        processes_df = ddf[ddf['proc_id'].isin(unique_proc_ids)] \
            .groupby(['proc_id']) \
            .agg({'app': min, 'hostname': min, 'rank': min}) \
            .compute()
        # Return as dict
        return processes_df.T.to_dict()


def filter_delta(df: pd.DataFrame, delta: float, metric='duration'):
    df[metric, 'csp'] = df[metric, 'sum'].cumsum() / df[metric, 'sum'].sum()
    df[metric, 'delta'] = df[metric, 'csp'].diff().fillna(df[metric, 'csp'])
    df_filtered = df[df[metric, 'delta'] > delta]
    df_filtered = df_filtered.reindex(sorted(df_filtered.columns), axis=1)
    return df_filtered


def format_df(df, stats_df: pd.DataFrame, add_xfer=True):
    df['duration', 'per'] = df.div(stats_df['duration', 'sum'].sum(), level=0)['duration', 'sum']
    if ('file_id', 'nunique') in df.columns:
        df['file_id', 'per'] = df.div(df['file_id', 'nunique'].max(), level=0)['file_id', 'nunique']
    if ('proc_id', 'nunique') in df.columns:
        df['proc_id', 'per'] = df.div(df['proc_id', 'nunique'].max(), level=0)['proc_id', 'nunique']
    if ('size', 'sum') in df.columns:
        df['bw', 'sum'] = df['size', 'sum'] / df['duration', 'sum']
        df['bw', 'per'] = df.div(df['bw', 'sum'].sum(), level=0)['bw', 'sum']
        df['size', 'per'] = df.div(stats_df['size', 'sum'].sum(), level=0)['size', 'sum']
    if ('index', 'count') in df.columns:
        df['index', 'per'] = df.div(stats_df['index', 'count'].sum(), level=0)['index', 'count']
    if add_xfer:
        df['xfer', 'max_fmt'] = pd.cut(df['size', 'max'], bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS, right=True)
        df['xfer', 'min_fmt'] = pd.cut(df['size', 'min'], bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS, right=True)
        df['xfer', 'mean_fmt'] = pd.cut(df['size', 'mean'], bins=XFER_SIZE_BINS, labels=XFER_SIZE_BIN_LABELS, right=True)
    if ('size', 'sum') in df.columns:
        df['bw', 'sum_fmt'] = df['bw', 'sum'].apply(lambda x: format(float(x)/1024.0/1024.0/1024.0, ".2f") + "GB/s")
        df['size', 'sum_fmt'] = df['size', 'sum'].apply(lambda x: format(float(x)/1024.0/1024.0/1024.0, ".2f") + "GB")
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
