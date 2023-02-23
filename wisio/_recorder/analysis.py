import dask.dataframe as dd
import numpy as np
import pandas as pd
from copy import copy
from dask.distributed import Client
from typing import Dict
from ..utils.dask_agg import nunique
from .constants import TIME_PRECISION, IOCat


IO_CATS = [io_cat.value for io_cat in list(IOCat)]
HLM_AGG = {
    'acc_pat': [min, max],
    'duration': [sum],
    'index': ['count'],
    'size': [min, max, sum],
}
LLC_AGG = {
    'acc_pat': [min, max],
    'duration': [sum],
    'index': ['count'],
    'size': [min, max, sum],
}
DELTA_BINS = [
    0,
    0.01,
    0.1,
    0.25,
    0.5,
    0.75,
    1
]
DELTA_BIN_LABELS = [
    'none',
    'very low',
    'low',
    'medium',
    'high',
    'very high',
    'critical'
]
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


def compute_main_view(
    log_dir: str,
    global_min_max: dict,
    view_types: list
):
    # Add 'io_cat' anyway
    groupby = view_types.copy()
    groupby.append('io_cat')
    # Prepare columns
    columns = list(HLM_AGG.keys())
    columns.extend(groupby)
    # Read Parquet files
    ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=get_parquet_columns(columns))
    # Set trange
    ddf = set_derived_ddf_fields(ddf=ddf, global_min_max=global_min_max)
    # Fix types
    ddf = fix_ddf_types(ddf=ddf)
    # Compute hlm
    main_view = ddf[ddf['io_cat'].isin(IO_CATS)] \
        .groupby(groupby) \
        .agg(HLM_AGG) \
        .reset_index() \
        .persist()
    # Return dataframe
    return main_view


def compute_view(
    main_view: dd.DataFrame,
    views: Dict[tuple, dd.DataFrame],
    view_permutation: tuple,
    metric='duration',
    delta=0.0001,
):
    # Read types
    parent_type = view_permutation[:-1]
    view_type = view_permutation[-1]
    # Get parent view
    parent_view = views[parent_type] if parent_type in views else main_view
    # Compute view
    group_view = parent_view \
        .groupby([view_type]) \
        .sum() \
        .sort_values((metric, 'sum'), ascending=False)
    # Filter view
    group_view = filter_delta(ddf=group_view, delta=delta, metric=metric)
    # Prepare columns
    view_column, metric_column, cut_column = (view_type, ''), (metric, 'score'), (metric, 'cut')
    # Get score view
    score_view = group_view.reset_index()[[view_column, metric_column, cut_column]]
    # Find filtered records and set duration scores
    view = parent_view \
        .query(f"`{view_column}`.isin(@indices)", local_dict={'indices': group_view.index.unique()}) \
        .drop(columns=[metric_column, cut_column], errors='ignore') \
        .merge(score_view, on=[view_column])
    # Set metric percentages
    view = set_metric_percentages(ddf=view, main_view=main_view, metric=metric)
    # Return view
    return view.persist()


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
    log_dir: str,
    unique_file_ids: list,
):
    # Read Parquet files
    ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=['file_id', 'filename'])
    # Compute unique filenames
    unique_filenames = ddf \
        .groupby(['file_id']) \
        .agg({'filename': 'first'}) \
        .compute()
    # Return as dict
    return unique_filenames.T.to_dict()


def compute_unique_processes(
    log_dir: str,
    unique_proc_ids: list,
):
    # Read Parquet files
    ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=['proc_id', 'app', 'hostname', 'rank'])
    # Compute unique processes
    unique_processes = ddf \
        .groupby(['proc_id']) \
        .agg({
            'app': 'first',
            'hostname': 'first',
            'rank': 'first'
        }) \
        .compute()
    # Return as dict
    return unique_processes.T.to_dict()


def filter_delta(ddf: dd.DataFrame, delta: float, metric='duration'):
    def set_delta(df: pd.DataFrame):
        df[metric, 'csp'] = df[metric, 'sum'].cumsum() / df[metric, 'sum'].sum()
        df[metric, 'delta'] = df[metric, 'csp'].diff().fillna(df[metric, 'csp'])
        df[metric, 'score'] = np.digitize(df[metric, 'delta'], bins=DELTA_BINS, right=True)
        df[metric, 'cut'] = np.choose(df[metric, 'score'] - 1, choices=DELTA_BINS, mode='clip')
        return df
    ddf = ddf.map_partitions(set_delta)
    return ddf[ddf[metric, 'delta'] > delta]


def fix_ddf_types(ddf: dd.DataFrame):
    ddf['acc_pat'] = ddf['acc_pat'].astype('i1')
    ddf['io_cat'] = ddf['io_cat'].astype('i1')
    return ddf


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
    return ddf.map_partitions(lambda df: df.assign(trange=np.digitize(df['tmid'], trange, right=True)))


def set_metric_percentages(ddf: dd.DataFrame, main_view: dd.DataFrame, metric: str):
    def set_metric_percentage(df: pd.DataFrame):
        df[metric, 'pero'] = df[metric, 'sum'] / main_view[metric, 'sum'].sum()
        df[metric, 'perr'] = df[metric, 'sum'] / df[metric, 'sum'].sum()
        return df
    return ddf.map_partitions(set_metric_percentage)
