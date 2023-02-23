import dask.dataframe as dd
import numpy as np
import pandas as pd
from copy import copy
from typing import Dict
from .constants import TIME_PRECISION, AccessPattern, IOCategory


HLM_AGG = {
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
    # Add `io_cat` and `acc_pat` anyway
    groupby = view_types.copy()
    groupby.append('io_cat')
    groupby.append('acc_pat')
    # Prepare columns
    columns = list(HLM_AGG.keys())
    columns.extend(groupby)
    # Read Parquet files
    ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=_get_parquet_columns(columns))
    # Set tranges
    ddf = _set_tranges(ddf=ddf, global_min_max=global_min_max)
    # Fix types
    ddf = _fix_ddf_types(ddf=ddf)
    # Compute high-level metrics
    hlm_view = ddf[ddf['cat'] == 0] \
        .groupby(groupby) \
        .agg(HLM_AGG) \
        .reset_index()
    # Flatten column names
    hlm_view = _flatten_column_names(ddf=hlm_view)
    # Set derived columns
    hlm_view = _set_derived_columns(ddf=hlm_view)
    # Compute main_view
    main_view = hlm_view \
        .groupby(view_types) \
        .sum() \
        .reset_index() \
        .persist()
    # Return main_view
    return main_view


def compute_view(
    main_view: dd.DataFrame,
    view_types: list,
    views: Dict[tuple, dd.DataFrame],
    view_permutation: tuple,
    max_io_time: dd.core.Scalar,
    metric='duration',
    delta=0.0001,
):
    # Read types
    parent_type = view_permutation[:-1]
    view_type = view_permutation[-1]
    # Get parent view
    parent_view = views[parent_type]['expanded_view'] if parent_type in views else main_view
    # Create colum names
    metric_col, score_col, cut_col = f"{metric}_sum", f"{metric}_score", f"{metric}_cut"
    # Check view type
    if view_type is not 'proc_id':
        # Compute `proc_id` view first
        proc_id_view = parent_view \
            .groupby([view_type, 'proc_id']) \
            .agg({metric_col: sum})
        # Then compute group view
        group_view = proc_id_view \
            .groupby([view_type]) \
            .max()
    else:
        # Compute group view
        group_view = parent_view \
            .groupby([view_type]) \
            .agg({metric_col: sum}) \
            .sort_values(metric_col, ascending=False)
    # Filter view
    group_view = filter_delta(ddf=group_view, delta=delta, metric=metric)
    # Get score view
    score_view = group_view.reset_index()[[view_type, score_col, cut_col]]
    # Find filtered records and set duration scores
    expanded_view = parent_view \
        .query(f"{view_type}.isin(@indices)", local_dict={'indices': group_view.index.unique()}) \
        .drop(columns=[score_col, cut_col], errors='ignore') \
        .merge(score_view, on=[view_type])
    # Set metric percentages
    expanded_view = _set_metric_percentages(ddf=expanded_view, max_io_time=max_io_time, metric=metric)
    # Check view type
    if view_type is not 'proc_id':
        # Compute subview first
        subview = expanded_view \
            .groupby([view_type, 'proc_id']) \
            .sum() \
            .reset_index()
        # Compute agg columns
        agg_columns = {col: max if any(x in col for x in 'duration time'.split()) else sum for col in subview.columns}
        # Compute grouped view
        grouped_view = subview \
            .groupby([view_type]) \
            .agg(agg_columns)
    else:
        # Compute grouped view
        grouped_view = expanded_view \
            .groupby([view_type]) \
            .sum()
        # Compute subview
        subview = grouped_view.reset_index()
    # Persist grouped view
    grouped_view = grouped_view \
        .drop(columns=[*view_types, score_col, cut_col, 'io_cat', 'acc_pat'], errors='ignore') \
        .merge(group_view[[score_col, cut_col]], left_index=True, right_index=True)
    # Set metric percentages
    grouped_view = _set_metric_percentages(ddf=grouped_view, max_io_time=max_io_time, metric=metric)
    # Return views
    return dict(
        expanded_view=expanded_view.persist(),
        grouped_view=grouped_view.persist()
    )


def compute_max_io_time(main_view: dd.DataFrame):
    return main_view.groupby(['proc_id']).sum()['duration_sum'].max()


def compute_unique_filenames(log_dir: str):
    # Read Parquet files
    ddf = dd.read_parquet(f"{log_dir}/*.parquet", columns=['file_id', 'filename'])
    # Compute unique filenames
    unique_filenames = ddf \
        .groupby(['file_id']) \
        .agg({'filename': 'first'}) \
        .compute()
    # Return as dict
    return unique_filenames.T.to_dict()


def compute_unique_processes(log_dir: str):
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
    metric_col, csp_col, delta_col, score_col, cut_col = (
        f"{metric}_sum",
        f"{metric}_csp",
        f"{metric}_delta",
        f"{metric}_score",
        f"{metric}_cut"
    )

    def set_delta(df: pd.DataFrame):
        df[csp_col] = df[metric_col].cumsum() / df[metric_col].sum()
        df[delta_col] = df[csp_col].diff().fillna(df[csp_col])
        df[score_col] = np.digitize(df[delta_col], bins=DELTA_BINS, right=True)
        df[cut_col] = np.choose(df[score_col] - 1, choices=DELTA_BINS, mode='clip')
        return df
    ddf = ddf.map_partitions(set_delta)
    return ddf[ddf[delta_col] > delta]


def _fix_ddf_types(ddf: dd.DataFrame):
    ddf['acc_pat'] = ddf['acc_pat'].astype('i1')
    ddf['io_cat'] = ddf['io_cat'].astype('i1')
    return ddf


def _flatten_column_names(ddf: dd.DataFrame):
    ddf.columns = ['_'.join(tup).rstrip('_') for tup in ddf.columns.values]
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


def _get_parquet_columns(columns: list):
    columns = copy(columns)
    columns.extend(['cat', 'duration', 'tmid'])
    return list(set(map(lambda x: x.replace('trange', 'tmid'), columns)))


def _set_derived_columns(ddf: dd.DataFrame):
    # Derive `io_cat` columns
    for col_suffix, col_value in zip(['time', 'size', 'count'], ['duration_sum', 'size_sum', 'index_count']):
        for io_cat in list(IOCategory):
            col_name = f"{io_cat.name.lower()}_{col_suffix}"
            ddf[col_name] = 0
            ddf[col_name] = ddf[col_name].mask(ddf['io_cat'] == io_cat.value, ddf[col_value])
    # Derive `data` columns
    ddf['data_count'] = ddf['write_count'] + ddf['read_count']
    ddf['data_size'] = ddf['write_size'] + ddf['read_size']
    ddf['data_time'] = ddf['write_time'] + ddf['read_time']
    # Derive `acc_pat` columns
    for col_suffix, col_value in zip(['time', 'size', 'count'], ['data_time', 'data_size', 'data_count']):
        for acc_pat in list(AccessPattern):
            col_name = f"{acc_pat.name.lower()}_{col_suffix}"
            ddf[col_name] = 0
            ddf[col_name] = ddf[col_name].mask(ddf['acc_pat'] == acc_pat.value, ddf[col_value])
    # Return ddf
    return ddf


def _set_tranges(ddf: dd.DataFrame, global_min_max: dict):
    tmid_min, tmid_max = global_min_max['tmid']
    trange = np.arange(tmid_min, tmid_max, TIME_PRECISION)
    return ddf.map_partitions(lambda df: df.assign(trange=np.digitize(df['tmid'], trange, right=True)))


def _set_metric_percentages(ddf: dd.DataFrame, max_io_time: dd.core.Scalar, metric: str):
    metric_col, pero_col, perr_col = (
        f"{metric}_sum",
        f"{metric}_pero",
        f"{metric}_perr"
    )
    ddf[pero_col] = ddf[metric_col] / max_io_time
    ddf[perr_col] = ddf[metric_col] / ddf[metric_col].sum()
    return ddf
