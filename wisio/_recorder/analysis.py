import dask.dataframe as dd
import numpy as np
import os
import pandas as pd
from dask.distributed import Future, get_client
from typing import Union
from .constants import (
    CAT_POSIX,
    TIME_PRECISION,
    AccessPattern,
    IOCategory,
)


ACC_PAT_SUFFIXES = ['time', 'size', 'count']
APP_NAME_COL = 'app_name'
BW_BINS = [  # bw_ranges = [0, 1, 128, 1024, 1024*64]
    0,  # -- 'critical'
    1024 ** 2,  # 1MB -- 'very high'
    1024 ** 2 * 16,  # 16MB -- 'high',
    1024 ** 2 * 16 * 16,  # 256MB -- 'medium',
    1024 ** 3,  # 1GB -- 'low',
    1024 ** 3 * 16,  # 16GB -- 'very low',
    1024 ** 3 * 16 * 4,  # 64GB -- 'trivial',
    1024 ** 4  # 1TB
]
DELTA_BINS = [
    0,
    0.001,
    0.01,
    0.1,
    0.25,
    0.5,
    0.75,
    0.9
]
DELTA_BIN_NAMES = [
    'none',
    'trivial',
    'very low',
    'low',
    'medium',
    'high',
    'very high',
    'critical'
]
DERIVED_MD_OPS = ['close', 'open', 'seek', 'stat']
FILE_COL = 'file_name'
FILE_DIR_COL = 'file_dir'
FILE_REGEX_COL = 'file_regex'
FILE_REGEX_PLACEHOLDER = '[0-9]'
HLM_AGG = {
    'duration': [sum],
    'index': ['count'],
    'size': [min, max, sum],
}
IO_CATS = [io_cat.value for io_cat in list(IOCategory)]
IO_TYPES = ['read', 'write', 'metadata']
IS_NORMALIZED = dict(
    duration=False,
    bw=True,
    iops=True,
    intensity=True,
    att_perf=True
)
IS_REVERSED = dict(
    duration=False,
    bw=True,
    iops=True,
    intensity=False,
    att_perf=True
)
NODE_NAME_COL = 'node_name'
PROC_COL = 'proc_name'
PROC_NAME_SEPARATOR = '#'
RANK_COL = 'rank'
TRANGE_COL = 'trange'
VIEW_AGG = {
    'bw': max,
    'duration_sum': sum,
    'index_count': sum,
    'intensity': max,
    'iops': max,
    'size_sum': sum,
}


def compute_main_view(
    log_dir: str,
    global_min_max: dict,
    view_types: list,
    persist=True
) -> dd.DataFrame:
    # Read Parquet files
    ddf = dd.read_parquet(f"{log_dir}/*.parquet")
    # Fix dtypes
    ddf['acc_pat'] = ddf['acc_pat'].astype(np.uint8)
    ddf['duration'] = ddf['duration'].astype(np.float64)
    ddf['io_cat'] = ddf['io_cat'].astype(np.uint8)
    # Compute tranges
    tranges = _compute_tranges(global_min_max=global_min_max)
    # Add `io_cat`, `acc_pat`, and `func_id` to groupby
    extra_cols = ['io_cat', 'acc_pat', 'func_id']
    groupby = view_types.copy()
    groupby.extend(extra_cols)
    # Compute high-level metrics
    hlm_view = ddf[(ddf['cat'] == CAT_POSIX) & (ddf['io_cat'].isin(IO_CATS))] \
        .map_partitions(set_tranges, tranges=tranges) \
        .groupby(groupby) \
        .agg(HLM_AGG) \
        .reset_index()
    if persist:
        hlm_view = hlm_view.persist()
    # Flatten column names
    hlm_view = _flatten_column_names(ddf=hlm_view)
    # Set derived columns
    hlm_view = _set_derived_columns(ddf=hlm_view)
    # Compute agg_view
    main_view = hlm_view \
        .drop(columns=extra_cols) \
        .groupby(view_types) \
        .sum()
    if persist:
        main_view = main_view.persist()
    # Delete hlm_view
    del hlm_view
    # Return main_view
    return main_view


def compute_group_view(
    parent_view: dd.DataFrame,
    view_type: str,
    metric_col: str,
    metric_max: dd.core.Scalar,
) -> tuple[dd.DataFrame, dd.core.Scalar]:
    # Check view type
    if view_type is not PROC_COL:
        # Compute proc view first
        group_view = parent_view \
            .groupby([view_type, PROC_COL]) \
            .agg(VIEW_AGG) \
            .map_partitions(set_bound_columns) \
            .groupby([view_type]) \
            .max()
    else:
        # Compute group view
        group_view = parent_view \
            .groupby([view_type]) \
            .agg(VIEW_AGG) \
            .map_partitions(set_bound_columns)

    # Compute metric max
    if metric_max is None:
        metric_max = group_view[metric_col].max()

    # Set metric deltas
    group_view = group_view.map_partitions(
        set_metric_deltas,
        metric_col=metric_col,
        metric_max=metric_max,
    )

    # Return group view & metric max
    return group_view, metric_max


def compute_view(
    parent_view: dd.DataFrame,
    view_type: str,
    metric_col: str,
    metric_max: dd.core.Scalar,
    cutoff=0.0001,
) -> dd.DataFrame:
    # Compute group view
    group_view, metric_max = compute_group_view(
        parent_view=parent_view,
        view_type=view_type,
        metric_col=metric_col,
        metric_max=metric_max,
    )

    # Create query column
    metric = _extract_metric(metric_col=metric_col)
    query_col = f"{metric}_norm" if IS_NORMALIZED[metric] else f"{metric}_delta"

    # Get the first index in case all the records get cut off
    first_ix = group_view.index.head(1, compute=False).to_series().min()

    if IS_REVERSED[metric]:
        # group_view = group_view.query(f"{query_col} < @cutoff | index == @first_ix", local_dict={'cutoff': 1 - cutoff, 'first_ix': first_ix})
        group_view = group_view.query(f"{query_col} < @cutoff", local_dict={'cutoff': 1 - cutoff})
    else:
        # group_view = group_view.query(f"{query_col} > @cutoff | index == @first_ix", local_dict={'cutoff': cutoff, 'first_ix': first_ix})
        group_view = group_view.query(f"{query_col} > @cutoff", local_dict={'cutoff': cutoff})

    # Find filtered records
    view = parent_view.query(f"{view_type} in @indices", local_dict={'indices': group_view.index.unique()})

    # Return view & metric max
    return view, metric_max


def compute_max_io_time(main_view: dd.DataFrame, time_col='duration_sum') -> dd.core.Scalar:
    return main_view.groupby([PROC_COL]).sum()[time_col].max()


def set_bound_columns(ddf: dd.DataFrame, is_initial=False):
    # Min(Peak IOPS, Peak I/O BW x I/O intensity) == higher the better
    # less than 25% of peak attainable performance -- reversed
    if not is_initial:
        ddf['bw_intensity'] = ddf['bw'] * ddf['intensity']
        ddf['att_perf'] = ddf[['iops', 'bw_intensity']].min(axis=1)

    # records less than %10 of attainable BW -- reversed
    ddf['bw'] = ddf['size_sum'] / ddf['duration_sum']

    # less than 25% of records -- reversed
    ddf['iops'] = ddf['index_count'] / ddf['duration_sum']

    # records which tend towards 1 >> 0.9
    ddf['intensity'] = 0.0
    ddf['intensity'] = ddf['intensity'].mask(ddf['size_sum'] > 0, ddf['index_count'] / ddf['size_sum'])

    if not is_initial:
        return ddf.drop(columns=['bw_intensity'])
    return ddf


def set_file_dir(df: pd.DataFrame):
    return df.assign(file_dir=df[FILE_COL].apply(lambda file_name: os.path.dirname(file_name)))


def set_file_regex(df: pd.DataFrame):
    return df.assign(file_regex=df[FILE_COL].replace(to_replace='[0-9]+', value=FILE_REGEX_PLACEHOLDER, regex=True))


def set_proc_name_parts(df: pd.DataFrame):
    return df \
        .assign(
            proc_name_parts=lambda df: df[PROC_COL].str.split(PROC_NAME_SEPARATOR),
            app_name=lambda df: df.proc_name_parts.str[0].astype(str),
            node_name=lambda df: df.proc_name_parts.str[1].astype(str),
            rank=lambda df: df.proc_name_parts.str[2].astype(str),
        ) \
        .drop(columns=['proc_name_parts'])


def set_logical_columns(view: dd.DataFrame) -> dd.DataFrame:
    return view \
        .reset_index() \
        .map_partitions(set_proc_name_parts) \
        .map_partitions(set_file_dir) \
        .map_partitions(set_file_regex)


def set_metric_deltas(df: pd.DataFrame, metric_col: str, metric_max: float):
    metric = _extract_metric(metric_col=metric_col)

    csp_col, delta_col, norm_col = (
        f"{metric}_csp",
        f"{metric}_delta",
        f"{metric}_norm",
    )

    if IS_NORMALIZED[metric]:
        if IS_REVERSED[metric]:
            df[norm_col] = 1 - df[metric_col] / metric_max
            return df
        else:
            df[norm_col] = df[metric_col] / metric_max
            return df

    df[csp_col] = df[metric_col].cumsum() / metric_max
    df[delta_col] = df[csp_col].diff().fillna(df[csp_col])

    return df


def set_metric_percentages(df: pd.DataFrame, metric_col: str, metric_max: float):
    metric = _extract_metric(metric_col=metric_col)

    pero_col, perr_col, norm_col = (
        f"{metric}_pero",
        f"{metric}_perr",
        f"{metric}_norm"
    )

    if IS_REVERSED[metric]:
        df[norm_col] = 1 - df[metric_col] / metric_max
        return df

    df[pero_col] = df[metric_col] / metric_max
    df[perr_col] = df[metric_col] / df[metric_col].sum()

    return df


def set_metric_scores(df: pd.DataFrame, metric_col: str, col: str, metric_max=None):
    metric = _extract_metric(metric_col=metric_col)

    bin_col, score_col, th_col = (
        f"{metric}_bin",
        f"{metric}_score",
        f"{metric}_th",
    )

    bins = np.multiply(DELTA_BINS, metric_max) if IS_NORMALIZED[metric] else DELTA_BINS
    if metric == 'bw':
        bins = BW_BINS

    bin_names = np.flip(DELTA_BIN_NAMES) if IS_REVERSED[metric] else DELTA_BIN_NAMES
    th_bins = np.flip(DELTA_BINS) if IS_REVERSED[metric] else DELTA_BINS

    df[bin_col] = np.digitize(df[col], bins=bins, right=True)
    df[score_col] = np.choose(df[bin_col] - 1, choices=bin_names, mode='clip')
    df[th_col] = np.choose(df[bin_col] - 1, choices=th_bins, mode='clip')

    return df.drop(columns=[bin_col])


def set_tranges(df: pd.DataFrame, tranges: Union[Future, np.ndarray]):
    tranges = tranges.result() if isinstance(tranges, Future) else tranges
    return df.assign(trange=np.digitize(df['tmid'], bins=tranges, right=True))


def _compute_tranges(global_min_max: dict, precision=TIME_PRECISION):
    tmid_min, tmid_max = global_min_max['tmid']
    tranges = np.arange(tmid_min, tmid_max, precision)
    return get_client().scatter(tranges)


def _extract_metric(metric_col: str):
    return metric_col.replace('_sum', '') if '_sum' in metric_col else metric_col


def _flatten_column_names(ddf: dd.DataFrame):
    ddf.columns = ['_'.join(tup).rstrip('_') for tup in ddf.columns.values]
    return ddf


def _set_derived_columns(ddf: dd.DataFrame):
    # Derive `io_cat` columns
    for col_suffix, col_value in zip(['time', 'size', 'count'], ['duration_sum', 'size_sum', 'index_count']):
        for io_cat in list(IOCategory):
            col_name = f"{io_cat.name.lower()}_{col_suffix}"
            ddf[col_name] = 0.0 if col_suffix == 'time' else 0
            ddf[col_name] = ddf[col_name].mask(ddf['io_cat'] == io_cat.value, ddf[col_value])
    for io_cat in list(IOCategory):
        min_name, max_name = f"{io_cat.name.lower()}_min", f"{io_cat.name.lower()}_max"
        ddf[min_name] = 0
        ddf[max_name] = 0
        ddf[min_name] = ddf[min_name].mask(ddf['io_cat'] == io_cat.value, ddf['size_min'])
        ddf[max_name] = ddf[max_name].mask(ddf['io_cat'] == io_cat.value, ddf['size_max'])
    # Derive `data` columns
    ddf['data_count'] = ddf['write_count'] + ddf['read_count']
    ddf['data_size'] = ddf['write_size'] + ddf['read_size']
    ddf['data_time'] = ddf['write_time'] + ddf['read_time']
    # Derive `acc_pat` columns
    for col_suffix, col_value in zip(ACC_PAT_SUFFIXES, ['data_time', 'data_size', 'data_count']):
        for acc_pat in list(AccessPattern):
            col_name = f"{acc_pat.name.lower()}_{col_suffix}"
            ddf[col_name] = 0.0 if col_suffix == 'time' else 0
            ddf[col_name] = ddf[col_name].mask(ddf['acc_pat'] == acc_pat.value, ddf[col_value])
    # Derive metadata operation columns
    for col_suffix, col_value in zip(['time', 'count'], ['duration_sum', 'index_count']):
        for md_op in DERIVED_MD_OPS:
            col_name = f"{md_op}_{col_suffix}"
            ddf[col_name] = 0.0 if col_suffix == 'time' else 0
            if md_op in ['close', 'open']:
                ddf[col_name] = ddf[col_name].mask(ddf['func_id'].str.contains(md_op) & ~ddf['func_id'].str.contains('dir'), ddf[col_value])
            else:
                ddf[col_name] = ddf[col_name].mask(ddf['func_id'].str.contains(md_op), ddf[col_value])
    # Set bound columns
    set_bound_columns(ddf=ddf, is_initial=True)
    # Return ddf
    return ddf
