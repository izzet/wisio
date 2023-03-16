import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Future, get_client
from typing import Dict, Union
from .constants import (
    CAT_POSIX,
    TIME_PRECISION,
    AccessPattern,
    IOCategory,
)


HLM_AGG = {
    'duration': [sum],
    'index': ['count'],
    'size': [min, max, sum],
}
IO_CATS = [io_cat.value for io_cat in list(IOCategory)]
DELTA_BINS = [
    0,
    0.001,
    0.01,
    0.1,
    0.25,
    0.5,
    0.75,
    1
]
DELTA_BIN_LABELS = [
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
PROC_COL = 'proc_name'
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
    view_types: list,
):
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
        .reset_index() \
        .persist()
    # Flatten column names
    hlm_view = _flatten_column_names(ddf=hlm_view)
    # Set derived columns
    hlm_view = _set_derived_columns(ddf=hlm_view)
    # Compute agg_view
    main_view = hlm_view \
        .drop(columns=extra_cols) \
        .groupby(view_types) \
        .sum() \
        .reset_index() \
        .persist()
    # Delete hlm_view
    del hlm_view
    # Return main_view
    return main_view


def compute_view(
    main_view: dd.DataFrame,
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
    parent_view = views[parent_type] if parent_type in views else main_view
    # Create colum names
    metric_col, delta_col = f"{metric}_sum", f"{metric}_delta"
    # Check view type
    if view_type is not PROC_COL:
        # Compute proc view first
        group_view = parent_view \
            .groupby([view_type, PROC_COL]) \
            .agg({metric_col: sum}) \
            .groupby([view_type]) \
            .max()
    else:
        # Compute group view
        group_view = parent_view \
            .groupby([view_type]) \
            .agg({metric_col: sum})
    # Set metric scores
    group_view = group_view \
        .map_partitions(set_metric_deltas, metric=metric, max_io_time=max_io_time) \
        .query(f"{delta_col} > @delta", local_dict={'delta': delta})
    # Find filtered records
    view = parent_view.query(f"{view_type} in @indices", local_dict={'indices': group_view.index.unique()})
    # Return view
    return view


def compute_max_io_time(main_view: dd.DataFrame):
    return main_view.groupby([PROC_COL]).sum()['duration_sum'].max()


def set_metric_deltas(df: pd.DataFrame, metric: str, max_io_time: float):
    metric_col, csp_col, delta_col = (
        f"{metric}_sum",
        f"{metric}_csp",
        f"{metric}_delta",
    )
    df[csp_col] = df[metric_col].cumsum() / max_io_time
    df[delta_col] = df[csp_col].diff().fillna(df[csp_col])
    return df


def set_metric_percentages(df: pd.DataFrame, metric: str, max_io_time: float):
    metric_col, pero_col, perr_col = (
        f"{metric}_sum",
        f"{metric}_pero",
        f"{metric}_perr"
    )
    df[pero_col] = df[metric_col] / max_io_time
    df[perr_col] = df[metric_col] / df[metric_col].sum()
    return df


def set_metric_scores(df: pd.DataFrame, metric: str, col: str):
    bin_col, score_col, threshold_col = (
        f"{metric}_bin",
        f"{metric}_score",
        f"{metric}_th",
    )
    df[bin_col] = np.digitize(df[col], bins=DELTA_BINS, right=True)
    df[score_col] = np.choose(df[bin_col] - 1, choices=DELTA_BIN_LABELS, mode='clip')
    df[threshold_col] = np.choose(df[bin_col] - 1, choices=DELTA_BINS, mode='clip')
    return df.drop(columns=[bin_col])


def set_tranges(df: pd.DataFrame, tranges: Union[Future, np.ndarray]):
    tranges = tranges.result() if isinstance(tranges, Future) else tranges
    return df.assign(trange=np.digitize(df['tmid'], bins=tranges, right=True))


def _compute_tranges(global_min_max: dict, precision=TIME_PRECISION):
    tmid_min, tmid_max = global_min_max['tmid']
    tranges = np.arange(tmid_min, tmid_max, precision)
    return get_client().scatter(tranges)


def _flatten_column_names(ddf: dd.DataFrame):
    ddf.columns = ['_'.join(tup).rstrip('_') for tup in ddf.columns.values]
    return ddf


def _set_derived_columns(ddf: dd.DataFrame):
    # Derive `io_cat` columns
    for col_suffix, col_value in zip(['time', 'size', 'count'], ['duration_sum', 'size_sum', 'index_count']):
        for io_cat in list(IOCategory):
            col_name = f"{io_cat.name.lower()}_{col_suffix}"
            ddf[col_name] = 0.0 if col_suffix is 'time' else 0
            ddf[col_name] = ddf[col_name].mask(ddf['io_cat'] == io_cat.value, ddf[col_value])
    # Derive `data` columns
    ddf['data_count'] = ddf['write_count'] + ddf['read_count']
    ddf['data_size'] = ddf['write_size'] + ddf['read_size']
    ddf['data_time'] = ddf['write_time'] + ddf['read_time']
    # Derive `acc_pat` columns
    for col_suffix, col_value in zip(['time', 'size', 'count'], ['data_time', 'data_size', 'data_count']):
        for acc_pat in list(AccessPattern):
            col_name = f"{acc_pat.name.lower()}_{col_suffix}"
            ddf[col_name] = 0.0 if col_suffix is 'time' else 0
            ddf[col_name] = ddf[col_name].mask(ddf['acc_pat'] == acc_pat.value, ddf[col_value])
    # Derive metadata operation columns
    for md_op in DERIVED_MD_OPS:
        col_name = f"{md_op}_time"
        ddf[col_name] = 0.0
        if md_op in ['close', 'open']:
            ddf[col_name] = ddf[col_name].mask(ddf['func_id'].str.contains(md_op) & ~ddf['func_id'].str.contains('dir'), ddf['duration_sum'])
        else:
            ddf[col_name] = ddf[col_name].mask(ddf['func_id'].str.contains(md_op), ddf['duration_sum'])
    # Return ddf
    return ddf
