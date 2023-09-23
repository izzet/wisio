import dask.dataframe as dd
import numpy as np
import os
import pandas as pd
from dask.distributed import Future, get_client
from typing import Tuple, Union
from ..types import (
    COL_FILE_NAME,
    COL_PROC_NAME,
    ViewNormalizationData,
    ViewResult,
    ViewType,
)
from .constants import (
    CAT_POSIX,
    TIME_PRECISION,
    AccessPattern,
    IOCategory,
)


ACC_PAT_SUFFIXES = ['time', 'size', 'count']
BW_BINS = [  # bw_ranges = [0, 1, 128, 1024, 1024*64]
    0,  # -- 'critical'
    1024 ** 2,  # 1MB -- 'very high'
    1024 ** 2 * 16,  # 16MB -- 'high',
    1024 ** 2 * 16 * 16,  # 256MB -- 'medium',
    1024 ** 3,  # 1GB -- 'low',
    1024 ** 3 * 16,  # 16GB -- 'very low',
    1024 ** 3 * 16 * 4,  # 64GB -- 'trivial',
    1024 ** 4  # 1TB -- 'none
]
BW_BINS_PER_PROC = [
    1,  # -- 'critical'
    1024 ** 2,  # 1MB -- 'very high'
    1024 ** 2 * 10,  # 10MB -- 'high'
    1024 ** 2 * 128,  # 128MB -- 'medium' --- fast hd
    1024 ** 2 * 256,  # 256MB -- 'low', --- nvme perf
    1024 ** 2 * 512,  # 512MB -- 'very low', --- hbm memory
    1024 ** 3,  # 1GB 'trivial' --- single thread bw for memory
    1024 ** 3 * 64,  # 64GB -- 'none', -- agg bw for memory
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
PROC_NAME_SEPARATOR = '#'
VIEW_AGG = {
    'bw': max,
    'data_count': sum,
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
    time_ranges = _compute_time_ranges(global_min_max=global_min_max)
    # Add `io_cat`, `acc_pat`, and `func_id` to groupby
    extra_cols = ['io_cat', 'acc_pat', 'func_id']
    groupby = view_types.copy()
    groupby.extend(extra_cols)
    # Compute high-level metrics
    hlm_view = ddf[(ddf['cat'] == CAT_POSIX) & (ddf['io_cat'].isin(IO_CATS))] \
        .map_partitions(set_time_ranges, time_ranges=time_ranges) \
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
    # Set hashed ids
    main_view['id'] = main_view.index.map(hash)
    # Delete hlm_view
    del hlm_view
    # Return main_view
    return main_view


def compute_group_view(
    parent_view: dd.DataFrame,
    view_type: str,
    metric_col: str,
    norm_data: ViewNormalizationData,
) -> Tuple[dd.DataFrame, ViewNormalizationData]:
    # Check view type
    if view_type is not COL_PROC_NAME:
        # Compute proc view first
        group_view = parent_view \
            .groupby([view_type, COL_PROC_NAME]) \
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

    # Compute normalization data
    if norm_data is None:
        norm_data = ViewNormalizationData(
            index_sum=group_view['index_count'].sum(),
            metric_max=group_view[metric_col].max()
        )

    # Set metric deltas & slope
    group_view = group_view \
        .sort_values(metric_col, ascending=False) \
        .map_partitions(set_metric_deltas, metric_col=metric_col, norm_data=norm_data) \
        .map_partitions(set_metric_slope, metric_col=metric_col)

    # Return group view & normalization data
    return group_view, norm_data


def compute_view(
    parent_view: dd.DataFrame,
    view_type: str,
    metric_col: str,
    norm_data: ViewNormalizationData,
    slope_threshold=45,
) -> ViewResult:
    # Compute group view
    group_view, norm_data = compute_group_view(
        parent_view=parent_view,
        view_type=view_type,
        metric_col=metric_col,
        norm_data=norm_data,
    )

    # Create columns
    metric = _extract_metric(metric_col=metric_col)
    query_col = f"{metric}_norm" if IS_NORMALIZED[metric] else f"{metric}_delta"
    slope_col = f"{metric}_slope"

    # Filter by slope
    filtered_view = group_view.query(f"{slope_col} < {slope_threshold}")

    # Find filtered records
    view = parent_view.query(
        f"{view_type} in @indices", local_dict={'indices': filtered_view.index.unique()})

    # Return views & normalization data
    return ViewResult(
        group_view=group_view,
        metric_col=metric_col,
        norm_data=norm_data,
        view=view,
        view_type=view_type,
    )


def compute_max_io_time(main_view: dd.DataFrame, time_col='duration_sum') -> dd.core.Scalar:
    return main_view.groupby([COL_PROC_NAME]).sum()[time_col].max()


def set_bound_columns(ddf: dd.DataFrame, is_initial=False):
    # Min(Peak IOPS, Peak I/O BW x I/O intensity) == higher the better
    # less than 25% of peak attainable performance -- reversed
    if not is_initial:
        ddf['bw_intensity'] = ddf['bw'] * ddf['intensity']
        ddf['att_perf'] = ddf[['iops', 'bw_intensity']].min(axis=1)

    # records less than %10 of attainable BW -- reversed
    ddf['bw'] = ddf['size_sum'] / ddf['duration_sum']

    # less than 25% of records -- reversed
    ddf['iops'] = ddf['data_count'] / ddf['duration_sum']

    # records which tend towards 1 >> 0.9
    ddf['intensity'] = 0.0
    ddf['intensity'] = ddf['intensity'].mask(
        ddf['size_sum'] > 0, ddf['index_count'] / ddf['size_sum'])

    if not is_initial:
        return ddf.drop(columns=['bw_intensity'])
    return ddf


def set_file_dir(df: pd.DataFrame):
    return df.assign(file_dir=df[COL_FILE_NAME].apply(lambda file_name: os.path.dirname(file_name)))


def set_file_regex(df: pd.DataFrame):
    return df.assign(file_regex=df[COL_FILE_NAME].replace(to_replace='[0-9]+', value=FILE_REGEX_PLACEHOLDER, regex=True))


def set_proc_name_parts(df: pd.DataFrame):
    return df \
        .assign(
            proc_name_parts=lambda df: df[COL_PROC_NAME].str.split(
                PROC_NAME_SEPARATOR),
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


def set_metric_deltas(df: pd.DataFrame, metric_col: str, norm_data: ViewNormalizationData):
    metric = _extract_metric(metric_col=metric_col)

    csp_col, delta_col, norm_col = (
        f"{metric}_csp",
        f"{metric}_delta",
        f"{metric}_norm",
    )

    if IS_NORMALIZED[metric]:
        if IS_REVERSED[metric]:
            df[norm_col] = 1 - df[metric_col] / norm_data.metric_max
            return df
        else:
            df[norm_col] = df[metric_col] / norm_data.metric_max
            return df

    df[csp_col] = df[metric_col].cumsum() / norm_data.metric_max
    df[delta_col] = df[csp_col].diff().fillna(df[csp_col])

    return df


def set_metric_slope(df: pd.DataFrame, metric_col: str):
    metric = _extract_metric(metric_col=metric_col)

    per_col, per_rev_cs_col, per_rev_cs_diff_col, slope_col, sum_col = (
        f"{metric}_per",
        f"{metric}_per_rev_cs",
        f"{metric}_per_rev_cs_diff",
        f"{metric}_slope",
        f"{metric_col}_sum",  # metric_col because of duration_sum
    )

    df['index_sum'] = df['index_count'].sum()
    df['index_cs'] = df['index_count'].cumsum()
    df['index_cs_per'] = 1 - df['index_cs'] / df['index_sum']
    df['index_cs_per_rev'] = 1 - df['index_cs_per']
    df['index_cs_per_rev_diff'] = df['index_cs_per_rev'].diff()

    df[sum_col] = df[metric_col].sum()
    df[per_col] = 1 - df[metric_col] / df[sum_col]
    df[per_rev_cs_col] = (1 - df[per_col]).cumsum()
    df[per_rev_cs_diff_col] = df[per_rev_cs_col].diff()

    df[slope_col] = np.rad2deg(np.arctan2(
        df['index_cs_per_rev_diff'], df[per_rev_cs_diff_col]))
    df[slope_col] = df[slope_col].fillna(0)

    return df


def set_metric_percentages(df: pd.DataFrame, metric_col: str, metric_max: float):
    metric = _extract_metric(metric_col=metric_col)

    pero_col, perr_col, norm_col = (
        f"{metric}_pero",
        f"{metric}_perr",
        f"{metric}_norm"
    )

    df[norm_col] = df[metric_col] / metric_max
    df[pero_col] = df[metric_col] / metric_max
    df[perr_col] = df[metric_col] / df[metric_col].sum()

    if IS_REVERSED[metric]:
        df[norm_col] = 1 - df[metric_col] / metric_max

    return df


def set_metric_scores(df: pd.DataFrame, view_type: ViewType, metric_col: str, col: str, metric_max=None):
    metric = _extract_metric(metric_col=metric_col)

    bin_col, score_col, th_col = (
        f"{metric}_bin",
        f"{metric}_score",
        f"{metric}_th",
    )

    bins = np.multiply(
        DELTA_BINS, metric_max) if IS_NORMALIZED[metric] else DELTA_BINS

    if metric == 'bw':
        bins = BW_BINS_PER_PROC if view_type == COL_PROC_NAME else BW_BINS

    if metric in ['bw', 'iops']:
        df = df.query(f"{metric} > 0")

    bin_names = np.flip(
        DELTA_BIN_NAMES) if IS_REVERSED[metric] else DELTA_BIN_NAMES
    th_bins = np.flip(DELTA_BINS) if IS_REVERSED[metric] else DELTA_BINS

    df[bin_col] = np.digitize(df[col], bins=bins, right=True)
    df[score_col] = np.choose(df[bin_col] - 1, choices=bin_names, mode='clip')
    df[th_col] = np.choose(df[bin_col] - 1, choices=th_bins, mode='clip')

    return df.drop(columns=[bin_col])


def set_time_ranges(df: pd.DataFrame, time_ranges: Union[Future, np.ndarray]):
    time_ranges = time_ranges.result() if isinstance(
        time_ranges, Future) else time_ranges
    return df.assign(time_range=np.digitize(df['tmid'], bins=time_ranges, right=True))


def _compute_time_ranges(global_min_max: dict, precision=TIME_PRECISION):
    tmid_min, tmid_max = global_min_max['tmid']
    time_ranges = np.arange(tmid_min, tmid_max, precision)
    return get_client().scatter(time_ranges)


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
            ddf[col_name] = ddf[col_name].mask(
                ddf['io_cat'] == io_cat.value, ddf[col_value])
    for io_cat in list(IOCategory):
        min_name, max_name = f"{io_cat.name.lower()}_min", f"{io_cat.name.lower()}_max"
        ddf[min_name] = 0
        ddf[max_name] = 0
        ddf[min_name] = ddf[min_name].mask(
            ddf['io_cat'] == io_cat.value, ddf['size_min'])
        ddf[max_name] = ddf[max_name].mask(
            ddf['io_cat'] == io_cat.value, ddf['size_max'])
    # Derive `data` columns
    ddf['data_count'] = ddf['write_count'] + ddf['read_count']
    ddf['data_size'] = ddf['write_size'] + ddf['read_size']
    ddf['data_time'] = ddf['write_time'] + ddf['read_time']
    # Derive `acc_pat` columns
    for col_suffix, col_value in zip(ACC_PAT_SUFFIXES, ['data_time', 'data_size', 'data_count']):
        for acc_pat in list(AccessPattern):
            col_name = f"{acc_pat.name.lower()}_{col_suffix}"
            ddf[col_name] = 0.0 if col_suffix == 'time' else 0
            ddf[col_name] = ddf[col_name].mask(
                ddf['acc_pat'] == acc_pat.value, ddf[col_value])
    # Derive metadata operation columns
    for col_suffix, col_value in zip(['time', 'count'], ['duration_sum', 'index_count']):
        for md_op in DERIVED_MD_OPS:
            col_name = f"{md_op}_{col_suffix}"
            ddf[col_name] = 0.0 if col_suffix == 'time' else 0
            if md_op in ['close', 'open']:
                ddf[col_name] = ddf[col_name].mask(ddf['func_id'].str.contains(
                    md_op) & ~ddf['func_id'].str.contains('dir'), ddf[col_value])
            else:
                ddf[col_name] = ddf[col_name].mask(
                    ddf['func_id'].str.contains(md_op), ddf[col_value])
    # Set bound columns
    set_bound_columns(ddf=ddf, is_initial=True)
    # Return ddf
    return ddf
