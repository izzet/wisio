import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.dataframe import to_numeric, from_delayed
from .constants import COL_PROC_NAME
from .types import ViewType


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
IS_NORMALIZED = dict(
    att_perf=True,
    bw=True,
    intensity=True,
    iops=True,
    time=False,
)
IS_REVERSED = dict(
    att_perf=True,
    bw=True,
    intensity=False,
    iops=True,
    time=False,
)


def set_bound_columns(ddf: dd.DataFrame, is_initial=False):
    # Min(Peak IOPS, Peak I/O BW x I/O intensity) == higher the better
    # less than 25% of peak attainable performance -- reversed
    if not is_initial:
        ddf['bw_intensity'] = ddf['bw'] * ddf['intensity']
        ddf['att_perf'] = ddf[['iops', 'bw_intensity']].min(axis=1)

    # records less than %10 of attainable BW -- reversed
    ddf['bw'] = ddf['size'] / ddf['time']

    # less than 25% of records -- reversed
    ddf['iops'] = ddf['data_count'] / ddf['time']

    # records which tend towards 1 >> 0.9
    ddf['intensity'] = 0.0
    ddf['intensity'] = ddf['intensity'].mask(
        ddf['size'] > 0, ddf['count'] / ddf['size'])

    if not is_initial:
        return ddf.drop(columns=['bw_intensity'])
    return ddf


def set_metric_deltas(df: pd.DataFrame, metric: str, metric_boundary: dd.core.Scalar):
    csp_col, delta_col, norm_col = (
        f"{metric}_csp",
        f"{metric}_delta",
        f"{metric}_norm",
    )

    # print('set_metric_deltas', metric, type(norm_data), norm_data.metric_max if norm_data is not None else -1)

    # fix: The columns in the computed data do not match the columns in the provided metadata
    df[csp_col] = 0.0
    df[delta_col] = 0.0

    if IS_NORMALIZED[metric]:
        if IS_REVERSED[metric]:
            df[norm_col] = 1 - df[metric] / metric_boundary
            return df
        else:
            df[norm_col] = df[metric] / metric_boundary
            return df

    df[csp_col] = df[metric].cumsum() / metric_boundary
    df[delta_col] = df[csp_col].diff().fillna(df[csp_col])

    return df


def set_metric_percentages(df: pd.DataFrame, metric: str, metric_boundary: float):
    pero_col, perr_col, norm_col = (
        f"{metric}_pero",
        f"{metric}_perr",
        f"{metric}_norm"
    )

    df[norm_col] = df[metric] / metric_boundary
    df[pero_col] = df[metric] / metric_boundary
    df[perr_col] = df[metric] / df[metric].sum()

    if IS_REVERSED[metric]:
        df[norm_col] = 1 - df[metric] / metric_boundary

    return df


def set_metric_scores(df: pd.DataFrame, view_type: ViewType, metric: str, value_col: str, metric_boundary=None):
    bin_col, score_col, th_col = (
        f"{metric}_bin",
        f"{metric}_score",
        f"{metric}_threshold",
    )

    bin_names = DELTA_BIN_NAMES
    bins = DELTA_BINS
    bins_th = DELTA_BINS

    if IS_NORMALIZED[metric]:
        bins = np.multiply(DELTA_BINS, metric_boundary)

    if IS_REVERSED[metric]:
        bin_names = np.flip(DELTA_BIN_NAMES)
        bins_th = np.flip(DELTA_BINS)

    if metric == 'bw':
        bins = BW_BINS_PER_PROC if view_type == COL_PROC_NAME else BW_BINS

    if metric in ['bw', 'iops']:
        df = df.query(f"{metric} > 0")

    df[bin_col] = np.digitize(df[value_col], bins=bins, right=True)
    df[score_col] = np.choose(df[bin_col] - 1, choices=bin_names, mode='clip')
    df[th_col] = np.choose(df[bin_col] - 1, choices=bins_th, mode='clip')

    return df.drop(columns=[bin_col])


def set_metric_slope(df: pd.DataFrame, metric: str):
    per_col, per_rev_cs_col, per_rev_cs_diff_col, slope_col, sum_col = (
        f"{metric}_per",
        f"{metric}_per_rev_cs",
        f"{metric}_per_rev_cs_diff",
        f"{metric}_slope",
        f"{metric}_sum"
    )

    df['count_sum'] = df['count'].sum()
    df['count_cs'] = df['count'].cumsum()
    df['count_cs_per'] = 1 - df['count_cs'] / df['count_sum']
    df['count_cs_per_rev'] = 1 - df['count_cs_per']
    df['count_cs_per_rev_diff'] = df['count_cs_per_rev'].diff().fillna(0)

    df[sum_col] = df[metric].sum()
    df[per_col] = 1 - df[metric] / df[sum_col]
    df[per_rev_cs_col] = (1 - df[per_col]).cumsum()
    df[per_rev_cs_diff_col] = df[per_rev_cs_col].diff().fillna(0)

    df[slope_col] = np.rad2deg(np.arctan2(
        df['count_cs_per_rev_diff'], df[per_rev_cs_diff_col]))
    df[slope_col] = df[slope_col].fillna(0)

    return df
