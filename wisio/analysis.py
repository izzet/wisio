import dask.dataframe as dd
import numpy as np
import pandas as pd
from typing import Callable, Dict, List, Union
from .types import Metric, Score, ViewType


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
IS_NORMALIZED: Dict[Metric, bool] = dict(
    att_perf=True,
    bw=True,
    intensity=True,
    iops=True,
    time=False,
)
IS_REVERSED: Dict[Metric, bool] = dict(
    att_perf=True,
    bw=True,
    intensity=False,
    iops=True,
    time=False,
)
SCORE_BINS: Dict[Metric, List[float]] = dict(
    iops=[
        np.tan(np.deg2rad(80)),  # 5.67128182
        np.tan(np.deg2rad(70)),  # 2.74747742
        np.tan(np.deg2rad(60)),  # 1.73205081
        np.tan(np.deg2rad(50)),  # 1.19175359
        np.tan(np.deg2rad(40)),  # 0.83909963
        np.tan(np.deg2rad(30)),  # 0.57735027
        np.tan(np.deg2rad(20)),  # 0.36397023
        np.tan(np.deg2rad(10)),  # 0.17632698
    ],
    time=[
        0,
        0.001,
        0.01,
        0.1,
        0.25,
        0.5,
        0.75,
        0.9
    ]
)
SCORE_INITIALS = {
    'none': 'NA',
    'trivial': 'TR',
    'very low': 'VL',
    'low': 'LO',
    'medium': 'MD',
    'high': 'HI',
    'very high': 'VH',
    'critical': 'CR',
}
SCORE_NAMES = [
    Score.NONE.value,
    Score.TRIVIAL.value,
    Score.VERY_LOW.value,
    Score.LOW.value,
    Score.MEDIUM.value,
    Score.HIGH.value,
    Score.VERY_HIGH.value,
    Score.CRITICAL.value,
]
THRESHOLD_FUNCTIONS: Dict[Metric, Callable[[int], Union[float, int]]] = dict(
    iops=lambda x: np.tan(np.deg2rad(x)),
    time=lambda x: x,
)


def set_bound_columns(ddf: Union[dd.DataFrame, pd.DataFrame], is_initial=False):
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
    ddf['intensity'] = ddf['intensity'].mask(ddf['size'] > 0, ddf['count'] / ddf['size'])

    if not is_initial:
        return ddf.drop(columns=['bw_intensity'])
    return ddf


def set_metric_scores(df: pd.DataFrame, view_type: ViewType, metric: Metric, metric_boundary=None):
    bin_col, score_col, slope_col = (
        f"{metric}_bin",
        f"{metric}_score",
        f"{metric}_slope",
    )

    bins = SCORE_BINS[metric]
    names = SCORE_NAMES

    # if IS_NORMALIZED[metric]:
    #     bins = np.multiply(bins, metric_boundary)

    # if IS_REVERSED[metric]:
    #     names = np.flip(names)
    #     thresholds = np.flip(thresholds)

    # if metric == 'bw':
    #     bins = BW_BINS_PER_PROC if view_type == COL_PROC_NAME else BW_BINS

    # if metric in ['bw', 'iops']:
    #     df = df.query(f"{metric} > 0")

    df[bin_col] = np.digitize(df[slope_col], bins=bins, right=True)
    df[score_col] = np.choose(df[bin_col] - 1, choices=names, mode='clip')

    return df


def set_metric_slope(df: pd.DataFrame, metric: Metric):
    slope_col = f"{metric}_slope"

    if metric == 'iops':
        df['time_per'] = df['time'] / df['time'].sum()
        df['count_per'] = df['count'] / df['count'].sum()
        df[slope_col] = df['count_per'] / df['time_per']
    elif metric == 'time':
        df[slope_col] = df[metric] / df[metric].sum()

    # 1. io_time == too trivial -- >50% threshold
    # 2. iops == slope analysis -- <45 degree roc (iops) as the main metric

    # automated bottleneck detection based on optimization function
    # one case is io_time (bin with old bins)
    # second case is iops (bin with roc bins)

    # iops = main metric
    # for the capability of automated bottleneck detection
    # absolute value of io time doesn't give us the rate of change depending on io_time/count

    return df
