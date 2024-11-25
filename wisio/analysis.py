import dask.dataframe as dd
import numpy as np
import pandas as pd
from typing import Callable, Dict, List, Union

from .constants import COL_PROC_NAME
from .metrics import KNOWN_METRICS
from .types import Metric, Score


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
# PERCENTILE_BINS = [0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1]
PERCENTILE_BINS = [0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9]
SLOPE_BINS: Dict[Metric, List[float]] = dict(
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
    time=[0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9],
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


def compute_time_boundaries(view: dd.DataFrame, view_type: str):
    time_cols = [col for col in view.columns if 'time' in col]
    view_types = view.index._meta.names
    if view_type not in view_types:
        raise ValueError(f"Cannot compute time boundary for view type: {view_type}")
    if len(view_types) == 1 and view_type in view_types:
        if view_type == COL_PROC_NAME:
            return view[time_cols].max()
        return view[time_cols].sum()
    elif COL_PROC_NAME in view_types:
        return view[time_cols].groupby(view_type).max().sum()
    return view[time_cols].groupby(view_type).sum()


def is_metric_time_bound(metric: Metric):
    return (
        metric in ['iops', 'ops', 'bw', 'time']
        or 'time_' in metric
        or '_bw' in metric
        or '_time' in metric
    )


def metric_time_column(metric: Metric):
    metric_col = metric.replace('_norm', '').replace('_per', '')
    return metric_col if '_time' in metric_col else 'time'


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
    ddf['intensity'] = ddf['intensity'].mask(
        ddf['size'] > 0, ddf['count'] / ddf['size']
    )

    if not is_initial:
        return ddf.drop(columns=['bw_intensity'])
    return ddf


def set_metric_scores(
    df: pd.DataFrame,
    metrics: List[Metric],
    metric_boundaries: Dict[Metric, float],
    is_slope_based: bool,
):
    for metric in metrics:
        if metric in KNOWN_METRICS:
            known_metric = KNOWN_METRICS[metric]
            df = known_metric.score(
                df,
                boundary=metric_boundaries[metric],
                slope=is_slope_based,
            )
        else:
            raise ValueError(f"Unknown metric: {metric}")
    return df


def set_metrics(
    df: pd.DataFrame,
    metrics: List[Metric],
    metric_boundaries: Dict[Metric, float],
    is_slope_based: bool,
):
    for metric in metrics:
        if metric in KNOWN_METRICS:
            known_metric = KNOWN_METRICS[metric]
            df = known_metric.calculate(
                df,
                boundary=metric_boundaries[metric],
                slope=is_slope_based,
            )
        else:
            raise ValueError(f"Unknown metric: {metric}")

    # 1. io_time == too trivial -- >50% threshold
    # 2. iops == slope analysis -- <45 degree roc (iops) as the main metric

    # automated bottleneck detection based on optimization function
    # one case is io_time (bin with old bins)
    # second case is iops (bin with roc bins)

    # iops = main metric
    # for the capability of automated bottleneck detection
    # absolute value of io time doesn't give us the rate of change depending on io_time/count

    return df


def set_unoverlapped_times(df: pd.DataFrame):
    required_columns = ['compute_time', 'app_io_time', 'io_time']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column {col} for unoverlapped time calculation")
    time_cols = [col for col in df.columns if '_time' in col]
    for time_col in time_cols:
        if 'u_' in time_col or '_per' in time_col:
            continue
        df[f'u_{time_col}'] = np.maximum(df[time_col] - df['compute_time'], 0)
    df['u_app_compute_time'] = np.maximum(df['compute_time'] - df['app_io_time'], 0)
    df['u_compute_time'] = np.maximum(df['compute_time'] - df['io_time'], 0)
    return df
