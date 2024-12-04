import abc
import dask.dataframe as dd
import numpy as np
import pandas as pd
from typing import Any, Dict, List

from .constants import COL_PROC_NAME
from .types import Layer, RawStats, Score


BW_BINS = [  # bw_ranges = [0, 1, 128, 1024, 1024*64]
    0,  # -- 'critical'
    1024**2,  # 1MB -- 'very high'
    1024**2 * 16,  # 16MB -- 'high',
    1024**2 * 16 * 16,  # 256MB -- 'medium',
    1024**3,  # 1GB -- 'low',
    1024**3 * 16,  # 16GB -- 'very low',
    1024**3 * 16 * 4,  # 64GB -- 'trivial',
    1024**4,  # 1TB -- 'none
]
BW_BINS_PER_PROC = [
    0,  # -- 'critical'
    1024**2,  # 1MB -- 'very high'
    1024**2 * 10,  # 10MB -- 'high'
    1024**2 * 128,  # 128MB -- 'medium' --- fast hd
    1024**2 * 256,  # 256MB -- 'low', --- nvme perf
    1024**2 * 512,  # 512MB -- 'very low', --- hbm memory
    1024**3,  # 1GB 'trivial' --- single thread bw for memory
    1024**3 * 64,  # 64GB -- 'none', -- agg bw for memory
]
# np.percentile(np.linspace(0, 1, 1000), [0, 10, 25, 50, 75, 90, 95, 99])
INTENSITY_BINS = [0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
PERCENTAGE_BINS = [0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9]
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
# SCORE_BINS = np.linspace(0, 1, num=len(SCORE_NAMES))
SCORE_BINS = [1, 2, 3, 4, 5, 6, 7]
SLOPE_BINS = [
    np.tan(np.deg2rad(80)),  # 5.67128182
    np.tan(np.deg2rad(70)),  # 2.74747742
    np.tan(np.deg2rad(60)),  # 1.73205081
    np.tan(np.deg2rad(50)),  # 1.19175359
    np.tan(np.deg2rad(40)),  # 0.83909963
    np.tan(np.deg2rad(30)),  # 0.57735027
    np.tan(np.deg2rad(20)),  # 0.36397023
    np.tan(np.deg2rad(10)),  # 0.17632698
]


class Metric(abc.ABC):
    def __init__(
        self, name: str, is_normalized: bool = False, is_reversed: bool = False
    ):
        self.name = name
        self.is_normalized = is_normalized
        self.is_reversed = is_reversed

    @abc.abstractmethod
    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        raise NotImplementedError

    def score_slope(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self._score(df, value_col, SLOPE_BINS, SCORE_BINS, True)

    def score_percentage(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self._score(df, value_col, PERCENTAGE_BINS, SCORE_BINS, False)

    def score_percentile(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self.score_percentage(df, value_col)

    def metric_column(self, slope=False) -> str:
        if slope:
            return f"{self.name}_slope"
        return f"{self.name}_per"

    def score_column(self, slope=False) -> str:
        return f"{self.metric_column(slope)}_score"

    def __str__(self):
        return self.name

    def _score(
        self,
        df: pd.DataFrame,
        value_col: str,
        bins: List[Any],
        names: List[str],
        slope: bool = False,
    ) -> pd.DataFrame:
        bin_col = f"{self.metric_column(slope)}_bin"
        score_col = self.score_column(slope)
        df[bin_col] = np.digitize(df[value_col], bins=bins, right=True)
        df[score_col] = np.choose(df[bin_col] - 1, choices=names, mode='clip')
        return df.drop(columns=[bin_col])


class NormalizedMetric(Metric):
    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df[self.metric_column()] = df[self.name] / boundary
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, self.metric_column())

    def metric_column(self, slope=False) -> str:
        return f"{self.name}_norm"


class PercentageMetric(Metric):
    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df[self.metric_column()] = df[self.name] / df[self.name].sum()
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, self.metric_column())

    def metric_column(self, slope=False) -> str:
        return f"{self.name}_per"


class RatioMetric(Metric):
    def __init__(self, name: str, val_col: str, div_col: str):
        super().__init__(name)
        self.val_col = val_col
        self.div_col = div_col

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df[self.name] = 0.0
        df[self.name] = df[self.name].mask(
            df[self.div_col] > 0,
            df[self.val_col] / df[self.div_col],
        )
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, self.name)

    def metric_column(self, slope=False) -> str:
        return self.name


class BandwidthMetric(Metric):
    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df[self.metric_column()] = df[f"{self.name}_size"] / df[f"{self.name}_time"]
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        bins = BW_BINS_PER_PROC if COL_PROC_NAME in df.index.names else BW_BINS
        return self._score(
            df=df,
            value_col=self.metric_column(),
            bins=bins,
            names=np.flip(SCORE_NAMES),
        )

    def metric_column(self, slope=False) -> str:
        return f"{self.name}_bw"


class OpsMetric(Metric):
    def __init__(self):
        super().__init__("ops", is_normalized=True)

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df["time_per"] = df["time"] / df["time"].sum()
        df["count_per"] = df["count"] / df["count"].sum()
        df["ops_slope"] = df["count_per"] / df["time_per"]
        drop_cols = ["time_per", "count_per"]
        if not slope:
            df["ops_rank"] = (1 / df["ops_slope"]).rank(pct=True)
            drop_cols.append("ops_slope")
        return df.drop(columns=drop_cols, errors="ignore")

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        value_col = self.metric_column(slope)
        if slope:
            return self.score_slope(df, value_col)
        return self.score_percentile(df, value_col)

    def metric_column(self, slope=False) -> str:
        if slope:
            return "ops_slope"
        return "ops_rank"


class DerivedRatioMetric(Metric):
    def __init__(self, name: str, val_col: str, div_col: str, derived_col: str):
        super().__init__(name)
        self.is_normalized = True
        self.score_col = f"u_{name}_score"

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        unovlp_col = f"u_{self.name}"
        df[unovlp_col] = 0.0
        df[unovlp_col] = df[unovlp_col].mask(
            df[self.name] - df["compute_time"] > 0,
            df[self.name] - df["compute_time"],
        )
        df[f"{unovlp_col}_per"] = 0.0
        df[f"{unovlp_col}_per"] = df[f"{unovlp_col}_per"].mask(
            df[self.name] > 0,
            df[unovlp_col] / df['time'],
        )
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, f"u_{self.name}_per")

    def metric_column(self, slope=False) -> str:
        return f"u_{self.name}_per"


class IntensityMetric(Metric):
    def __init__(self):
        super().__init__("intensity")

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df['intensity'] = 0.0
        df['intensity'] = df['intensity'].mask(df['size'] > 0, df['count'] / df['size'])
        df['intensity_rank'] = df['intensity'].rank(pct=True)
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentile(df, self.metric_column())

    def metric_column(self, slope=False) -> str:
        return "intensity_rank"


KNOWN_METRICS: Dict[str, Metric] = {
    'posix_bw': BandwidthMetric('posix_data'),
    'intensity': IntensityMetric(),
    # 'dataloader_image_bw': BandwidthMetric('data'),
    'io_compute_per': RatioMetric('io_compute_per', 'io_time', 'compute_time'),
    'iops': OpsMetric(),
    'ops': OpsMetric(),
    'app_time_norm': NormalizedMetric('app_time'),
    'app_time_per': PercentageMetric('app_time'),
    'dataloader_time_norm': NormalizedMetric('dataloader_time'),
    'dataloader_time_per': PercentageMetric('dataloader_time'),
    'posix_read_bw': BandwidthMetric('posix_read'),
    'posix_time_norm': NormalizedMetric('posix_time'),
    'posix_time_per': PercentageMetric('posix_time'),
    'posix_write_bw': BandwidthMetric('posix_write'),
    # 'u_checkpoint_io_time_per': DerivedRatioMetric(
    #     'u_checkpoint_io_time_per', 'checkpoint_io_time', 'time', 'compute_time'
    # ),
    'u_io_time_per': RatioMetric('u_io_time_per', 'u_io_time', 'time'),
    # 'u_read_io_time': DerivedRatioMetric('read_io_time'),
    'write_bw': BandwidthMetric('write'),
}


def _find_metric(metrics, suffix):
    return [m for m in metrics if m.endswith(suffix)]


def _find_metric_pairs(metrics, suffix1, suffix2):
    set1 = set(m[: -len(suffix1)] for m in metrics if m.endswith(suffix1))
    set2 = set(m[: -len(suffix2)] for m in metrics if m.endswith(suffix2))
    common_prefixes = set1.intersection(set2)
    return [(prefix + suffix1, prefix + suffix2) for prefix in common_prefixes]


def set_metrics(df: pd.DataFrame, time_boundary: float) -> pd.DataFrame:
    metric_cols = []
    # Set count metrics
    for count_col in _find_metric(df.columns, '_count'):
        count_per_col = f"{count_col}_per"
        df[count_per_col] = df[count_col] / df[count_col].sum()
        metric_cols.append(count_per_col)
    # Set unoverlapped time metrics (this has to come before time percentage calc.)
    if 'compute_time' in df.columns:
        for time_col in _find_metric(df.columns, '_time'):
            if 'u_' in time_col or time_col == 'compute_time':
                continue
            df[f"u_{time_col}"] = np.maximum(df[time_col] - df['compute_time'], 0)
            df[f"u_{time_col}"] = df[f"u_{time_col}"].astype('double[pyarrow]')
    # Set time metrics
    for time_col in _find_metric(df.columns, '_time'):
        time_norm_col = f"{time_col}_norm"
        time_per_col = f"{time_col}_per"
        df[time_norm_col] = df[time_col] / time_boundary
        df[time_per_col] = df[time_col] / df[time_col].sum()
        metric_cols.append(time_norm_col)
        metric_cols.append(time_per_col)
    # Set bandwidth metrics
    for size_col, time_col in _find_metric_pairs(df.columns, '_size', '_time'):
        bw_col = size_col.replace('_size', '_bw')
        df[bw_col] = df[size_col] / df[time_col]
        metric_cols.append(bw_col)
    # Set intensity metrics
    for count_col, size_col in _find_metric_pairs(df.columns, '_count', '_size'):
        intensity_col = count_col.replace('_count', '_intensity')
        df[intensity_col] = 0.0
        df[intensity_col] = df[intensity_col].mask(
            df[size_col] > 0, df[count_col] / df[size_col]
        )
        metric_cols.append(intensity_col)
    # Set ops metrics
    for count_col, time_col in _find_metric_pairs(df.columns, '_count', '_time'):
        ops_col = count_col.replace('_count', '_ops')
        df[ops_col] = df[count_col] / df[time_col]
        df[ops_col] = np.where(np.isnan, df[ops_col], 0)
        metric_cols.append(ops_col)
    for count_per_col, time_per_col in _find_metric_pairs(
        df.columns, '_count_per', '_time_per'
    ):
        ops_slope_col = count_per_col.replace('_count_per', '_ops_slope')
        ops_rank_col = count_per_col.replace('_count_per', '_ops_rank')
        df[ops_slope_col] = df[count_per_col] / df[time_per_col]
        df[ops_slope_col] = np.where(np.isnan, df[ops_slope_col], 0)
        df[ops_rank_col] = (1 / df[ops_slope_col]).rank(pct=True)
        metric_cols.append(ops_slope_col)
        metric_cols.append(ops_rank_col)
    cols = df.columns[df.columns.str.endswith('_bw|_per|_intensity|_norm|_rank|_slope')]
    df[cols] = df[cols].fillna(0).astype('double[pyarrow]')
    # df.columns = df.columns.map(lambda col: 'm_' + col if col in metric_cols else col)
    return df.sort_index(axis=1)


def set_metric_scores(df: pd.DataFrame) -> pd.DataFrame:
    score_cols = []
    # Set bandwidth scores
    for bw_col in _find_metric(df.columns, '_bw'):
        bw_bins = BW_BINS_PER_PROC if COL_PROC_NAME in df.index.names else BW_BINS
        bw_score_col = f"{bw_col}_score"
        # df[bw_score_col] = pd.cut(
        #     df[bw_col],
        #     bins=bw_bins,
        #     labels=np.flip(SCORE_BINS),
        #     right=True,
        # )
        df[bw_score_col] = np.digitize(df[bw_col], bins=bw_bins, right=True)
        df[bw_score_col] = df[bw_score_col].mask(np.isnan(df[bw_col]), 0)
        score_cols.append(bw_score_col)
    # Set intensity scores
    for intensity_col in _find_metric(df.columns, '_intensity'):
        intensity_score_col = f"{intensity_col}_score"
        # df[intensity_score_col] = pd.cut(
        #     df[intensity_col],
        #     bins=INTENSITY_BINS,
        #     labels=SCORE_BINS,
        #     right=True,
        # )
        df[intensity_score_col] = np.digitize(
            df[intensity_col], bins=INTENSITY_BINS, right=True
        )
        df[intensity_score_col] = df[intensity_score_col].mask(
            np.isnan(df[intensity_col]), 0
        )
        score_cols.append(intensity_score_col)
    # Set percentage scores
    for col in ["_norm", "_intensity", "_per", "_rank", "_util"]:
        for metric in _find_metric(df.columns, col):
            score_col = f"{metric}_score"
            # df[score_col] = pd.cut(
            #     df[metric],
            #     bins=PERCENTAGE_BINS,
            #     labels=SCORE_BINS,
            #     right=True,
            # )
            df[score_col] = np.digitize(df[metric], bins=PERCENTAGE_BINS, right=True)
            df[score_col] = df[score_col].mask(np.isnan(df[metric]), 0)
            score_cols.append(score_col)
    # Set slope scores
    for slope_col in _find_metric(df.columns, '_slope'):
        slope_score_col = f"{slope_col}_score"
        # df[slope_score_col] = pd.cut(
        #     df[slope_col],
        #     bins=SLOPE_BINS,
        #     labels=SCORE_BINS,
        #     right=True,
        # )
        df[slope_score_col] = np.digitize(df[slope_col], bins=SLOPE_BINS, right=True)
        df[slope_score_col] = df[slope_score_col].mask(np.isnan(df[slope_col]), 0)
        score_cols.append(slope_score_col)
    cols = df.columns[df.columns.str.endswith('_score')]
    df[cols] = df[cols].fillna(0).astype('uint8[pyarrow]')
    # df.columns = df.columns.map(lambda col: 's_' + col if col in score_cols else col)
    return df.sort_index(axis=1)
