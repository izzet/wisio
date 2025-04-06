import abc
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional

from .constants import COL_PROC_NAME
from .types import Layer, Score


POSIX_BW_BINS = [  # System-wide bandwidth bins
    0,  # Complete blocking/stalled I/O
    1024**2,  # 1MB - Single legacy HDD performance
    1024**2 * 4,  # 4MB - RAID0 HDD or basic SSD
    1024**2 * 16,  # 16MB - Consumer SATA SSD
    1024**2 * 64,  # 64MB - Mid-range NVMe SSD
    1024**2 * 256,  # 256MB - High-end PCIe 4.0 NVMe
    1024**3,  # 1GB - Entry enterprise storage
    1024**3 * 16,  # 16GB - High-end enterprise storage
    1024**3 * 64,  # 64GB - Distributed storage system
    1024**4,  # 1TB - High-performance parallel filesystem
]
POSIX_BW_BINS_PER_PROC = [  # Per-process bandwidth bins
    0,  # Complete blocking/process stall
    1024**2,  # 1MB - Legacy storage process throughput
    1024**2 * 4,  # 4MB - Basic HDD sequential read
    1024**2 * 10,  # 10MB - Good HDD or basic SSD per-process
    1024**2 * 64,  # 64MB - Consumer NVMe single process
    1024**2 * 128,  # 128MB - High-end NVMe single process
    1024**2 * 256,  # 256MB - Entry DDR4 memory bandwidth
    1024**2 * 512,  # 512MB - DDR5/HBM single channel
    1024**3,  # 1GB - Multi-channel memory bandwidth
    1024**3 * 64,  # 64GB - Full system memory bandwidth
]
INTENSITY_BINS = [0, 0.001, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999]
PERCENTAGE_BINS = [0, 0.0001, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99]
RATE_BINS = list(np.exp(np.linspace(0, 9, 10)))
SCORE_NAMES = [
    Score.NONE.value,
    Score.TRIVIAL.value,
    Score.VERY_LOW.value,
    Score.LOW.value,
    Score.MEDIUM_LOW.value,
    Score.MEDIUM.value,
    Score.MEDIUM_HIGH.value,
    Score.HIGH.value,
    Score.VERY_HIGH.value,
    Score.CRITICAL.value,
]
# SCORE_BINS = np.linspace(0, 1, num=len(SCORE_NAMES))
SCORE_BINS = [1, 2, 3, 4, 5, 6, 7, 8, 9]
SLOPE_BINS = [
    np.tan(np.deg2rad(85)),  # 11.43005230
    np.tan(np.deg2rad(80)),  # 5.67128182
    np.tan(np.deg2rad(70)),  # 2.74747742
    np.tan(np.deg2rad(60)),  # 1.73205081
    np.tan(np.deg2rad(50)),  # 1.19175359
    np.tan(np.deg2rad(40)),  # 0.83909963
    np.tan(np.deg2rad(30)),  # 0.57735027
    np.tan(np.deg2rad(20)),  # 0.36397023
    np.tan(np.deg2rad(10)),  # 0.17632698
    np.tan(np.deg2rad(5)),  # 0.08748866
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
        bins = (
            POSIX_BW_BINS_PER_PROC if COL_PROC_NAME in df.index.names else POSIX_BW_BINS
        )
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


def _get_layer_dep_list(
    layer_deps: Dict[Layer, Optional[Layer]],
) -> Dict[str, List[str]]:
    def _get_layer_deps(parent_layer: str):
        deps = [parent_layer]
        while layer_deps.get(parent_layer):
            parent_layer = layer_deps[parent_layer]
            deps.append(parent_layer)
        return list(reversed(deps))

    layer_dep_list = {}
    for layer in layer_deps:
        if layer_deps[layer] is not None:
            layer_dep_list[layer] = _get_layer_deps(layer)
    return layer_dep_list


def _find_metric(metrics, suffix):
    return [m for m in metrics if m.endswith(suffix)]


def _find_metric_pairs(metrics, suffix1, suffix2):
    set1 = set(m[: -len(suffix1)] for m in metrics if m.endswith(suffix1))
    set2 = set(m[: -len(suffix2)] for m in metrics if m.endswith(suffix2))
    common_prefixes = set1.intersection(set2)
    return [(prefix + suffix1, prefix + suffix2) for prefix in common_prefixes]


def set_metrics(
    df: pd.DataFrame,
    layer_defs: Dict[Layer, str],
    layer_deps: Dict[Layer, Optional[Layer]],
    time_boundary: float,
) -> pd.DataFrame:
    drop_cols = []
    metric_cols = []
    # Set overhead time metrics
    for layer, parent in layer_deps.items():
        if not parent:
            continue
        child_layers = [
            child for child, parent in layer_deps.items() if parent == layer
        ]
        if not child_layers:
            continue
        overhead_time_col = f"{layer}_overhead_time_sum"
        child_times = sum(df[f"{child}_time_sum"].fillna(0) for child in child_layers)
        df[overhead_time_col] = np.maximum(df[f"{layer}_time_sum"] - child_times, 0)
        df[overhead_time_col] = df[overhead_time_col].astype('double[pyarrow]')
        metric_cols.append(overhead_time_col)
    # Set unoverlapped times if there is compute time
    if 'compute_time_sum' in df.columns:
        # Set unoverlapped time metrics (this has to come before time percentage calc.)
        for time_col in _find_metric(df.columns, '_time_sum'):
            if (
                time_col.startswith('u_')
                or time_col.startswith('d_')
                or 'compute_time_sum' in time_col
                or 'app_' in time_col
                or 'training_' in time_col
            ):
                continue
            compute_times = df['compute_time_sum'].fillna(0)
            df[f"u_{time_col}"] = np.maximum(df[time_col] - compute_times, 0)
            df[f"u_{time_col}"] = df[f"u_{time_col}"].astype('double[pyarrow]')
    # Set count metrics
    for count_col in _find_metric(df.columns, '_count_sum'):
        count_per_col = count_col.replace('_sum', '_per')
        df[count_per_col] = df[count_col] / df[count_col].sum()
        metric_cols.append(count_per_col)
    # Set time metrics
    for time_col in _find_metric(df.columns, '_time_sum'):
        time_norm_col = time_col.replace('_sum', '_norm')
        time_per_col = time_col.replace('_sum', '_per')
        df[time_norm_col] = df[time_col] / time_boundary
        df[time_per_col] = df[time_col] / df[time_col].sum()
        metric_cols.append(time_norm_col)
        # metric_cols.append(time_per_col)
        drop_cols.append(time_per_col)
    # Set bandwidth metrics
    for size_col, time_col in _find_metric_pairs(df.columns, '_size_sum', '_time_sum'):
        bw_col = size_col.replace('_size_sum', '_bw')
        df[bw_col] = df[size_col] / df[time_col]
        df[bw_col] = df[bw_col].mask(df[size_col] == 0, pd.NA)
        metric_cols.append(bw_col)
    # Set intensity metrics
    for count_col, size_col in _find_metric_pairs(
        df.columns, '_count_sum', '_size_sum'
    ):
        intensity_col = count_col.replace('_count_sum', '_intensity')
        df[intensity_col] = df[count_col] / df[size_col]
        df[intensity_col] = df[intensity_col].mask(df[size_col] == 0, pd.NA)
        metric_cols.append(intensity_col)
    # Set ops metrics
    for count_col, time_col in _find_metric_pairs(
        df.columns, '_count_sum', '_time_sum'
    ):
        ops_col = count_col.replace('_count_sum', '_ops')
        df[ops_col] = df[count_col] / df[time_col]
        metric_cols.append(ops_col)
    for count_per_col, time_per_col in _find_metric_pairs(
        df.columns, '_count_per', '_time_per'
    ):
        ops_slope_col = count_per_col.replace('_count_per', '_ops_slope')
        ops_rank_col = count_per_col.replace('_count_per', '_ops_rank')
        df[ops_slope_col] = df[count_per_col] / df[time_per_col]
        df[ops_slope_col] = np.where(np.isnan, df[ops_slope_col], 0)
        df[ops_rank_col] = (1 / df[ops_slope_col]).rank(pct=True)
        # metric_cols.append(ops_slope_col)
        metric_cols.append(ops_rank_col)
        drop_cols.append(ops_slope_col)
    if 'compute_time_sum' in df.columns:
        # Set compute time ratios
        for layer in layer_defs.keys():
            ratio_col = f"{layer.lower()}_compute_time_per"
            df[ratio_col] = df[f"{layer.lower()}_time_sum"] / df['compute_time_sum']
            metric_cols.append(ratio_col)
    cols = df.columns[df.columns.str.contains('_bw|_per|_intensity|_norm|_rank|_slope')]
    df[cols] = df[cols].replace([np.inf, -np.inf], np.nan).astype('double[pyarrow]')
    df = df.drop(columns=drop_cols)
    # df.columns = df.columns.map(lambda col: 'm_' + col if col in metric_cols else col)
    return df.sort_index(axis=1)


def set_metric_diffs(
    df: pd.DataFrame,
    layer_deps: Dict[Layer, Optional[Layer]],
) -> pd.DataFrame:
    def _calculate_diff(df: pd.DataFrame, dep_list: List[str]):
        diff = df[f"{dep_list[-1]}_time_sum"].fillna(0)
        for dep in dep_list[:-1]:
            diff -= df[f"{dep}_time_sum"].fillna(0)
        return np.where(np.isnan(diff) | (diff < 0), 0, diff)

    layer_dep_list = _get_layer_dep_list(layer_deps)
    for layer, dep_list in layer_dep_list.items():
        df[f"d_{layer.lower()}_time_sum"] = _calculate_diff(df.copy(), dep_list)
    return df


def set_metric_scores(
    df: pd.DataFrame,
    unscored_metrics: Optional[List[str]] = [],
) -> pd.DataFrame:
    metric_cols = [
        col
        for col in df.columns
        if col not in unscored_metrics and not col.startswith('d_')
    ]
    # Set bandwidth scores
    for bw_col in _find_metric(metric_cols, '_bw'):
        if 'posix' in bw_col:
            bw_bins = (
                POSIX_BW_BINS_PER_PROC
                if COL_PROC_NAME in df.index.names
                else POSIX_BW_BINS
            )
            bw_score_col = f"{bw_col}_score"
            bw_digitized = np.digitize(df[bw_col], bins=bw_bins, right=True)
            df[bw_score_col] = len(POSIX_BW_BINS) - bw_digitized
            df[bw_score_col] = df[bw_score_col].mask(np.isnan(df[bw_col]), pd.NA)
    # Set intensity scores
    for intensity_col in _find_metric(metric_cols, '_intensity'):
        intensity_score_col = f"{intensity_col}_score"
        df[intensity_score_col] = np.digitize(
            df[intensity_col], bins=INTENSITY_BINS, right=True
        )
        df[intensity_score_col] = df[intensity_score_col].mask(
            np.isnan(df[intensity_col]) | np.isinf(df[intensity_col]), pd.NA
        )
    # Set percentage scores
    for col in ['_norm', '_per', '_rank', '_util']:
        for metric in _find_metric(metric_cols, col):
            score_col = f"{metric}_score"
            df[score_col] = np.digitize(df[metric], bins=PERCENTAGE_BINS, right=True)
            if col == '_util':
                df[score_col] = len(PERCENTAGE_BINS) - df[score_col]
            df[score_col] = df[score_col].mask(np.isnan(df[metric]), pd.NA)
    # Set rate scores
    for rate_col in _find_metric(metric_cols, '_rate'):
        rate_score_col = f"{rate_col}_score"
        df[rate_score_col] = np.digitize(df[rate_col], bins=RATE_BINS, right=True)
        df[rate_score_col] = df[rate_score_col].mask(np.isnan(df[rate_col]), pd.NA)
    # Set slope scores
    for slope_col in _find_metric(metric_cols, '_slope'):
        slope_score_col = f"{slope_col}_score"
        df[slope_score_col] = np.digitize(df[slope_col], bins=SLOPE_BINS, right=True)
        df[slope_score_col] = df[slope_score_col].mask(np.isnan(df[slope_col]), pd.NA)
    cols = df.columns[df.columns.str.endswith('_score')]
    df[cols] = df[cols] / len(SCORE_NAMES)  # score from 0 to 1
    df[cols] = df[cols].astype('double[pyarrow]')
    return df.sort_index(axis=1)
