import abc
import numpy as np
import pandas as pd
from typing import Any, Dict, List

from .constants import COL_PROC_NAME
from .types import Score


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
# PERCENTAGE_BINS = [0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1]
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
        return self._score(df, value_col, SLOPE_BINS, SCORE_NAMES, True)

    def score_percentage(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self._score(df, value_col, PERCENTAGE_BINS, SCORE_NAMES, False)

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
    'data_bw': BandwidthMetric('data'),
    'intensity': IntensityMetric(),
    'io_bw': BandwidthMetric('data'),
    'io_compute_per': RatioMetric('io_compute_per', 'io_time', 'compute_time'),
    'io_time_norm': NormalizedMetric('io_time'),
    'io_time_per': PercentageMetric('io_time'),
    'iops': OpsMetric(),
    'ops': OpsMetric(),
    'read_bw': BandwidthMetric('read'),
    'time_norm': NormalizedMetric('time'),
    'time_per': PercentageMetric('time'),
    # 'u_checkpoint_io_time_per': DerivedRatioMetric(
    #     'u_checkpoint_io_time_per', 'checkpoint_io_time', 'time', 'compute_time'
    # ),
    'u_io_time_per': RatioMetric('u_io_time_per', 'u_io_time', 'time'),
    # 'u_read_io_time': DerivedRatioMetric('read_io_time'),
    'write_bw': BandwidthMetric('write'),
}
