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
        self.score_col = f"{name}_score"

    @abc.abstractmethod
    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        raise NotImplementedError

    def score_slope(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self._score(
            df=df,
            value_col=value_col,
            bins=SLOPE_BINS,
            names=SCORE_NAMES,
        )

    def score_percentage(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self._score(
            df=df,
            value_col=value_col,
            bins=PERCENTAGE_BINS,
            names=SCORE_NAMES,
        )

    def score_percentile(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self.score_percentage(df, value_col)

    def query_column_name(self, slope=False) -> str:
        if slope:
            return f"{self.name}_slope"
        return f"{self.name}_percentile"

    def __str__(self):
        return self.name

    def _score(
        self,
        df: pd.DataFrame,
        value_col: str,
        bins: List[Any],
        names: List[str],
    ) -> pd.DataFrame:
        bin_col = f"{self.name}_bin"
        df[bin_col] = np.digitize(df[value_col], bins=bins, right=True)
        df[self.score_col] = np.choose(df[bin_col] - 1, choices=names, mode='clip')
        return df.drop(columns=[bin_col])


class BandwidthMetric(Metric):
    def __init__(self, name: str):
        super().__init__(name, is_reversed=True)
        self.score_col = f"{self.name}_bw_score"

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df[f"{self.name}_bw"] = df[f"{self.name}_size"] / df[f"{self.name}_time"]
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        bins = BW_BINS_PER_PROC if COL_PROC_NAME in df.index.names else BW_BINS
        return self._score(
            df=df,
            value_col=f"{self.name}_bw",
            bins=bins,
            names=np.flip(SCORE_NAMES),
        )

    def query_column_name(self, slope=False) -> str:
        return f"{self.name}_bw"


class TimeMetric(Metric):
    def __init__(self):
        super().__init__("time")

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df["time_normalized"] = df["time"] / boundary
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, "time_normalized")

    def query_column_name(self, slope=False) -> str:
        return "time_normalized"


class OpsMetric(Metric):
    def __init__(self):
        super().__init__("ops", is_normalized=True)

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df["time_per"] = df["time"] / df["time"].sum()
        df["count_per"] = df["count"] / df["count"].sum()
        df["ops_slope"] = df["count_per"] / df["time_per"]
        drop_cols = ["time_per", "count_per"]
        if not slope:
            df["ops_percentile"] = (1 / df["ops_slope"]).rank(pct=True)
            drop_cols.append("ops_slope")
        return df.drop(columns=drop_cols, errors="ignore")

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        if slope:
            return self.score_slope(df, "ops_slope")
        return self.score_percentile(df, "ops_percentile")


class IOComputeRatioMetric(Metric):
    def __init__(self):
        super().__init__("io_compute_ratio")

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df["io_compute_ratio"] = 0.0
        df["io_compute_ratio"] = df["io_compute_ratio"].mask(
            df["compute_time"] > 0, df["io_time"] / df["compute_time"]
        )
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, "io_compute_ratio")

    def query_column_name(self, slope=False) -> str:
        return "io_compute_ratio"


class UnoverlappedDerivedIOMetric(Metric):
    def __init__(self, name: str):
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

    def query_column_name(self, slope=False) -> str:
        return f"u_{self.name}_per"


class UnoverlappedIOMetric(Metric):
    def __init__(self, name: str = "u_io_time"):
        super().__init__(name)
        self.is_normalized = True

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df[f"{self.name}_per"] = df[self.name] / df['time']
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, f"{self.name}_per")

    def query_column_name(self, slope=False) -> str:
        return f"{self.name}_per"


class IntensityMetric(Metric):
    def __init__(self, name: str):
        super().__init__(f"{name}")

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df['intensity'] = 0.0
        df['intensity'] = df['intensity'].mask(df['size'] > 0, df['count'] / df['size'])
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, f"{self.name}_per")

    def query_column_name(self, slope=False) -> str:
        return f"{self.name}_per"


KNOWN_METRICS: Dict[str, Metric] = {
    'data_bw': BandwidthMetric('data'),
    'io_compute_ratio': IOComputeRatioMetric(),
    'io_time': TimeMetric(),
    'iops': OpsMetric(),
    'ops': OpsMetric(),
    'read_bw': BandwidthMetric('read'),
    'time': TimeMetric(),
    'u_checkpoint_io_time': UnoverlappedDerivedIOMetric('checkpoint_io_time'),
    'u_io_time': UnoverlappedIOMetric(),
    'u_read_io_time': UnoverlappedDerivedIOMetric('read_io_time'),
    'write_bw': BandwidthMetric('write'),
}
