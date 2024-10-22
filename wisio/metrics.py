import abc
import numpy as np
import pandas as pd
from typing import Dict

from .types import Score


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
        bin_col = f"{self.name}_bin"
        score_col = f"{self.name}_score"
        df[bin_col] = np.digitize(df[value_col], bins=SLOPE_BINS, right=True)
        df[score_col] = np.choose(df[bin_col] - 1, choices=SCORE_NAMES, mode='clip')
        return df.drop(columns=[bin_col])

    def score_percentage(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        bin_col = f"{self.name}_bin"
        score_col = f"{self.name}_score"
        df[bin_col] = np.digitize(df[value_col], bins=PERCENTAGE_BINS, right=True)
        df[score_col] = np.choose(df[bin_col] - 1, choices=SCORE_NAMES, mode='clip')
        return df.drop(columns=[bin_col])

    def score_percentile(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        return self.score_percentage(df, value_col)

    def query_column_name(self, slope=False) -> pd.Series:
        if slope:
            return f"{self.name}_slope"
        return f"{self.name}_percentile"

    def __str__(self):
        return self.name


class TimeMetric(Metric):
    def __init__(self):
        super().__init__("time")

    def calculate(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        df["time_normalized"] = df["time"] / boundary
        return df

    def score(self, df: pd.DataFrame, boundary: float, slope=False) -> pd.DataFrame:
        return self.score_percentage(df, "time_normalized")

    def query_column_name(self, slope=False) -> pd.Series:
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

    def query_column_name(self, slope=False) -> pd.Series:
        return "io_compute_ratio"


KNOWN_METRICS: Dict[str, Metric] = {
    "io_compute_ratio": IOComputeRatioMetric(),
    "iops": OpsMetric(),
    "ops": OpsMetric(),
    "time": TimeMetric(),
}
