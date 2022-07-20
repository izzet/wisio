import numpy as np
import pandas as pd
from dask.dataframe import DataFrame, Series
from typing import Any
from vani.common.interfaces import _Filter


class Filter(_Filter):

    def __init__(self, min: float, max: float, n_bins=10) -> None:
        self.min = min
        self.max = max
        self.n_bins = n_bins

    def detect_bottlenecks(self, results: Series, threshold=False) -> Any:
        # Prepare labels
        labels = [label for label in range(1, self.n_bins + 1)]
        # Create min/max indices
        min_max_indices = [np.finfo(float).min, np.finfo(float).max]
        min_max_values = [self.min, self.max]
        # Check if the effect of the filter is inversed
        if self.is_inversed():
            labels = [label for label in reversed(labels)]
            min_max_indices = [np.finfo(float).max, np.finfo(float).min]
            min_max_values = [self.max, self.min]
        # Add zeroed bin to fix labeling, this has no effect if there's already 0-indexed bins
        min_max_bins = pd.Series(min_max_values, index=min_max_indices)
        fixed_results = results.add(min_max_bins, fill_value=0).sort_values(ascending=False)
        # Label results
        labeled_results = pd.cut(fixed_results, bins=self.n_bins, labels=labels)
        # Delete the effect of min/max bins
        for min_max_index in min_max_indices:
            del labeled_results[min_max_index]
        # Flag >50% problematic or all depending on threshold
        return labeled_results[labeled_results > int(self.n_bins / 2)] if threshold else labeled_results

    def is_inversed(self) -> bool:
        return False

    @classmethod
    def on(cls, ddf: DataFrame) -> Any:
        return cls(min=0, max=0).apply(ddf)


class BandwidthFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf["bandwidth"].mean()/1024.0

    def is_inversed(self) -> bool:
        return True

    def name(self) -> str:
        return 'Bandwidth'

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby("tbin")


class DurationFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf["duration"].max()

    def name(self) -> str:
        return 'Duration'

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf


class IOSizeFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf["size"].sum()/1024.0/1024.0/1024.0

    def name(self) -> str:
        return 'Size'

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby("tbin")


class IOTimeFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        print("IOTimeFilter")

    def name(self) -> str:
        return 'Time'

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby("tbin")


class ParallelismFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf["rank"].nunique()

    def name(self) -> str:
        return 'Parallelism'

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby("tbin")


class TransferSizeFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return super().apply(ddf)

    def name(self) -> str:
        return 'Xfer Size'

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby("tbin")
