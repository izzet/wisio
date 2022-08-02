import numpy as np
import pandas as pd
from dask.dataframe import DataFrame, Series
from scipy import stats
from typing import Any
from vani.common.constants import VALUE_FORMAT
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
        # Check if the filter is normally distributed
        if self.is_normally_distributed():
            min_max_values = [self.min, self.max/2]
        # Check if the effect of the filter is inversed
        if self.is_inversed():
            labels = list(reversed(labels))
            min_max_indices = list(reversed(min_max_indices))
            min_max_values = list(reversed(min_max_values))
        # Add zeroed bin to fix labeling, this has no effect if there's already 0-indexed bins
        min_max_bins = pd.Series(min_max_values, index=min_max_indices)
        fixed_results = results.add(min_max_bins, fill_value=0).sort_values(ascending=False)
        # Check if the filter is normally distributed
        if self.is_normally_distributed():
            normalized_results = stats.norm.pdf(fixed_results, loc=self.max/2, scale=np.std(fixed_results))
            fixed_results = pd.Series(normalized_results, index=fixed_results.index)
            # Label results
        labeled_results = pd.cut(fixed_results, bins=self.n_bins, labels=labels)
        # Delete the effect of min/max bins
        for min_max_index in min_max_indices:
            del labeled_results[min_max_index]
        # Flag >50% problematic or all depending on threshold
        if self.is_inversed():
            return labeled_results[labeled_results < int(self.n_bins / 2)] if threshold else labeled_results
        return labeled_results[labeled_results > int(self.n_bins / 2)] if threshold else labeled_results

    def format_value(self, value: float) -> str:
        return VALUE_FORMAT.format(value) + self.unit()

    def is_inversed(self) -> bool:
        return False

    def is_normally_distributed(self) -> bool:
        return False

    @classmethod
    def on(cls, ddf: DataFrame) -> Any:
        return cls(min=0, max=0).apply(ddf)


class BandwidthFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf['bandwidth'].mean()/1024.0/1024.0/1024.0

    def is_inversed(self) -> bool:
        return True

    def name(self) -> str:
        return "Bandwidth"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby('tbin')

    def unit(self) -> str:
        return "GB/s"


class DurationFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf['duration'].max()

    def name(self) -> str:
        return "Duration"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf

    def unit(self) -> str:
        return "s"


class FileFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf['filename'].nunique()

    def format_value(self, value: float) -> str:
        return str(int(value))

    def is_inversed(self) -> bool:
        return True

    def name(self) -> str:
        return "Files"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby('tbin')

    def unit(self) -> str:
        return ""


class IOOpsFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf['index'].count()

    def format_value(self, value: float) -> str:
        return str(int(value))

    def is_normally_distributed(self) -> bool:
        return True

    def name(self) -> str:
        return "Ops"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby('tbin')

    def unit(self) -> str:
        return ""


class IOSizeFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf['size'].sum()/1024.0/1024.0/1024.0

    def name(self) -> str:
        return "Size"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby('tbin')

    def unit(self) -> str:
        return "GB"


class IOTimeFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        io_time = ddf['duration'].sum()
        n_ranks = ddf['rank'].nunique()
        return io_time / n_ranks

    def name(self) -> str:
        return "Time"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby('tbin')

    def unit(self) -> str:
        return "s/p"


class ParallelismFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf['rank'].nunique()

    def format_value(self, value: float) -> str:
        return str(int(value))

    def is_inversed(self) -> bool:
        return True

    def is_normally_distributed(self) -> bool:
        return True

    def name(self) -> str:
        return "Parallelism"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby('tbin')

    def unit(self) -> str:
        return ""


class XferSizeFilter(Filter):

    def apply(self, ddf: DataFrame) -> Any:
        return ddf['size'].mean()/1024.0/1024.0

    def is_inversed(self) -> bool:
        return True

    def name(self) -> str:
        return "Xfer"

    def prepare(self, ddf: DataFrame) -> Any:
        return ddf.groupby('tbin')

    def unit(self) -> str:
        return "MB"
