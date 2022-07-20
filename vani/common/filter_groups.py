import numpy as np
from dask.dataframe import DataFrame
from numpy import ndarray
from typing import Any, List, Tuple, Type
from vani.common.filters import BandwidthFilter, IOSizeFilter, ParallelismFilter
from vani.common.interfaces import _BinInfo, _Filter, _FilterGroup, _Node
from vani.common.nodes import Node


class FilterGroup(_FilterGroup):

    def __init__(self, n_bins: int) -> None:
        self.n_bins = n_bins

    def create_node(self, ddf: DataFrame, bin: Tuple[float, float], filter: _Filter, label: str, parent=None, children=None) -> _Node:
        return Node(ddf=ddf, bin=bin, filter_group=self, filter=filter, label=label, parent=parent, children=children)


class TimelineFilterGroup(FilterGroup):

    def __init__(self, job_time: float, total_size: float, mean_bw: float, max_duration: float, n_ranks: int, n_bins=10) -> None:
        super().__init__(n_bins)
        assert job_time > 0
        assert mean_bw > 0
        assert max_duration > 0
        assert n_ranks > 0
        assert total_size > 0
        self.job_time = job_time
        self.mean_bw = mean_bw
        self.max_duration = max_duration
        self.n_ranks = n_ranks
        self.total_size = total_size
        # Init filters
        self.filter_instances = [
            IOSizeFilter(min=0, max=self.total_size, n_bins=n_bins),
            BandwidthFilter(min=0, max=self.mean_bw, n_bins=n_bins),
            ParallelismFilter(min=0, max=self.n_ranks, n_bins=n_bins)
        ]

    def filters(self) -> List[_Filter]:
        return self.filter_instances

    def is_bin_threshold_exceeded(self, bin_step: Any) -> bool:
        return self.max_duration <= bin_step

    def metrics_of(self, filter: _Filter) -> List[_Filter]:
        return [metric for metric in self.filters() if metric != filter]

    def set_bins(self, ddf: DataFrame, start: Any, stop: Any) -> _BinInfo:
        # Calculate bins
        bins, step = self.__calculate_bins_for_time_range(start, stop)
        # Set bins
        self.__set_bins(ddf, bins, step)
        # Return bins & step
        return bins, step

    def __calculate_bins_for_time_range(self, start: float, stop: float):
        # Return linear space between start and stop
        # n_bins + 1 because when start=0 and stop=33.702, only this way it returns [0, 16.851, 33.702]
        return np.linspace(start, stop, num=self.n_bins + 1, retstep=True)

    def __set_bins(self, ddf: DataFrame, bins: ndarray, step: float):
        # Clear tbin values first
        ddf['tbin'] = 0
        # Then set bins
        for bin in bins:
            # 0 <= tmid < 3.744676801893446
            tstart_cond = ddf['tmid'].ge(bin)
            tend_cond = ddf['tmid'].lt(bin + step)
            ddf['tbin'] = ddf['tbin'].mask(tstart_cond & tend_cond, bin)
