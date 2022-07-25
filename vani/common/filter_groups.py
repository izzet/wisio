import numpy as np
from dask.dataframe import DataFrame
from numpy import ndarray
from typing import Any, List, Tuple
from vani.common.filters import BandwidthFilter, FileFilter, IOSizeFilter, ParallelismFilter, TransferSizeFilter
from vani.common.interfaces import _BinInfo, _BinNode, _Filter, _FilterGroup
from vani.common.nodes import BinNode


class FilterGroup(_FilterGroup):

    def __init__(self, n_bins=2) -> None:
        self.n_bins = n_bins

    def create_node(self, ddf: DataFrame, bin: Tuple[float, float], filter: _Filter, parent=None) -> _BinNode:
        return BinNode(ddf=ddf, bin=bin, filter_group=self, filter=filter, parent=parent)

    def __repr__(self) -> str:
        return self.name()


class TimelineFilterGroup(FilterGroup):

    def __init__(self, job_time: float, total_size: float, mean_bw: float, max_duration: float, total_ranks: int, total_files: int, n_bins=2) -> None:
        super().__init__(n_bins)
        assert job_time > 0
        assert mean_bw > 0
        assert max_duration > 0
        assert total_ranks > 0
        assert total_size > 0
        self.job_time = job_time
        self.mean_bw = mean_bw
        self.max_duration = max_duration
        self.total_ranks = total_ranks
        self.total_size = total_size
        self.total_files = total_files
        # Init filters
        self.filter_instances = [
            IOSizeFilter(min=0, max=self.total_size, n_bins=n_bins),
            BandwidthFilter(min=0, max=self.mean_bw, n_bins=n_bins),
            ParallelismFilter(min=0, max=self.total_ranks, n_bins=n_bins),
            FileFilter(min=0, max=self.total_files, n_bins=n_bins),
            # TransferSizeFilter(min=0, max=1, n_bins=n_bins)
        ]

    def filters(self) -> List[_Filter]:
        return self.filter_instances

    def metrics_of(self, filter: _Filter) -> List[_Filter]:
        return [metric for metric in self.filters() if metric != filter]

    def name(self) -> str:
        return "Timeline Analysis"

    def next_bins(self, start: Any, stop: Any) -> _BinInfo:
        # Return linear space between start and stop
        return np.linspace(start, stop, num=self.n_bins + 1, retstep=True)

    def set_bins(self, ddf: DataFrame, bins: ndarray):
        # Clear tbin values first
        ddf['tbin'] = 0
        # Then set bins
        for bin_index in range(len(bins) - 1):
            bin_start = bins[bin_index]
            bin_stop = bins[bin_index + 1]
            # 0 <= tmid < 3.744676801893446
            tstart_cond = ddf['tmid'].ge(bin_start)
            tend_cond = ddf['tmid'].lt(bin_stop)
            ddf['tbin'] = ddf['tbin'].mask(tstart_cond & tend_cond, bin_start)
