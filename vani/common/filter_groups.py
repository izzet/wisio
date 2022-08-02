import dask
import numpy as np
from dask.dataframe import DataFrame
from numpy import ndarray
from typing import Any, List, Tuple
from vani.common.constants import SECONDS_FORMAT
from vani.common.filters import *
from vani.common.interfaces import _BinInfo, _BinNode, _Filter, _FilterGroup
from vani.common.nodes import BinNode

_NUM_NEXT_BINS = 3


class FilterGroup(_FilterGroup):

    def __init__(self, n_bins=2) -> None:
        self.n_bins = n_bins

    def create_node(self, ddf: DataFrame, bin: Tuple[float, float], filter: _Filter, parent=None) -> _BinNode:
        return BinNode(ddf=ddf, bin=bin, filter_group=self, filter=filter, parent=parent)

    def __repr__(self) -> str:
        return self.name()


class TimelineReadWriteFilterGroup(FilterGroup):

    def __init__(self, job_time: float, n_bins=2) -> None:
        super().__init__(n_bins)
        assert job_time > 0
        self.job_time = job_time
        self.n_bins = n_bins

    def binned_by(self) -> str:
        return 'tbin'

    def create_root(self, ddf: DataFrame, filter: _Filter, parent=None) -> _BinNode:
        self.set_bins(ddf=ddf, bins=[0, self.job_time])
        return self.create_node(ddf=ddf, bin=(0, self.job_time), filter=filter, parent=parent)

    def filters(self) -> List[_Filter]:
        return self.filter_instances

    def metrics_of(self, filter: _Filter) -> List[_Filter]:
        return [metric for metric in self.filters() if metric != filter]

    def name(self) -> str:
        return "Timeline Read/Write"

    def next_bins(self, start: Any, stop: Any) -> _BinInfo:
        # Return linear space between start and stop
        return np.linspace(start, stop, num=_NUM_NEXT_BINS, retstep=True)

    def prepare(self, ddf: DataFrame, debug=False) -> None:
        # Init stat tasks
        stats_tasks = {
            "I/O time/p": IOTimeFilter.on(ddf=ddf),
            "Max duration": DurationFilter.on(ddf=ddf),
            "Max xfer": ddf['size'].max()/1024.0/1024.0,
            "Mean BW": BandwidthFilter.on(ddf=ddf),
            "Total ranks": ParallelismFilter.on(ddf=ddf),
            "Total size": IOSizeFilter.on(ddf=ddf),
            "Total files": FileFilter.on(ddf=ddf),
            "Total ops": IOOpsFilter.on(ddf=ddf)
        }
        # Compute stats
        io_time, max_duration, max_size, mean_bw, total_ranks, total_size, total_files, total_ops = dask.compute(*stats_tasks.values())
        # Print stats
        stats = [io_time, max_duration, max_size, mean_bw, total_ranks, total_size, total_files, total_ops]
        if debug:
            for index, stat in enumerate(stats_tasks):
                print(f"{stat}: {SECONDS_FORMAT.format(stats[index])}")
        # Set stats
        self.io_time = io_time
        self.max_duration = max_duration
        self.max_size = max_size
        self.mean_bw = mean_bw
        self.total_files = total_files
        self.total_ops = total_ops
        self.total_ranks = total_ranks
        self.total_size = total_size
        # Init filters
        self.filter_instances = [
            IOSizeFilter(min=0, max=self.total_size, n_bins=self.n_bins),
            IOTimeFilter(min=0, max=self.io_time, n_bins=self.n_bins),
            IOOpsFilter(min=0, max=self.total_ops, n_bins=self.n_bins),
            FileFilter(min=0, max=self.total_files, n_bins=self.n_bins),
            BandwidthFilter(min=0, max=self.mean_bw, n_bins=self.n_bins),
            ParallelismFilter(min=0, max=self.total_ranks, n_bins=self.n_bins),
            XferSizeFilter(min=0, max=self.max_size, n_bins=self.n_bins)
        ]

    def set_bins(self, ddf: DataFrame, bins: ndarray):
        # Clear tbin values first
        binned_by = self.binned_by()
        ddf[binned_by] = 0
        # Then set bins
        for bin_index in range(len(bins) - 1):
            # Read bin range
            bin_start = bins[bin_index]
            bin_stop = bins[bin_index + 1]
            # 0 <= tmid < 3.744676801893446
            tstart_cond = ddf['tmid'].ge(bin_start)
            tend_cond = ddf['tmid'].lt(bin_stop)
            ddf[binned_by] = ddf[binned_by].mask(tstart_cond & tend_cond, bin_start)


class TimelineMetadataFilterGroup(TimelineReadWriteFilterGroup):

    def __init__(self, job_time: float, n_bins=2) -> None:
        super().__init__(job_time, n_bins)

    def name(self) -> str:
        return "Timeline Metadata"

    def prepare(self, ddf: DataFrame, debug=False) -> None:
        # Init stat tasks
        stats_tasks = {
            "I/O time/p": IOTimeFilter.on(ddf=ddf),
            "Max duration": DurationFilter.on(ddf=ddf),
            "Total ranks": ParallelismFilter.on(ddf=ddf),
            "Total files": FileFilter.on(ddf=ddf),
            "Total ops": IOOpsFilter.on(ddf=ddf)
        }
        # Compute stats
        io_time, max_duration, total_ranks, total_files, total_ops = dask.compute(*stats_tasks.values())
        # Print stats
        stats = [io_time, max_duration, total_ranks, total_files, total_ops]
        if debug:
            for index, stat in enumerate(stats_tasks):
                print(f"{stat}: {SECONDS_FORMAT.format(stats[index])}")
        # Set stats
        self.io_time = io_time
        self.max_duration = max_duration
        self.total_files = total_files
        self.total_ops = total_ops
        self.total_ranks = total_ranks
        # Init filters
        self.filter_instances = [
            IOTimeFilter(min=0, max=io_time, n_bins=self.n_bins),
            IOOpsFilter(min=0, max=total_ops, n_bins=self.n_bins),
            FileFilter(min=0, max=total_files, n_bins=self.n_bins),
            ParallelismFilter(min=0, max=total_ranks, n_bins=self.n_bins),
        ]
