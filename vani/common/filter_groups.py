import dask
import numpy as np
from dask.dataframe import DataFrame
from numpy import ndarray

from typing import Any, Dict, List, Tuple
from vani.common.filters import *
from vani.common.interfaces import _Bin, _BinInfo, _BinNode, _Filter, _FilterGroup
from vani.common.nodes import BinNode
from vani.utils.string_utils import to_snake_case
from vani.utils.yaml_utils import load_persisted


class FilterGroup(_FilterGroup):

    def __init__(self, n_bins=2, stats_file_prefix="") -> None:
        self.filter_instances = []
        self.n_bins = n_bins
        self.stats = dict()
        self._stats_file_name = f"{stats_file_prefix}{to_snake_case(self.name())}_stats.yaml"

    def create_node(self, ddf: DataFrame, bin: _Bin, parent=None) -> _BinNode:
        return BinNode(ddf=ddf, bin=bin, filter_group=self, parent=parent)

    def filters(self) -> List[_Filter]:
        return self.filter_instances

    def prepare(self, ddf: DataFrame, global_stats: Dict, persist_stats=True, debug=False) -> None:
        if debug:
            print(f"---{self.name()} Stats---")
        def compute_stats():
            if debug:
                print("Computing stats...")
            return self.compute_stats(ddf=ddf, global_stats=global_stats)
        self.stats = load_persisted(path=self._stats_file_name, fallback=compute_stats, persist=persist_stats)
        if debug:
            print(self.stats)
            print("-----------")

    def __repr__(self) -> str:
        return self.name()


class TimelineReadWriteFilterGroupBase(FilterGroup):

    def binned_by(self) -> str:
        return 'tbin'

    def calculate_bins(self, global_stats: Dict, n_bins: int) -> _BinInfo:
        bins, bin_step = np.linspace(0, global_stats['job_time'], num=n_bins+1, retstep=True)
        self.bins = bins
        self.bin_step = bin_step
        return bins, bin_step

    def get_bins(self) -> _BinInfo:
        return self.bins, self.bin_step

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


class TimelineReadWriteFilterGroup(TimelineReadWriteFilterGroupBase):

    def compute_stats(self, ddf: DataFrame, global_stats: Dict) -> Dict:
        # Init stat tasks
        stats_tasks = [
            IOTimeFilter.on(ddf=ddf),
            DurationFilter.on(ddf=ddf),
            ddf['size'].max()/1024.0/1024.0,
            BandwidthFilter.on(ddf=ddf),
            ParallelismFilter.on(ddf=ddf),
            IOSizeFilter.on(ddf=ddf),
            FileFilter.on(ddf=ddf),
            IOOpsFilter.on(ddf=ddf)
        ]
        # Compute stats
        io_time, max_duration, max_size, mean_bw, total_ranks, total_size, total_files, total_ops = dask.compute(*stats_tasks)
        # Return stats
        return dict(
            io_time=float(io_time),
            job_time=float(global_stats['job_time']),
            max_duration=float(max_duration),
            max_size=float(max_size),
            mean_bw=float(mean_bw),
            total_files=int(total_files),
            total_ops=int(total_ops),
            total_ranks=int(total_ranks),
            total_size=float(total_size)
        )

    def name(self) -> str:
        return "Timeline Read-Write"

    def prepare(self, ddf: DataFrame, global_stats: Dict, persist_stats=True, debug=False) -> None:
        super().prepare(ddf, global_stats, persist_stats, debug)
        # Init filters
        self.filter_instances = [
            IOSizeFilter(min=0, max=self.stats['total_size'], n_bins=self.n_bins),
            IOTimeFilter(min=0, max=self.stats['io_time'], n_bins=self.n_bins),
            IOOpsFilter(min=0, max=self.stats['total_ops'], n_bins=self.n_bins),
            FileFilter(min=0, max=self.stats['total_files'], n_bins=self.n_bins),
            BandwidthFilter(min=0, max=self.stats['mean_bw'], n_bins=self.n_bins),
            ParallelismFilter(min=0, max=self.stats['total_ranks'], n_bins=self.n_bins),
            XferSizeFilter(min=0, max=self.stats['max_size'], n_bins=self.n_bins)
        ]


class TimelineReadFilterGroup(TimelineReadWriteFilterGroup):

    def name(self) -> str:
        return "Timeline Read"

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        return "I/O read operations"


class TimelineWriteFilterGroup(TimelineReadWriteFilterGroup):

    def name(self) -> str:
        return "Timeline Write"

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        return "I/O write operations"


class TimelineMetadataFilterGroup(TimelineReadWriteFilterGroupBase):

    def compute_stats(self, ddf: DataFrame, global_stats: Dict) -> Dict:
        # Init stat tasks
        stats_tasks = [
            IOTimeFilter.on(ddf=ddf),
            DurationFilter.on(ddf=ddf),
            ParallelismFilter.on(ddf=ddf),
            FileFilter.on(ddf=ddf),
            IOOpsFilter.on(ddf=ddf)
        ]
        # Compute stats
        io_time, max_duration, total_ranks, total_files, total_ops = dask.compute(*stats_tasks)
        # Return stats
        return dict(
            io_time=float(io_time),
            job_time=float(global_stats['job_time']),
            max_duration=float(max_duration),
            total_files=int(total_files),
            total_ops=int(total_ops),
            total_ranks=int(total_ranks),
        )

    def name(self) -> str:
        return "Timeline Metadata"

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        return "I/O metadata operations"

    def prepare(self, ddf: DataFrame, global_stats: Dict, persist_stats=True, debug=False) -> None:
        super().prepare(ddf, global_stats, persist_stats, debug)
        # Init filters
        self.filter_instances = [
            IOTimeFilter(min=0, max=self.stats['io_time'], n_bins=self.n_bins),
            IOOpsFilter(min=0, max=self.stats['total_ops'], n_bins=self.n_bins),
            FileFilter(min=0, max=self.stats['total_files'], n_bins=self.n_bins),
            ParallelismFilter(min=0, max=self.stats['total_ranks'], n_bins=self.n_bins),
        ]
