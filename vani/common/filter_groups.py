import dask
import numpy as np
import yaml
from dask.dataframe import DataFrame
from numpy import ndarray
from os.path import isfile
from typing import Any, Dict, List, Tuple
from vani.common.filters import *
from vani.common.interfaces import _BinInfo, _BinNode, _Filter, _FilterGroup
from vani.common.nodes import BinNode
from vani.utils.string_utils import to_snake_case

_NUM_NEXT_BINS = 3


class FilterGroup(_FilterGroup):

    def __init__(self, n_bins=2, stats_file_prefix="") -> None:
        self.n_bins = n_bins
        self.stats = dict()
        self._stats_file_name = f"{stats_file_prefix}{to_snake_case(self.name())}_stats.yaml"

    def create_node(self, ddf: DataFrame, bin: Tuple[float, float], filter: _Filter, parent=None) -> _BinNode:
        return BinNode(ddf=ddf, bin=bin, filter_group=self, filter=filter, parent=parent)

    def load_stats(self) -> Dict:
        if not isfile(self._stats_file_name):
            return dict()
        with open(self._stats_file_name, 'r') as file:
            return yaml.safe_load(file)

    def persist_stats(self) -> None:
        with open(self._stats_file_name, 'w') as file:
            yaml.dump(self.stats, file, sort_keys=True)

    def prepare(self, ddf: DataFrame, all_ddf: DataFrame, persist_stats=True, debug=False) -> None:
        if debug:
            print("---Stats---")
        if persist_stats:
            self.stats = self.load_stats()
            if debug and self.stats:
                print("Stats loaded from file")
        if not self.stats:
            if debug:
                print("Computing stats...")
            self.stats = self.compute_stats(ddf=ddf, all_ddf=all_ddf)
        if debug:
            print(self.stats)
        if persist_stats:
            self.persist_stats()
        if debug:
            print("-----------")

    def __repr__(self) -> str:
        return self.name()


class TimelineReadWriteFilterGroupBase(FilterGroup):

    def binned_by(self) -> str:
        return 'tbin'

    def create_root(self, ddf: DataFrame, filter: _Filter, parent=None) -> _BinNode:
        self.set_bins(ddf=ddf, bins=[0, self.stats['job_time']])
        return self.create_node(ddf=ddf, bin=(0, self.stats['job_time']), filter=filter, parent=parent)

    def filters(self) -> List[_Filter]:
        return self.filter_instances

    def metrics_of(self, filter: _Filter) -> List[_Filter]:
        return [metric for metric in self.filters() if metric != filter]

    def next_bins(self, start: Any, stop: Any) -> _BinInfo:
        # Return linear space between start and stop
        return np.linspace(start, stop, num=_NUM_NEXT_BINS, retstep=True)

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

    def compute_stats(self, ddf: DataFrame, all_ddf: DataFrame) -> Dict:
        # Init stat tasks
        stats_tasks = [
            all_ddf['tend'].max(),
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
        job_time, io_time, max_duration, max_size, mean_bw, total_ranks, total_size, total_files, total_ops = dask.compute(*stats_tasks)
        # Return stats
        return dict(
            io_time=float(io_time),
            job_time=float(job_time),
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

    def prepare(self, ddf: DataFrame, all_ddf: DataFrame, persist_stats=True, debug=False) -> None:
        super().prepare(ddf, all_ddf, persist_stats, debug)
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

    def compute_stats(self, ddf: DataFrame, all_ddf: DataFrame) -> Dict:
        # Init stat tasks
        stats_tasks = [
            all_ddf['tend'].max(),
            IOTimeFilter.on(ddf=ddf),
            DurationFilter.on(ddf=ddf),
            ParallelismFilter.on(ddf=ddf),
            FileFilter.on(ddf=ddf),
            IOOpsFilter.on(ddf=ddf)
        ]
        # Compute stats
        job_time, io_time, max_duration, total_ranks, total_files, total_ops = dask.compute(*stats_tasks)
        # Return stats
        return dict(
            io_time=float(io_time),
            job_time=float(job_time),
            max_duration=float(max_duration),
            total_files=int(total_files),
            total_ops=int(total_ops),
            total_ranks=int(total_ranks),
        )

    def name(self) -> str:
        return "Timeline Metadata"

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        return "I/O metadata operations"

    def prepare(self, ddf: DataFrame, all_ddf: DataFrame, persist_stats=True, debug=False) -> None:
        super().prepare(ddf, all_ddf, persist_stats, debug)
        # Init filters
        self.filter_instances = [
            IOTimeFilter(min=0, max=self.stats['io_time'], n_bins=self.n_bins),
            IOOpsFilter(min=0, max=self.stats['total_ops'], n_bins=self.n_bins),
            FileFilter(min=0, max=self.stats['total_files'], n_bins=self.n_bins),
            ParallelismFilter(min=0, max=self.stats['total_ranks'], n_bins=self.n_bins),
        ]
