from anytree import NodeMixin
from dask.dataframe import DataFrame
from typing import Any, Tuple

from vani.common.interfaces import _Filter, _FilterGroup, _Node


class Node(_Node, NodeMixin):

    def __init__(self, ddf: DataFrame, bin: Tuple[float, float], filter_group: _FilterGroup, filter: _Filter, label: str, parent=None, children=None) -> None:
        super(Node, self).__init__()
        self.bin = bin
        self.ddf = ddf
        self.filter = filter
        self.filter_group = filter_group
        self.label = label
        self.metrics = filter_group.metrics_of(filter)
        self.observations = []
        self.score = 0
        self.parent = parent
        if children:
            self.children = children
        start, stop = bin
        bins, step = filter_group.set_bins(ddf=ddf, start=start, stop=stop)
        self.bins = bins
        self.bin_step = step

    def analyze(self):
        # Apply filter first & detect bottlenecks
        bottlenecks = self.__apply_filter()

        # Then apply metrics
        # Then cross metrics
        return bottlenecks, self.bins, self.bin_step

    def __apply_filter(self) -> Any:
        # Let filter prepare data
        prepared_ddf = self.filter.prepare(self.ddf)
        # Apply filter first
        filter_results = self.filter.apply(prepared_ddf).compute()
        # Detect potential bottlenecks
        bottlenecks = self.filter.detect_bottlenecks(results=filter_results, threshold=True)
        # Prepare metric tasks
        for metric in self.metrics:
            # Apply filter first
            metric_results = metric.apply(prepared_ddf).compute()
            # Detect potential bottlenecks
            bottlenecks2 = metric.detect_bottlenecks(results=metric_results, threshold=False)
        # Apply filter first
        return bottlenecks

    def __apply_metrics(self, bottleneck_df: DataFrame) -> Any:
        # Init metric tasks
        metric_tasks = []
        # Loop through metrics
        for metric in self.metrics:
            # Apply metric
            metric.apply()
        return None
