import dask
from anytree import NodeMixin, RenderTree
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
        self.name = label
        self.observations = []
        self.parent = parent
        self.score = 0
        if children:
            self.children = children
        start, stop = bin
        bins, step = filter_group.set_bins(ddf=ddf, start=start, stop=stop)
        self.bins = bins
        self.bin_step = step

    def analyze(self):
        # Apply filter first
        filter_task = self.__apply_filter(ddf=self.ddf)
        # Then apply metrics
        metric_tasks = self.__apply_metrics(ddf=self.ddf)
        # Compute tasks
        filter_result, *metric_results = dask.compute(filter_task, *metric_tasks)

        self.filter_result = filter_result
        self.metric_results = metric_results

        # Detect bottlenecks
        self.bottlenecks = self.__detect_filter_bottlenecks(filter_result=filter_result)
        # Make observations for each metric
        self.observations = self.__detect_metric_bottlenecks(metric_results=metric_results)
        # Then cross metrics
        return self.bottlenecks, self.bins, self.bin_step

    def render_tree(self):
        for pre, fill, node in RenderTree(self):
            name = '-'.join(['{:.3f}'.format(bin) for bin in node.bin])
            print("%s%s" % (pre, f'[{name}]={node.label}'))
            bottleneck_lines = []
            for bottleneck_index in range(len(node.bottlenecks)):
                bottleneck_bin = node.bottlenecks.index.array[bottleneck_index]
                bottleneck_bins = [bottleneck_bin, bottleneck_bin + node.bin_step]
                bottleneck_name = '-'.join(['{:.3f}'.format(bin) for bin in bottleneck_bins])
                bottleneck_value = node.bottlenecks.values[bottleneck_index]
                bottleneck_lines.append(f'[{bottleneck_name}]={bottleneck_value} ({node.filter_result.values[bottleneck_index]})')
            bottleneck_formatted = ', '.join(bottleneck_lines)
            print(" %s%s %s" % (fill, f'{self.filter.name()}:', bottleneck_formatted))
            for index, metric in enumerate(self.metrics):
                metric_result = node.metric_results[index]
                observation = node.observations[index]
                observation_lines = []
                for observation_index in range(len(observation)):
                    observation_bin = observation.index.array[observation_index]
                    observation_bins = [observation_bin, observation_bin + node.bin_step]
                    observation_name = '-'.join(['{:.3f}'.format(bin) for bin in observation_bins])
                    observation_value = observation.values[observation_index]
                    observation_lines.append(f'[{observation_name}]={observation_value} ({metric_result.values[observation_index]})')
                observations_formatted = ', '.join(observation_lines)
                print(" %s%s %s" % (fill, f'{metric.name()}:', observations_formatted))

    def __apply_filter(self, ddf: DataFrame) -> Any:
        # Let filter prepare data
        prepared_ddf = self.filter.prepare(ddf)
        # Apply filter first
        return self.filter.apply(prepared_ddf)

    def __apply_metrics(self, ddf: DataFrame) -> Any:
        # Init tasks
        metric_tasks = []
        # Prepare metric tasks
        for metric in self.metrics:
            # Prepare data first
            prepared_ddf = metric.prepare(ddf)
            # Apply filter first
            metric_task = metric.apply(prepared_ddf)
            # Add it to tasks
            metric_tasks.append(metric_task)
        return metric_tasks

    def __detect_filter_bottlenecks(self, filter_result: Any) -> Any:
        return self.filter.detect_bottlenecks(results=filter_result, threshold=True)

    def __detect_metric_bottlenecks(self, metric_results: Any) -> Any:
        # Init observations
        observations = []
        for index, metric in enumerate(self.metrics):
            # Make observations
            observation = metric.detect_bottlenecks(results=metric_results[index], threshold=False)
            observations.append(observation)
        return observations
