import dask
import numpy as np
from anytree import NodeMixin, RenderTree
from dask.dataframe import DataFrame
from pandas import Categorical
from typing import Any, List, Tuple
from vani.common.constants import PERCENTAGE_FORMAT, SECONDS_FORMAT
from vani.common.interfaces import _BinNode, _Filter, _FilterGroup


class BinNode(_BinNode, NodeMixin):

    def __init__(self, ddf: DataFrame, bin: Tuple[float, float], filter_group: _FilterGroup, filter: _Filter, parent=None) -> None:
        super(BinNode, self).__init__()
        start, stop = bin
        self.bin = bin
        self.bin_step = stop - start
        self.ddf = ddf
        self.filter = filter
        self.filter_group = filter_group
        self.metrics = filter_group.metrics_of(filter)
        self.parent = parent
        self.score = 0

    def analyze(self):
        # Apply filter first
        filter_task = self.__apply_filter(ddf=self.ddf)
        # Then apply metrics
        metric_tasks = self.__apply_metrics(ddf=self.ddf)
        # Compute tasks
        filter_result, *metric_results = dask.compute(filter_task, *metric_tasks)
        # Keep results
        self.filter_result = filter_result
        self.metric_results = metric_results
        # Detect bottlenecks
        self.bottlenecks = self.__detect_filter_bottlenecks(filter_result=filter_result)
        # Make observations for each metric
        self.observations = self.__detect_metric_bottlenecks(metric_results=metric_results)
        # Calculate score
        self.score = self.__calculate_score(filter_result=self.filter_result,
                                            bottlenecks=self.bottlenecks,
                                            observations=self.observations)
        # Return bottlenecks
        return self.bottlenecks, self.score

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

    def __calculate_score(self, filter_result: Categorical, bottlenecks: Categorical, observations: Categorical) -> float:
        filter_labels = []
        for index in range(len(filter_result)):
            bin_label = bottlenecks.values[index] if len(bottlenecks) else 1
            filter_labels.append(bin_label)
        all_metric_labels = []
        for index in range(len(self.metrics)):
            observation = observations[index]
            metric_labels = []
            for observation_index in range(len(observation)):
                bin_label = observation.values[observation_index]
                metric_labels.append(bin_label)
            all_metric_labels.extend(metric_labels)
        score = (np.sum(filter_labels) + np.sum(all_metric_labels))/(len(filter_labels) + len(all_metric_labels))
        return score / 10.0

    def __detect_filter_bottlenecks(self, filter_result: Any) -> Any:
        return self.filter.detect_bottlenecks(results=filter_result, threshold=False)

    def __detect_metric_bottlenecks(self, metric_results: Any) -> Any:
        # Init observations
        observations = []
        for index, metric in enumerate(self.metrics):
            # Make observations
            observation = metric.detect_bottlenecks(results=metric_results[index], threshold=False)
            observations.append(observation)
        return observations

    def __format_filter_lines(self, filter_name: str, lines: List[str]) -> str:
        return f"{filter_name}={' '.join(lines)}"

    def __format_line(self, line_parts: List[str]) -> str:
        return " ".join(list(filter(None, line_parts)))

    def __render_node_filter_result(self):
        lines = []
        for index in range(len(self.filter_result)):
            bin = self.filter_result.index.array[index]
            bins = [bin, bin + self.bin_step]
            bin_name = "-".join([SECONDS_FORMAT.format(bin) for bin in bins])
            bin_label = self.bottlenecks.values[index] if len(self.bottlenecks) else 1
            bin_label_fmt = str(bin_label)
            bin_value = self.filter_result.values[index]
            bin_value_fmt = self.filter.format_value(bin_value)
            bin_percent = (bin_value/self.filter.max)*100.0
            bin_percent_fmt = PERCENTAGE_FORMAT.format(bin_percent)
            lines.append(self.__format_line([bin_label_fmt, bin_value_fmt, bin_percent_fmt]))
        return self.__format_filter_lines(self.filter.name(), lines)

    def __render_node_metric_results(self):
        all_lines = []
        for index, metric in enumerate(self.metrics):
            metric_result = self.metric_results[index]
            observation = self.observations[index]
            lines = []
            for observation_index in range(len(observation)):
                bin = observation.index.array[observation_index]
                bins = [bin, bin + self.bin_step]
                bin_name = "-".join([SECONDS_FORMAT.format(bin) for bin in bins])
                bin_label = observation.values[observation_index]
                bin_label_fmt = str(bin_label)
                bin_value = metric_result.values[observation_index]
                bin_value_fmt = metric.format_value(bin_value)
                bin_percent = (bin_value / metric.max) * 100.0
                bin_percent_fmt = PERCENTAGE_FORMAT.format(bin_percent)
                lines.append(self.__format_line([bin_label_fmt, bin_value_fmt, bin_percent_fmt]))
            all_lines.append(self.__format_filter_lines(metric.name(), lines))
        return all_lines

    def __repr__(self) -> str:
        bin_name = "-".join([SECONDS_FORMAT.format(bin) for bin in self.bin])
        columns = []
        filter_result_fmt = self.__render_node_filter_result()
        metric_results_fmt = self.__render_node_metric_results()
        columns.append(filter_result_fmt)
        columns.extend(metric_results_fmt)
        score_fmt = PERCENTAGE_FORMAT.format(self.score * 100.0)
        return f"[{bin_name}] {' | '.join(columns)} | Confidence={score_fmt}"


class FilterGroupNode(NodeMixin):

    def __init__(self, title: str, filter_group: _FilterGroup) -> None:
        super(FilterGroupNode, self).__init__()
        self.filter_group = filter_group
        self.title = title


class AnalysisNode(NodeMixin):

    def __init__(self, filter_group_nodes: List[FilterGroupNode]) -> None:
        super(AnalysisNode, self).__init__()
        self.children = filter_group_nodes

    def render_tree(self):
        for pre, fill, node in RenderTree(self):
            if isinstance(node, AnalysisNode):
                print("%s%s" % (pre, "Analysis"))
            elif isinstance(node, FilterGroupNode):
                print("%s%s" % (pre, node.title))
            else:
                print("%s%s" % (pre, node))
