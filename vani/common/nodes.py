import dask
import numpy as np
import random
from anytree import Node, NodeMixin, PreOrderIter, RenderTree
from dask.dataframe import DataFrame
from typing import Any, List, Tuple
from vani.common.constants import PERCENTAGE_FORMAT, SECONDS_FORMAT
from vani.common.hypotheses import Hypothesis
from vani.common.interfaces import _BinNode, _DescribesObservation, _Filter, _FilterGroup
from vani.common.observations import Observation


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
        self.filter_labels = self.__detect_filter_bottlenecks(filter_result=filter_result)
        # Make observations for each metric
        self.all_metric_labels = self.__detect_metric_bottlenecks(metric_results=metric_results)
        # Generate observations
        self.observations = self.__generate_observations()
        # Calculate score
        self.score = self.__calculate_score()
        # Return bottlenecks
        return self.filter_labels, self.score

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        return self.__bin_name()

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

    def __bin_name(self) -> str:
        return "-".join([SECONDS_FORMAT.format(bin) for bin in self.bin])

    def __calculate_score(self) -> float:
        score = 0
        if self.observations:
            labels = []
            for observation in self.observations:
                labels.append(observation.label)
            score = np.sum(labels) / len(self.observations)
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

    def __generate_observations(self):
        observations = []
        for index in range(len(self.filter_result)):
            label = self.filter_labels.values[index] if len(self.filter_labels) else 1
            value = self.filter_result.values[index]
            formatted_value = self.filter.format_value(value)
            score = value / self.filter.max
            observations.append(Observation(name=self.filter.name(),
                                            desc=self.filter.observation_desc(label=label, value=value, score=score),
                                            label=label,
                                            value=value,
                                            score=score,
                                            formatted_value=formatted_value))
        for index, metric in enumerate(self.metrics):
            metric_result = self.metric_results[index]
            metric_labels = self.all_metric_labels[index]
            for observation_index in range(len(metric_labels)):
                label = metric_labels.values[observation_index]
                value = metric_result.values[observation_index]
                formatted_value = metric.format_value(value)
                score = value / metric.max
                observations.append(Observation(name=metric.name(),
                                                desc=metric.observation_desc(label=label, value=value, score=score),
                                                label=label,
                                                value=value,
                                                score=score,
                                                formatted_value=formatted_value))
        return observations

    def __repr__(self) -> str:
        bin_name = self.__bin_name()
        columns = []
        for observation in self.observations:
            columns.append(repr(observation))
        score_fmt = PERCENTAGE_FORMAT.format(self.score * 100.0)
        return f"[{bin_name}] {' | '.join(columns)} | Confidence={score_fmt}"


class FilterGroupNode(_DescribesObservation, NodeMixin):

    def __init__(self, title: str, filter_group: _FilterGroup) -> None:
        super(FilterGroupNode, self).__init__()
        self.filter_group = filter_group
        self.title = title

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        return self.filter_group.observation_desc()


class AnalysisNode(NodeMixin):

    def __init__(self, filter_group_nodes: List[FilterGroupNode]) -> None:
        super(AnalysisNode, self).__init__()
        self.children = filter_group_nodes

    def generate_hypotheses(self):
        # Loop through all nodes and generate hypotheses
        hypotheses = []
        hypothesis_root = Node(name="Hypotheses")
        hypothesis_group = None
        for node in PreOrderIter(self):
            if isinstance(node.parent, FilterGroupNode):
                hypothesis_group = Hypothesis(desc=node.parent.observation_desc(),
                                              observations=node.observations,
                                              score=node.score,
                                              weighted_score=node.depth * node.score,
                                              parent=hypothesis_root)
                hypotheses.append(hypothesis_group)
            elif isinstance(node, BinNode):
                Hypothesis(desc=node.observation_desc(),
                           observations=node.observations,
                           score=node.score,
                           weighted_score=node.depth * node.score,
                           parent=hypothesis_group)

        def mysort(items):
            return sorted(items, key=lambda item: item.weighted_score, reverse=True)

        seen = []
        for pre, fill, hypothesis in RenderTree(hypothesis_root, childiter=mysort):
            if isinstance(hypothesis, Hypothesis):
                if hypothesis.score and hypothesis.score not in seen:
                    score_fmt = PERCENTAGE_FORMAT.format(hypothesis.score * 100.0)
                    confidence_stmt = random.choice([f"we suspect an I/O bottleneck with a confidence of {score_fmt}",
                                                    f"there is a {score_fmt} possibility of an I/O bottleneck"])
                    print("%s%s %s %s" % (pre, hypothesis.start_desc(), confidence_stmt, hypothesis.reason_desc()))
                    seen.append(hypothesis.score)

    def render(self):
        for pre, fill, node in RenderTree(self):
            if isinstance(node, AnalysisNode):
                print("%s%s" % (pre, "Analysis"))
            elif isinstance(node, FilterGroupNode):
                print("%s%s" % (pre, node.title))
            elif node.score:
                print("%s%s" % (pre, node))
