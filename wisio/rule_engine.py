import logging
import dask.dataframe as dd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from dask import compute
from typing import Dict, List

from .rules import (
    KNOWN_RULES,
    BottleneckRule,
    CharacteristicAccessPatternRule,
    CharacteristicFileCountRule,
    CharacteristicIOOpsRule,
    CharacteristicIOSizeRule,
    CharacteristicIOTimeRule,
    CharacteristicProcessCount,
    CharacteristicXferSizeRule,
    KnownCharacteristics as kc,
)
from .types import (
    BottlenecksPerViewPerMetric,
    Characteristics,
    Metric,
    RuleResult,
    RuleResultsPerViewPerMetricPerRule,
    ViewType,
)
from .utils.logger import ElapsedTimeLogger


class RuleEngine(object):

    def __init__(self, rules: Dict[ViewType, List[str]]) -> None:
        self.rules = rules

    def process_characteristics(self, main_view: dd.DataFrame) -> Characteristics:
        with ElapsedTimeLogger(message='Create characteristics rules', level=logging.DEBUG):
            rules = [
                CharacteristicIOTimeRule(),
                CharacteristicIOOpsRule(),
                CharacteristicIOSizeRule(),
                CharacteristicXferSizeRule(rule_key=kc.READ_XFER_SIZE.value),
                CharacteristicXferSizeRule(rule_key=kc.WRITE_XFER_SIZE.value),
                CharacteristicProcessCount(rule_key=kc.NODE_COUNT.value),
                CharacteristicProcessCount(rule_key=kc.APP_COUNT.value),
                CharacteristicProcessCount(rule_key=kc.PROC_COUNT.value),
                CharacteristicFileCountRule(),
                CharacteristicAccessPatternRule(),
            ]

        with ElapsedTimeLogger(message='Define characteristics tasks', level=logging.DEBUG):
            tasks = {}
            for rule in rules:
                tasks[rule.rule_key] = rule.define_tasks(main_view=main_view)

        # visualize(tasks, filename='characteristics')

        with ElapsedTimeLogger(message='Compute characteristics tasks', level=logging.DEBUG):
            results = compute(tasks)
        # with performance_report(filename='characteristics-report.html'):
        #     results = compute(tasks)

        with ElapsedTimeLogger(message='Handle characteristics task results', level=logging.DEBUG):
            characteristics = {}
            for rule in rules:
                result = results[0][rule.rule_key]
                characteristics[rule.rule_key] = rule.handle_task_results(
                    result=result,
                    characteristics=characteristics,
                )

        return characteristics

    def process_bottlenecks(
        self,
        evaluated_views: BottlenecksPerViewPerMetric,
        characteristics: Dict[str, RuleResult],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        threshold=0.5,
    ) -> RuleResultsPerViewPerMetricPerRule:

        with ElapsedTimeLogger(message='Create bottlenecks rules', level=logging.DEBUG):
            rules = {rule: BottleneckRule(rule_key=rule, rule=KNOWN_RULES[rule])
                     for rule in KNOWN_RULES}

        with ElapsedTimeLogger(message='Define bottlenecks tasks', level=logging.DEBUG):
            tasks = {}
            with ThreadPoolExecutor(max_workers=len(rules)) as executor:
                for rule, impl in rules.items():
                    for rule_tasks in executor.map(
                        self._define_tasks,
                        evaluated_views.keys(),
                        evaluated_views.values(),
                        np.repeat(rule, len(evaluated_views)),
                        np.repeat(impl, len(evaluated_views)),
                        np.repeat(metric_boundaries, len(evaluated_views)),
                        np.repeat(characteristics, len(evaluated_views)),
                        np.repeat(threshold, len(evaluated_views)),
                    ):
                        tasks.update(rule_tasks)

        # visualize(tasks, filename='bottlenecks')

        with ElapsedTimeLogger(message='Compute bottlenecks tasks', level=logging.DEBUG):
            results = compute(tasks)
        # with performance_report(filename='bottlenecks-report.html'):
        #     results = compute(tasks)

        with ElapsedTimeLogger(message='Handle bottlenecks task results', level=logging.DEBUG):
            bottlenecks = {}
            with ThreadPoolExecutor(max_workers=len(rules)) as executor:
                for rule, impl in rules.items():
                    for rule_bottlenecks in executor.map(
                        self._handle_task_results,
                        evaluated_views.keys(),
                        evaluated_views.values(),
                        np.repeat(rule, len(evaluated_views)),
                        np.repeat(impl, len(evaluated_views)),
                        np.repeat(results, len(evaluated_views)),
                        np.repeat(characteristics, len(evaluated_views)),
                    ):
                        bottlenecks.update(rule_bottlenecks)

        return bottlenecks

    @staticmethod
    def _define_tasks(
        metric: str,
        evaluated_views: BottlenecksPerViewPerMetric,
        rule: str,
        rule_impl: BottleneckRule,
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        characteristics: Dict[str, RuleResult],
        threshold: float
    ):
        tasks = {}
        tasks[rule] = {}
        tasks[rule][metric] = {}
        for view_key in evaluated_views:
            bottleneck_result = evaluated_views[view_key]
            tasks[rule][metric][view_key] = rule_impl.define_tasks(
                metric=metric,
                metric_boundary=metric_boundaries[metric],
                view_key=view_key,
                bottleneck_result=bottleneck_result,
                characteristics=characteristics,
                threshold=threshold,
            )
        return tasks

    @staticmethod
    def _handle_task_results(
        metric: str,
        evaluated_views: BottlenecksPerViewPerMetric,
        rule: str,
        rule_impl: BottleneckRule,
        results: tuple,
        characteristics: Dict[str, RuleResult],
    ):
        bottlenecks = {}
        bottlenecks[rule] = {}
        bottlenecks[rule][metric] = {}
        for view_key in evaluated_views:
            result = results[rule][metric][view_key]
            bottlenecks[rule][metric][view_key] = rule_impl.handle_task_results(
                metric=metric,
                view_key=view_key,
                result=result,
                characteristics=characteristics,
            )
        return bottlenecks
