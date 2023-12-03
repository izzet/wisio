import dask.dataframe as dd
from dask import compute, visualize
from dask.distributed import performance_report
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
    RuleResultsPerViewPerMetric,
    ViewType,
)


class RuleEngine(object):

    def __init__(self, rules: Dict[ViewType, List[str]]) -> None:
        self.rules = rules

    def process_characteristics(self, main_view: dd.DataFrame) -> Characteristics:
        rules = [
            CharacteristicIOTimeRule(),
            CharacteristicIOOpsRule(),
            CharacteristicIOSizeRule(),
            CharacteristicXferSizeRule(rule_key=kc.READ_XFER_SIZE.value),
            CharacteristicXferSizeRule(rule_key=kc.WRITE_XFER_SIZE.value),
            CharacteristicProcessCount(rule_key=kc.NODE_COUNT.value),
            CharacteristicProcessCount(rule_key=kc.APP_COUNT.value),
            CharacteristicFileCountRule(),
            CharacteristicAccessPatternRule(),
        ]

        tasks = {}
        for rule in rules:
            tasks[rule.rule_key] = rule.define_tasks(main_view=main_view)

        # visualize(tasks, filename='characteristics')

        results = compute(tasks)
        # with performance_report(filename='characteristics-report.html'):
        #     results = compute(tasks)

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
    ) -> RuleResultsPerViewPerMetric:

        rules = {rule: BottleneckRule(rule_key=rule, rule=KNOWN_RULES[rule])
                 for rule in KNOWN_RULES}

        tasks = {}
        for metric in evaluated_views:
            tasks[metric] = {}
            for view_key in evaluated_views[metric]:
                tasks[metric][view_key] = {}
                for rule, impl in rules.items():
                    bottleneck_result = evaluated_views[metric][view_key]
                    tasks[metric][view_key][rule] = impl.define_tasks(
                        metric=metric,
                        metric_boundary=metric_boundaries[metric],
                        view_key=view_key,
                        bottleneck_result=bottleneck_result,
                        characteristics=characteristics,
                        threshold=threshold,
                    )

        # visualize(tasks, filename='bottlenecks')

        results = compute(tasks)
        # with performance_report(filename='bottlenecks-report.html'):
        #     results = compute(tasks)

        bottlenecks = {}
        for metric in evaluated_views:
            bottlenecks[metric] = {}
            for view_key in evaluated_views[metric]:
                bottlenecks[metric][view_key] = {}
                for rule, impl in rules.items():
                    result = results[0][metric][view_key][rule]
                    bottlenecks[metric][view_key][rule] = impl.handle_task_results(
                        metric=metric,
                        view_key=view_key,
                        result=result,
                        characteristics=characteristics,
                    )

        return bottlenecks
