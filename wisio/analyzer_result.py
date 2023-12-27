import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import venn
from dataclasses import asdict, dataclass
from distributed import get_client
from matplotlib import ticker
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from scipy.stats import skew
from typing import Dict, List, Set, Tuple


from .analysis import DELTA_BIN_NAMES, DELTA_BINS
from .constants import (
    COL_PROC_NAME,
    COL_TIME_RANGE,
    EVENT_ATT_REASONS,
    EVENT_COMP_HLM,
    EVENT_COMP_MAIN_VIEW,
    EVENT_COMP_PERS,
    EVENT_DET_BOTT,
)
from .rules import KnownCharacteristics
from .types import (
    AnalysisSetup,
    Characteristics,
    MainView,
    Metric,
    RawStats,
    RuleResultsPerViewPerMetricPerRule,
    ScoringPerViewPerMetric,
    ViewKey,
    ViewResultsPerViewPerMetric,
    view_name,
)
from .utils.dask_utils import flatten_column_names


@dataclass
class AnalyzerResultOutputCharacteristicsType:
    complexity: float
    io_time: float
    job_time: float
    num_apps: int
    num_files: int
    num_nodes: int
    num_ops: int
    num_procs: int
    num_time_periods: int
    per_io_time: float


@dataclass
class AnalyzerResultOutputCountsType:
    raw_count: int
    hlm_count: int
    main_view_count: int
    avg_perspective_count: Dict[str, int]
    avg_perspective_std: Dict[str, float]
    perspective_skewness: Dict[str, float]
    root_perspective_skewness: Dict[str, float]
    per_records_discarded: Dict[str, float]
    per_records_retained: Dict[str, float]
    num_bottlenecks: Dict[str, int]
    num_metrics: int
    num_perspectives: int
    num_rules: int
    evaluated_records: Dict[str, int]
    perspective_count_tree: Dict[str, Dict[str, int]]
    reasoned_records: Dict[str, int]
    slope_filtered_records: Dict[str, int]


@dataclass
class AnalyzerResultOutputSeveritiesType:
    critical_count: Dict[str, int]
    very_high_count: Dict[str, int]
    high_count: Dict[str, int]
    medium_count: Dict[str, int]
    low_count: Dict[str, int]
    very_low_count: Dict[str, int]
    trivial_count: Dict[str, int]
    none_count: Dict[str, int]
    rest_count: Dict[str, int]
    critical_tree: Dict[str, Dict[str, int]]


@dataclass
class AnalyzerResultOutputThroughputsType:
    bottlenecks: Dict[str, float]
    evaluated_records: Dict[str, float]
    perspectives: Dict[str, float]
    reasoned_records: Dict[str, float]
    rules: Dict[str, float]
    slope_filtered_records: Dict[str, float]


@dataclass
class AnalyzerResultOutputTimingsType:
    read_traces: Dict[str, float]
    compute_hlm: Dict[str, float]
    compute_main_view: Dict[str, float]
    compute_perspectives: Dict[str, float]
    detect_bottlenecks: Dict[str, float]
    attach_reasons: Dict[str, float]


@dataclass
class AnalyzerResultOutputType:
    characteristics: AnalyzerResultOutputCharacteristicsType
    counts: AnalyzerResultOutputCountsType
    setup: AnalysisSetup
    severities: AnalyzerResultOutputSeveritiesType
    throughputs: AnalyzerResultOutputThroughputsType
    timings: AnalyzerResultOutputTimingsType


class AnalyzerResultOutput(object):

    def __init__(
        self,
        analysis_setup: AnalysisSetup,
        bottlenecks: RuleResultsPerViewPerMetricPerRule,
        characteristics: Characteristics,
        evaluated_views: ScoringPerViewPerMetric,
        main_view: MainView,
        raw_stats: RawStats,
        view_results: ViewResultsPerViewPerMetric,
    ) -> None:
        self.analysis_setup = analysis_setup
        self.bottlenecks = bottlenecks
        self.characteristics = characteristics
        self.evaluated_views = evaluated_views
        self.main_view = main_view
        self.raw_stats = raw_stats
        self.view_results = view_results

    def _create_output_type(self):

        job_time = float(self.raw_stats.job_time.compute())
        for characteristic in self.characteristics:
            characteristic_value = self.characteristics[characteristic].value
            if characteristic == KnownCharacteristics.APP_COUNT.value:
                num_apps = int(characteristic_value)
            elif characteristic == KnownCharacteristics.COMPLEXITY.value:
                complexity = float(characteristic_value)
            elif characteristic == KnownCharacteristics.FILE_COUNT.value:
                num_files = int(characteristic_value)
            elif characteristic == KnownCharacteristics.IO_COUNT.value:
                num_ops = int(characteristic_value)
            elif characteristic == KnownCharacteristics.IO_TIME.value:
                io_time = float(characteristic_value)
            elif characteristic == KnownCharacteristics.NODE_COUNT.value:
                num_nodes = int(characteristic_value)
            elif characteristic == KnownCharacteristics.PROC_COUNT.value:
                num_procs = int(characteristic_value)
            elif characteristic == KnownCharacteristics.TIME_PERIOD.value:
                num_time_periods = int(characteristic_value)
        per_io_time = io_time/job_time

        characteristics = AnalyzerResultOutputCharacteristicsType(
            complexity=complexity,
            io_time=io_time,
            job_time=job_time,
            num_apps=num_apps,
            num_files=num_files,
            num_nodes=num_nodes,
            num_ops=num_ops,
            num_procs=num_procs,
            num_time_periods=num_time_periods,
            per_io_time=per_io_time,
        )

        main_view_count = len(self.main_view)
        raw_count = int(self.raw_stats.total_count.compute())
        perspective_count_tree = {}
        num_metrics = 0
        perspectives = set()
        root_view_type_counts = {}
        for metric in self.view_results:
            perspective_count_tree[metric] = {}
            root_view_type_counts[metric] = []
            num_metrics = num_metrics + 1
            for view_key, view_result in self.view_results[metric].items():
                count_key = view_name(view_key, '->')
                count = len(view_result.view)
                perspective_count_tree[metric][count_key] = count
                perspectives.add(view_key)
                if len(view_key) == 1:
                    root_view_type_counts[metric].append(count)
        num_metrics = num_metrics
        num_perspectives = len(perspectives)
        avg_perspective_count = {}
        avg_perspective_std = {}
        per_records_discarded = {}
        per_records_retained = {}
        perspective_skewness = {}
        root_perspective_skewness = {}
        for metric in perspective_count_tree:
            perspective_counts = [perspective_count_tree[metric][count_key]
                                  for count_key in perspective_count_tree[metric]]
            perspective_avg = np.average(perspective_counts)
            perspective_std = np.std(perspective_counts)
            perspective_records_per = perspective_avg/raw_count
            avg_perspective_count[metric] = perspective_avg
            avg_perspective_std[metric] = perspective_std
            per_records_discarded[metric] = 1-perspective_records_per
            per_records_retained[metric] = perspective_records_per
            perspective_skewness[metric] = abs(skew(perspective_counts))
            root_perspective_skewness[metric] = abs(skew(
                root_view_type_counts[metric]))

        num_bottlenecks = {}
        num_rules = 0
        critical_count = {}
        very_high_count = {}
        high_count = {}
        medium_count = {}
        low_count = {}
        very_low_count = {}
        trivial_count = {}
        none_count = {}
        rest_count = {}
        critical_tree = {}
        for rule in self.bottlenecks:
            num_rules = num_rules + 1
            for metric in self.bottlenecks[rule]:
                num_bottlenecks[metric] = num_bottlenecks.get(metric, 0)
                critical_count[metric] = critical_count.get(metric, 0)
                very_high_count[metric] = very_high_count.get(metric, 0)
                high_count[metric] = high_count.get(metric, 0)
                medium_count[metric] = medium_count.get(metric, 0)
                low_count[metric] = low_count.get(metric, 0)
                very_low_count[metric] = very_low_count.get(metric, 0)
                trivial_count[metric] = trivial_count.get(metric, 0)
                none_count[metric] = none_count.get(metric, 0)
                rest_count[metric] = rest_count.get(metric, 0)
                critical_tree[metric] = critical_tree.get(metric, {})
                for view_key, results in self.bottlenecks[rule][metric].items():
                    count_key = view_name(view_key, '->')
                    count = critical_tree[metric].get(count_key, 0)
                    critical_tree[metric][count_key] = count
                    for result in results:
                        result_score = result.extra_data[f"{metric}_score"]
                        if result_score == DELTA_BIN_NAMES[0]:
                            none_count[metric] = none_count[metric] + 1
                        elif result_score == DELTA_BIN_NAMES[1]:
                            trivial_count[metric] = trivial_count[metric] + 1
                        elif result_score == DELTA_BIN_NAMES[2]:
                            very_low_count[metric] = very_low_count[metric] + 1
                        elif result_score == DELTA_BIN_NAMES[3]:
                            low_count[metric] = low_count[metric] + 1
                        elif result_score == DELTA_BIN_NAMES[4]:
                            medium_count[metric] = medium_count[metric] + 1
                        elif result_score == DELTA_BIN_NAMES[5]:
                            high_count[metric] = high_count[metric] + 1
                        elif result_score == DELTA_BIN_NAMES[6]:
                            very_high_count[metric] = very_high_count[metric] + 1
                        elif result_score == DELTA_BIN_NAMES[7]:
                            critical_count[metric] = critical_count[metric] + 1
                            critical_tree[metric][count_key] = critical_tree[metric][count_key] + 1
                        num_bottlenecks[metric] = num_bottlenecks[metric] + 1

        severities = AnalyzerResultOutputSeveritiesType(
            critical_count=critical_count,
            critical_tree=critical_tree,
            high_count=high_count,
            low_count=low_count,
            medium_count=medium_count,
            none_count=none_count,
            rest_count=rest_count,
            trivial_count=trivial_count,
            very_high_count=very_high_count,
            very_low_count=very_low_count,
        )

        elapsed_times = {}
        for _, event in get_client().get_events('elapsed_times'):
            elapsed_times[event['key']] = event['elapsed_time']
        attach_reasons = {}
        compute_hlm = {}
        compute_main_view = {}
        compute_perspectives = {}
        detect_bottlenecks = {}
        read_traces = {}
        attach_reasons['time'] = float(elapsed_times[EVENT_ATT_REASONS])
        compute_hlm['time'] = float(elapsed_times[EVENT_COMP_HLM])
        compute_main_view['time'] = float(elapsed_times[EVENT_COMP_MAIN_VIEW])
        compute_perspectives['time'] = float(elapsed_times[EVENT_COMP_PERS])
        detect_bottlenecks['time'] = float(elapsed_times[EVENT_DET_BOTT])
        read_traces['time'] = 0
        timings = AnalyzerResultOutputTimingsType(
            attach_reasons=attach_reasons,
            compute_hlm=compute_hlm,
            compute_main_view=compute_main_view,
            compute_perspectives=compute_perspectives,
            detect_bottlenecks=detect_bottlenecks,
            read_traces=read_traces,
        )

        evaluated_records = {}
        reasoned_records = {}
        slope_filtered_records = {}
        for metric in self.evaluated_views:
            evaluated_records[metric] = 0
            reasoned_records[metric] = 0
            slope_filtered_records[metric] = 0
            for view_key in self.evaluated_views[metric]:
                scoring = self.evaluated_views[metric][view_key]
                view_evaluated_records = len(scoring.evaluated_groups)
                view_slope_filtered_records = len(scoring.attached_records)
                evaluated_records[metric] = evaluated_records[metric] + \
                    view_evaluated_records
                reasoned_records[metric] = evaluated_records[metric] * num_rules
                slope_filtered_records[metric] = slope_filtered_records[metric] + \
                    view_slope_filtered_records

        counts = AnalyzerResultOutputCountsType(
            avg_perspective_count=avg_perspective_count,
            avg_perspective_std=avg_perspective_std,
            evaluated_records=evaluated_records,
            hlm_count=main_view_count,
            main_view_count=main_view_count,
            num_bottlenecks=num_bottlenecks,
            num_metrics=num_metrics,
            num_perspectives=num_perspectives,
            num_rules=num_rules,
            per_records_discarded=per_records_discarded,
            per_records_retained=per_records_retained,
            perspective_count_tree=perspective_count_tree,
            perspective_skewness=perspective_skewness,
            raw_count=raw_count,
            reasoned_records=reasoned_records,
            root_perspective_skewness=root_perspective_skewness,
            slope_filtered_records=slope_filtered_records,
        )

        bottlenecks_tput = {}
        perspectives_tput = {}
        reasoned_records_tput = {}
        rules_tput = {}
        slope_filtered_records_tput = {}
        evaluated_records_tput = {}

        for metric in evaluated_records:
            bottlenecks_tput[metric] = num_bottlenecks[metric] / \
                detect_bottlenecks[metric]
            evaluated_records_tput[metric] = evaluated_records[metric] / \
                detect_bottlenecks[metric]
            perspectives_tput[metric] = num_perspectives / \
                compute_perspectives[metric]
            reasoned_records_tput[metric] = (
                evaluated_records[metric]*num_rules) / attach_reasons[metric]
            rules_tput[metric] = num_rules / attach_reasons[metric]
            slope_filtered_records_tput[metric] = slope_filtered_records[metric] / \
                compute_perspectives[metric]

        throughputs = AnalyzerResultOutputThroughputsType(
            bottlenecks=bottlenecks_tput,
            evaluated_records=evaluated_records_tput,
            perspectives=perspectives_tput,
            reasoned_records=reasoned_records_tput,
            rules=rules_tput,
            slope_filtered_records=slope_filtered_records_tput,
        )

        return AnalyzerResultOutputType(
            characteristics=characteristics,
            counts=counts,
            setup=self.analysis_setup,
            severities=severities,
            throughputs=throughputs,
            timings=timings,
        )

    def console(self, max_bottlenecks_per_view_type=3, show_debug=True):

        output = self._create_output_type()

        char_table = Table(box=None, show_header=False)
        char_table.add_column(style="cyan")
        char_table.add_column()

        char_table.add_row(
            'Job Time', f"{output.characteristics.job_time:.2f} seconds")

        # Add each key-value pair to the table as a row
        for char in self.characteristics.values():
            if char.detail_list is None:
                char_table.add_row(char.description,
                                   char.value_fmt)
            else:
                detail_tree = Tree(char.value_fmt)
                for detail in char.detail_list:
                    detail_tree.add(detail)
                char_table.add_row(char.description, detail_tree)

        tree_dict = {}
        for rule in self.bottlenecks:
            tree_dict[rule] = {}
            for metric in self.bottlenecks[rule]:
                for view_key in self.bottlenecks[rule][metric]:
                    tree_dict[rule][view_key] = tree_dict[rule].get(
                        view_key, [])
                    if len(self.bottlenecks[rule][metric][view_key]) == 0:
                        continue
                    for result in self.bottlenecks[rule][metric][view_key]:
                        tree_dict[rule][view_key].append(result)

        # TODO metric
        metric = 'time'

        bott_table = Table(box=None, show_header=False)
        bott_count = 0
        for rule in tree_dict:
            rule_tree = Tree(rule)
            total_count = 0
            for view_key in tree_dict[rule]:
                results = tree_dict[rule][view_key]
                view_record_count = len(results)
                total_count = total_count + view_record_count
                view_tree = Tree(
                    f"{view_name(view_key, '->')} ({view_record_count} bottlenecks)")
                if view_record_count == 0:
                    continue
                for result in results:
                    result_score = result.extra_data[f"{metric}_score"]
                    if result.reasons is None or len(result.reasons) == 0:
                        view_tree.add(self._colored_description(
                            result.description, result_score))
                    else:
                        bott_tree = Tree(self._colored_description(
                            result.description, result_score))
                        for reason in result.reasons:
                            bott_tree.add(self._colored_description(
                                reason.description, result_score))
                        view_tree.add(bott_tree)
                    if (max_bottlenecks_per_view_type > 0 and
                            len(view_tree.children) == max_bottlenecks_per_view_type and
                            view_record_count > max_bottlenecks_per_view_type):
                        view_tree.add(
                            f"({view_record_count - max_bottlenecks_per_view_type} more)")
                        break
                rule_tree.add(view_tree)
            bott_count = bott_count + total_count
            if len(rule_tree.children) > 0:
                rule_tree.label = f"{rule_tree.label} ({total_count} bottlenecks)"
                bott_table.add_row(rule_tree)

        char_panel = Panel(char_table, title='I/O Characteristics')
        bott_panel = Panel(bott_table, title='I/O Bottlenecks')

        if show_debug:
            main_view_count = output.counts.main_view_count
            raw_total_count = output.counts.raw_count

            debug_table = Table(box=None, show_header=False)
            debug_table.add_column(style="cyan")
            debug_table.add_column()

            retained_tree = Tree(f"raw: {raw_total_count} records (100%)")
            main_view_tree = retained_tree.add(
                f"aggregated view: {main_view_count} ({main_view_count/raw_total_count*100:.2f}% 100%)")

            for metric in output.counts.perspective_count_tree:
                metric_tree = Tree((
                    f"{metric}: "
                    f"avg. {output.counts.avg_perspective_count[metric]:.2f} "
                    f"std. {output.counts.avg_perspective_std[metric]:.2f} "
                    f"({output.counts.avg_perspective_count[metric]/raw_total_count*100:.2f}% "
                    f"{output.counts.avg_perspective_count[metric]/main_view_count*100:.2f}%)"
                ))
                for view_name_str in output.counts.perspective_count_tree[metric]:
                    view_count = output.counts.perspective_count_tree[metric][view_name_str]
                    metric_tree.add((
                        f"{view_name_str}: "
                        f"{view_count} "
                        f"({view_count/raw_total_count*100:.2f}% "
                        f"{view_count/main_view_count*100:.2f}%)"
                    ))
                main_view_tree.add(metric_tree)

            debug_table.add_row('Retained Records', retained_tree)

            count_table = Table(box=None, show_header=True)
            count_table.add_column()
            count_table.add_column('Count')
            count_table.add_column('Processed Record Count')
            count_table.add_row(
                'Perspectives',
                f"{output.counts.num_perspectives}",
                f"{output.counts.slope_filtered_records['time']}",
            )
            count_table.add_row(
                'Bottlenecks',
                f"{output.counts.num_bottlenecks['time']}",
                f"{output.counts.evaluated_records['time']}",
            )
            count_table.add_row(
                'Rules',
                f"{output.counts.num_rules}",
                f"{output.counts.reasoned_records['time']}",
            )

            debug_table.add_row('Counts', count_table)

            tput_table = Table(box=None, show_header=True)
            tput_table.add_column()
            tput_table.add_column('Throughput')
            tput_table.add_column('Record Throughput')
            tput_table.add_row(
                'Perspectives',
                f"{output.throughputs.perspectives['time']:.2f} perspectives/sec",
                f"{output.throughputs.slope_filtered_records['time']:.2f} records/sec",
            )
            tput_table.add_row(
                'Bottlenecks',
                f"{output.throughputs.bottlenecks['time']:.2f} bottlenecks/sec",
                f"{output.throughputs.evaluated_records['time']:.2f} records/sec",
            )
            tput_table.add_row(
                'Rules',
                f"{output.throughputs.rules['time']:.2f} rules/sec",
                f"{output.throughputs.reasoned_records['time']:.2f} records/sec",
            )

            debug_table.add_row('Throughputs', tput_table)

            tot_bottlenecks = 0
            for metric in output.counts.num_bottlenecks:
                num_bottlenecks = output.counts.num_bottlenecks[metric]
                tot_bottlenecks = tot_bottlenecks + num_bottlenecks
            severity_tree = Tree(
                f"total: {tot_bottlenecks} bottlenecks (100%)")
            for metric in output.counts.num_bottlenecks:
                num_bottlenecks = output.counts.num_bottlenecks[metric]
                severity_metric_tree = severity_tree.add((
                    f"{metric}: "
                    f"{num_bottlenecks} "
                    f"({num_bottlenecks/tot_bottlenecks*100:.2f}% 100%)"
                ))
                severity_metric_tree.add((
                    f"critical: "
                    f"{output.severities.critical_count[metric]} "
                    f"({output.severities.critical_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.critical_count[metric]/num_bottlenecks*100:.2f}%)"
                ))
                severity_metric_tree.add((
                    f"very high: "
                    f"{output.severities.very_high_count[metric]} "
                    f"({output.severities.very_high_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.very_high_count[metric]/num_bottlenecks*100:.2f}%)"
                ))
                severity_metric_tree.add((
                    f"high: "
                    f"{output.severities.high_count[metric]} "
                    f"({output.severities.high_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.high_count[metric]/num_bottlenecks*100:.2f}%)"
                ))
                severity_metric_tree.add((
                    f"medium: "
                    f"{output.severities.medium_count[metric]} "
                    f"({output.severities.medium_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.medium_count[metric]/num_bottlenecks*100:.2f}%)"
                ))
                severity_metric_tree.add((
                    f"low: "
                    f"{output.severities.low_count[metric]} "
                    f"({output.severities.low_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.low_count[metric]/num_bottlenecks*100:.2f}%)"
                ))
                severity_metric_tree.add((
                    f"very low: "
                    f"{output.severities.very_low_count[metric]} "
                    f"({output.severities.very_low_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.very_low_count[metric]/num_bottlenecks*100:.2f}%)"
                ))
                severity_metric_tree.add((
                    f"trivial: "
                    f"{output.severities.trivial_count[metric]} "
                    f"({output.severities.trivial_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.trivial_count[metric]/num_bottlenecks*100:.2f}%)"
                ))
                severity_metric_tree.add((
                    f"none: "
                    f"{output.severities.none_count[metric]} "
                    f"({output.severities.none_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.none_count[metric]/num_bottlenecks*100:.2f}%)"
                ))

            debug_table.add_row('Severities', severity_tree)

            setup_table = Table(box=None, show_header=False)
            setup_table.add_column()
            setup_table.add_column()
            setup_table.add_row('Accuracy', output.setup.accuracy)
            setup_table.add_row(
                'Checkpoint', 'enabled' if output.setup.checkpoint else 'disabled')
            setup_table.add_row('Cluster memory', f"{output.setup.memory}")
            setup_table.add_row('Cluster # of workers',
                                f"{output.setup.num_workers}")
            setup_table.add_row('Cluster # of threads per workers',
                                f"{output.setup.num_threads_per_worker}")
            setup_table.add_row(
                'Cluster processes', 'enabled' if output.setup.processes else 'disabled')
            setup_table.add_row('Cluster type', output.setup.cluster_type)
            setup_table.add_row(
                'Debug', 'enabled' if output.setup.debug else 'disabled')
            setup_table.add_row('Metric threshold',
                                f"{output.setup.metric_threshold:.2f}")
            setup_table.add_row('Slope threshold',
                                f"{output.setup.slope_threshold:.2f}")
            debug_table.add_row('Setup', setup_table)

            debug_panel = Panel(debug_table, title='Debug')

            Console().print(char_panel, bott_panel, debug_panel)
        else:
            Console().print(char_panel, bott_panel)

    def csv(self, name: str, file_path: str, max_bottlenecks_per_view_type=3, show_debug=True):

        output = self._create_output_type()
        output_dict = asdict(obj=output)
        output_df = pd.DataFrame.from_dict(output_dict, orient='index') \
            .stack() \
            .to_frame()

        dropping_indices = []
        for ix, row in output_df.copy().iterrows():
            if isinstance(row[0], dict):
                dropping_indices.append(ix)
                nested_df = pd.json_normalize(row[0])
                for _, nested_row in nested_df.iterrows():
                    secondary_ix = None
                    value = None
                    if len(nested_df.columns) == 1:
                        secondary_ix = f"{nested_row.index[0]}__{ix[1]}"
                        value = nested_row[0]
                        output_df.loc[(ix[0], secondary_ix),] = value
                    else:
                        for nested_col in nested_df.columns:
                            suffix = ix[1].replace('_tree', '')
                            secondary_ix = f"{nested_col.replace('.', '_').replace('->', '_')}_{suffix}"
                            value = nested_row[nested_col]
                            output_df.loc[(ix[0], secondary_ix),] = value

        output_df = output_df \
            .drop(index=dropping_indices) \
            .rename(columns={output_df.columns[0]: name})

        output_df.index.set_names(['type', 'value'], inplace=True)

        output_df.sort_index().to_csv(file_path, encoding='utf8')

        timings_df = self._create_timings_df()

        timings_df.sort_values(['type', 'key']).to_csv(
            file_path.replace('.csv', '_timings.csv'), encoding='utf8')

        # TODO
        metric = 'time'
        view_result = self.view_results[metric][('time_range',)]
        view_result.slope_view[[f"{metric}_slope", f"{metric}_per_rev_cs", 'count_cs_per_rev']] \
            .compute() \
            .to_csv(file_path.replace('.csv', '_slope.csv'), encoding='utf8')

    def _colored_description(self, description: str, result_score: str = None):
        if result_score is None:
            return description
        if result_score == DELTA_BIN_NAMES[0]:
            return description
        elif result_score == DELTA_BIN_NAMES[1]:
            return f"[light_cyan3]{description}"
        elif result_score == DELTA_BIN_NAMES[2]:
            return f"[chartreuse2]{description}"
        elif result_score == DELTA_BIN_NAMES[3]:
            return f"[yellow4]{description}"
        elif result_score == DELTA_BIN_NAMES[4]:
            return f"[yellow3]{description}"
        elif result_score == DELTA_BIN_NAMES[5]:
            return f"[orange3]{description}"
        elif result_score == DELTA_BIN_NAMES[6]:
            return f"[dark_orange3]{description}"
        elif result_score == DELTA_BIN_NAMES[7]:
            return f"[red3]{description}"

    def _create_timings_df(self):
        timing_events = get_client().get_events('timings')

        timings = []
        for _, timing in timing_events:
            timings.append(timing)

        timings_df = pd.DataFrame(timings)

        timings_df = timings_df[['key', 'type', 'time', 'size']] \
            .groupby(['key', 'type']) \
            .max() \
            .reset_index() \
            .pivot(index='key', columns='type', values=['size', 'time'])

        timings_df['time', 'elapsed'] = timings_df['time',
                                                   'end'] - timings_df['time', 'start']

        timings_df = flatten_column_names(timings_df)

        timings_df['type'] = timings_df.index.map(lambda x: x.split('_')[-1])

        return timings_df


class AnalysisResultPlots(object):

    def __init__(
        self,
        main_view: MainView,
        view_results: ViewResultsPerViewPerMetric,
        evaluated_views: ScoringPerViewPerMetric,
    ):
        self.main_view = main_view
        self.view_results = view_results
        self.bottlenecks = evaluated_views  # TODO
        self._cmap = plt.cm.get_cmap('RdYlGn')

    def bottleneck_bar(
        self,
        figsize: Tuple[int, int],
        metrics: List[Metric],
        thresholds: List[float],
        markers: List[str],
        colors: List[str],
        labels: List[str] = [],
        marker_size=72,
    ):
        proc_names = list(self.main_view.reset_index()[COL_PROC_NAME].unique())
        proc_names.sort(key=lambda x: int(x.split('#')[2]))  # order by rank

        dur_mlv = self.bottlenecks['duration'][(
            COL_PROC_NAME,)]['mid_level_view']
        dur_col = next(col for col in dur_mlv.columns if 'duration' in col)
        dur_data = dur_mlv.compute()

        fig, ax = plt.subplots(figsize=figsize)

        bar_data = []
        bar_h = 1

        for y, proc_name in enumerate(proc_names):
            try:
                bar_args = dict(
                    xranges=dur_data.loc[proc_name][dur_col].to_dict().items(),
                    yrange=(y, bar_h),
                    facecolors='C0',
                    alpha=0.8,
                )

                ax.broken_barh(**bar_args)

                bar_data.append(bar_args)
            except KeyError:
                continue

        scatter_data = {}

        for m, metric in enumerate(metrics):
            scatter_data[metric] = []

            data = self.bottlenecks[metric][(
                COL_PROC_NAME,)]['mid_level_view'].compute()

            for y, proc_name in enumerate(proc_names):
                try:
                    for time_range, threshold in data.loc[proc_name][f"{metric}_th"].to_dict().items():
                        # print(proc_name, y, time_range, threshold)
                        if threshold >= thresholds[m]:
                            scatter_args = dict(
                                x=time_range,
                                y=y + (bar_h / 2),
                                s=marker_size,
                                c=colors[m],
                                marker=markers[m],
                                alpha=0.6,
                            )
                            ax.scatter(**scatter_args)

                            scatter_data[metric].append(scatter_args)

                except KeyError:
                    continue

        # len(bot_dur_proc_ml.index.get_level_values(0).unique()))
        ax.set_ylim(0, len(proc_names))
        ax.set_xlim(0, max(dur_data.index.get_level_values(1)))
        ax.set_ylabel('Ranks')
        ax.set_xlabel('Job Time')

        legend_handles = [Line2D([0], [0], color='C0', label='I/O Op')]
        for m, metric in enumerate(metrics):
            legend_handles.append(Line2D(
                xdata=[0],
                ydata=[0],
                color='w',
                label=metric if len(labels) == 0 else labels[m],
                marker=markers[m],
                markerfacecolor=colors[m],
                markersize=marker_size / 8,
            ))

        plt.legend(handles=legend_handles, loc='upper right')

        return fig, ax, bar_data, scatter_data

    def bottleneck_timeline(self, metric: Metric):
        return self._bottleneck_timeline_plot(metric=metric, figsize=(10, 5), title=metric)

    def bottleneck_timeline3(
        self,
        metric1: Metric,
        metric2: Metric,
        metric3: Metric,
        figsize: Tuple[int, int],
        label1: str = None,
        label2: str = None,
        label3: str = None,
        threshold=0.0,
        sample_count=0,
    ):
        # plt.style.use('seaborn-poster')
        fig = plt.figure()

        ax1_line, _ = self._bottleneck_timeline_plot(
            metric=metric1,
            figsize=figsize,
            threshold=threshold,
            yaxis_formatter=self._ticker_for_metric(metric1),
            yaxis_label=label1,
            sample_count=sample_count,
            scatter_zorder=4,
        )
        ax1_line.set_xlabel('Timeline')

        ax2 = ax1_line.twinx()
        ax2.spines['right'].set_position(('axes', 1.0))
        self._bottleneck_timeline_plot(
            metric=metric2,
            figsize=figsize,
            ax=ax2,
            color='C4',
            marker='x',
            threshold=threshold,
            yaxis_formatter=self._ticker_for_metric(metric2),
            yaxis_label=label2,
            sample_count=sample_count,
            scatter_zorder=5,
        )

        ax3 = ax1_line.twinx()
        ax3.spines['right'].set_position(('axes', 1.25))
        self._bottleneck_timeline_plot(
            metric=metric3,
            figsize=figsize,
            ax=ax3,
            color='C5',
            marker='v',
            threshold=threshold,
            yaxis_formatter=self._ticker_for_metric(metric3),
            yaxis_label=label3,
            sample_count=sample_count,
            scatter_zorder=6,
        )

        plt.tight_layout()

        legend_handles = [
            Line2D([0], [0], color='C0', label=label1, lw=2, marker='o'),
            Line2D([0], [0], color='C4', label=label2, lw=2, marker='X'),
            Line2D([0], [0], color='C5', label=label3, lw=2, marker='v'),
        ]

        plt.legend(handles=legend_handles, loc='upper right')

        # Add the colorbar separately using the RdYlGn colormap
        # You can adjust the position and size of the colorbar as desired
        cmap = plt.cm.RdYlGn  # Choose the RdYlGn colormap
        norm = plt.Normalize(vmin=0, vmax=1)  # Normalize the data
        mappable = plt.cm.ScalarMappable(
            norm=norm, cmap=cmap)  # Create the mappable
        mappable.set_array(DELTA_BINS)  # Set the data for the colorbar

        # Position the colorbar within the figure
        # Left, bottom, width, and height are in fractions of the figure size (0 to 1)
        # You can adjust these values
        colorbar_ax = fig.add_axes([0.68, 0.1, 0.2, 0.03])
        colorbar = plt.colorbar(
            mappable, cax=colorbar_ax, orientation='horizontal')
        colorbar.set_ticklabels(['critical', 'medium', 'trivial'])
        # Adjust the font size as needed
        colorbar.ax.tick_params(labelsize=12, pad=2)

        # Add a label to the colorbar
        # colorbar.set_label('Colorbar Label')
        colorbar_label = 'Bottleneck Severity'
        # Position the label at the top of the colorbar
        colorbar.ax.xaxis.set_label_position('top')
        # Adjust font size and labelpad as needed
        colorbar.ax.set_xlabel(colorbar_label, fontsize=12, labelpad=4)

        return fig

    def _bottleneck_timeline_plot(
        self,
        metric: Metric,
        figsize: Tuple[int, int],
        threshold: float = 0,
        ax: Axes = None,
        title: str = None,
        color='C0',
        marker='o',
        marker_size=96,
        yaxis_formatter: ticker.Formatter = None,
        yaxis_label: str = None,
        sample_count=0,
        scatter_zorder=0,
    ):
        bott = self.bottlenecks[metric][(COL_TIME_RANGE,)].bottlenecks
        metric_col = next(col for col in bott.columns if metric in col)
        data = bott.compute()
        ax_line = data[metric_col].plot(
            ax=ax, color=color, figsize=figsize, title=title, alpha=0.8)
        if yaxis_formatter is not None:
            ax_line.yaxis.set_major_formatter(yaxis_formatter)
        if yaxis_label is not None:
            ax_line.yaxis.set_label(yaxis_label)
        filtered_data = data.query(f"{metric}_th >= {threshold}").reset_index()
        if sample_count > 0:
            filtered_data = filtered_data.sort_values(
                f"{metric}_th", ascending=False).head(sample_count)
        colors = np.vectorize(self._color_map)(filtered_data[f"{metric}_th"])
        ax_scatter = filtered_data.plot.scatter(
            ax=ax_line,
            x=COL_TIME_RANGE,
            y=metric_col,
            c=colors,
            cmap=self._cmap,
            marker=marker,
            s=marker_size,
            zorder=scatter_zorder,
        )
        ax_scatter.set_ylabel(yaxis_label)
        return ax_line, ax_scatter

    def metric_relations2(
        self,
        view_key: ViewKey,
        metric1: Metric,
        metric2: Metric,
        label1: str = None,
        label2: str = None,
    ):
        return self._metric_relations(
            view_key=view_key,
            metrics=[metric1, metric2],
            labels=[label1, label2],
        )

    def metric_relations3(
        self,
        view_key: ViewKey,
        metric1: Metric,
        metric2: Metric,
        metric3: Metric,
        label1: str = None,
        label2: str = None,
        label3: str = None,
    ):
        return self._metric_relations(
            view_key=view_key,
            metrics=[metric1, metric2, metric3],
            labels=[label1, label2, label3],
        )

    def _metric_relations(self, view_key: ViewKey, metrics: List[Metric], labels: List[str]):
        sets = [set(self.view_results[metric][view_key].view['id'].unique().compute())
                for metric in metrics]
        labels = self._venn_labels(sets, labels, metrics)
        fig, ax = venn.venn3(labels, names=metrics, figsize=(5, 5))
        # ax.get_legend().remove()
        # fig.tight_layout()
        return fig, ax

    def slope(
        self,
        metric: Metric,
        view_keys: List[ViewKey],
        legends: List[str] = [],
        slope_threshold: int = 45,
        ax: Axes = None,
        xlabel: str = None,
        ylabel: str = None,
        figsize: Tuple[int, int] = None,
        color: str = None,
    ):
        if ax is None:
            _, ax = plt.subplots(figsize=figsize)
        legend_handles = []
        x_col = f"{metric}_per_rev_cs"
        y_col = 'count_cs_per_rev'
        for i, view_key in enumerate(view_keys):
            view_result = self.view_results[metric][view_key]
            slope_view = view_result.slope_view.compute()
            color = f"C{i}" if color is None else color
            self._plot_slope(
                ax=ax,
                color=color,
                metric=metric,
                slope_threshold=slope_threshold,
                slope_view=slope_view,
                x_col=x_col,
                y_col=y_col,
            )
            if len(legends) > 0:
                legend_handles.append(
                    Line2D([0], [0], color=color, label=legends[i]))
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        if xlabel is not None:
            ax.set_xlabel(xlabel)
        if ylabel is not None:
            ax.set_ylabel(ylabel)
        if len(legend_handles) > 0:
            ax.legend(handles=legend_handles)
        return ax, slope_view

    @staticmethod
    def _plot_slope(
        ax: Axes,
        color: str,
        metric: Metric,
        slope_threshold: int,
        slope_view: pd.DataFrame,
        x_col: str,
        y_col: str,
    ):
        slope_cond = slope_view[f"{metric}_slope"] < slope_threshold
        slope_view.loc[slope_cond, f"{x_col}_line"] = slope_view[x_col]
        line = slope_view[f"{x_col}_line"].to_numpy()
        x = slope_view[x_col].to_numpy()
        y = slope_view[y_col].to_numpy()
        last_non_nan_index = np.where(~np.isnan(line))[0][-1]
        dotted = np.copy(line)
        if np.all(np.isnan(line[last_non_nan_index + 1:])):
            # complete dotted line if all values after last non-nan are nan
            dotted[last_non_nan_index + 1:] = x[last_non_nan_index + 1:]
        mask = np.isfinite(dotted)
        ax.plot(dotted[mask], y[mask], c=color, ls=':')
        ax.plot(line, y, c=color)

    def view_relations2(
        self,
        metric: Metric,
        view_key1: ViewKey,
        view_key2: ViewKey,
        label1: str = None,
        label2: str = None,
    ):
        return self._view_relations(
            metric=metric,
            view_keys=[view_key1, view_key2],
            labels=[label1, label2],
        )

    def view_relations3(
        self,
        metric: Metric,
        view_key1: ViewKey,
        view_key2: ViewKey,
        view_key3: ViewKey,
        label1: str = None,
        label2: str = None,
        label3: str = None,
    ):
        return self._view_relations(
            metric=metric,
            view_keys=[view_key1, view_key2, view_key3],
            labels=[label1, label2, label3],
        )

    def _view_relations(self, metric: Metric, view_keys: List[ViewKey], labels: List[str]):
        names = [view_name(view_key).replace('_', '\_')
                 for view_key in view_keys]
        sets = [set(self.view_results[metric][view_key].view['id'].unique().compute())
                for view_key in view_keys]
        labels = self._venn_labels(sets, labels, names)
        fig, ax = venn.venn3(labels, names=names, figsize=(5, 5))
        # ax.get_legend().remove()
        # fig.tight_layout()
        return fig, ax

    @staticmethod
    def _color_map(threshold: float):
        if threshold >= 0.9:
            return 'red'
        elif threshold >= 0.75:
            return 'darkorange'
        elif threshold >= 0.5:
            return 'orange'
        elif threshold >= 0.25:
            return 'gold'
        elif threshold >= 0.1:
            return 'yellow'
        elif threshold >= 0.01:
            return 'yellowgreen'
        elif threshold >= 0.001:
            return 'limegreen'
        else:
            return 'green'

    def _ticker_for_metric(self, metric: Metric):
        if metric == 'bw':
            return self._ticker_bw_formatter
        elif metric == 'duration':
            return ticker.StrMethodFormatter('{x:.1f}')
        elif metric == 'iops':
            return self._ticker_human_formatter
        return None

    @ticker.FuncFormatter
    def _ticker_human_formatter(x, pos):
        num = float('{:.3g}'.format(x))
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])

    @ticker.FuncFormatter
    def _ticker_bw_formatter(x, pos):
        return f"{math.ceil(x / 1024.0 ** 3)}"

    @staticmethod
    def _venn_labels(sets: List[Set], labels: List[str], names: List[str]):
        fixed_labels = {}
        for label, value in venn.get_labels(sets).items():
            label_value = int(value)
            if label_value > 1_000_000:
                fixed_labels[label] = f"{label_value // 1_000_000}M"
            if label_value > 1_000:
                fixed_labels[label] = f"{label_value // 1_000}K"
            else:
                fixed_labels[label] = value

        pos_arr = ['10', '01'] if len(sets) == 2 else ['100', '010', '001']
        for index, pos in enumerate(pos_arr):
            bold_label = names[index] if labels[index] == None else labels[index]
            fixed_labels[pos] = f"{fixed_labels[pos]}\n" + \
                rf"$\bf{{{bold_label}}}$"
        return fixed_labels


class AnalysisResult(object):

    def __init__(
        self,
        analysis_setup: AnalysisSetup,
        bottlenecks: RuleResultsPerViewPerMetricPerRule,
        characteristics: Characteristics,
        evaluated_views: ScoringPerViewPerMetric,
        main_view: MainView,
        metric_boundaries,
        raw_stats: RawStats,
        view_results: ViewResultsPerViewPerMetric,
    ):
        self.analysis_setup = analysis_setup
        self.bottlenecks = bottlenecks
        self.characteristics = characteristics
        self.evaluated_views = evaluated_views
        self.main_view = main_view
        self.metric_boundaries = metric_boundaries
        self.raw_stats = raw_stats
        self.view_results = view_results

        self.output = AnalyzerResultOutput(
            analysis_setup=analysis_setup,
            characteristics=characteristics,
            bottlenecks=bottlenecks,
            evaluated_views=evaluated_views,
            main_view=main_view,
            raw_stats=raw_stats,
            view_results=view_results,
        )

        self.plots = AnalysisResultPlots(
            evaluated_views=evaluated_views,
            main_view=main_view,
            view_results=view_results,
        )
