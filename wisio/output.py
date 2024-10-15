import abc
import dask
import dask.dataframe as dd
import dataclasses
import inflect
import numpy as np
import pandas as pd
import sqlite3
from dask.distributed import get_client
from hydra.core.hydra_config import HydraConfig
from pandas.api.types import is_numeric_dtype
from rich.console import Console
from rich.panel import Panel
from rich.padding import Padding
from rich.table import Table
from rich.terminal_theme import DEFAULT_TERMINAL_THEME
from rich.tree import Tree
from scipy.stats import skew
from typing import Dict, List

from .analysis import SCORE_INITIALS, SCORE_NAMES
from .constants import (
    COL_APP_NAME,
    COL_FILE_DIR,
    COL_FILE_NAME,
    COL_FILE_PATTERN,
    COL_NODE_NAME,
    COL_PROC_NAME,
    COL_RANK,
    EVENT_ATT_REASONS,
    EVENT_COMP_HLM,
    EVENT_COMP_MAIN_VIEW,
    EVENT_COMP_PERS,
    EVENT_DET_BOT,
    EVENT_READ_TRACES,
    EVENT_SAVE_BOT,
)
from .rules import (
    HUMANIZED_KNOWN_RULES,
    MAX_REASONS,
    BottleneckRule,
    KnownCharacteristics,
)
from .types import (
    AnalyzerResultType,
    Characteristics,
    Metric,
    OutputCharacteristicsType,
    OutputCountsType,
    OutputSeveritiesType,
    OutputThroughputsType,
    OutputTimingsType,
    OutputType,
    RawStats,
    Score,
    humanized_metric_name,
    humanized_view_name,
    view_name as format_view_name,
)


class Output(abc.ABC):
    def __init__(
        self,
        compact: bool = False,
        group_behavior: bool = False,
        name: str = "",
        root_only: bool = False,
        view_names: List[str] = [],
    ):
        self.compact = compact
        self.group_behavior = group_behavior
        self.name = name
        self.output_dir = HydraConfig.get().runtime.output_dir
        self.pluralize = inflect.engine()
        self.root_only = root_only
        self.view_names = view_names

    def handle_result(self, metrics: List[Metric], result: AnalyzerResultType):
        raise NotImplementedError

    def _create_output_df(
        self, metric: Metric, result: AnalyzerResultType
    ) -> pd.DataFrame:
        output = self._create_output_type(
            group_behavior=self.group_behavior,
            metric=metric,
            result=result,
        )
        output_dict = dataclasses.asdict(obj=output)
        for output_key in output_dict.copy():
            if output_key.startswith('_'):
                output_dict.pop(output_key)
        output_df = (
            pd.DataFrame.from_dict(output_dict, orient='index').stack().to_frame()
        )

        dropping_indices = []
        for ix, row in output_df.copy().iterrows():
            if isinstance(row[0], dict):
                dropping_indices.append(ix)
                type_type = ix[0]
                value_type = ix[1]
                nested_df = pd.json_normalize(row[0])
                for _, nested_row in nested_df.iterrows():
                    secondary_ix = None
                    value = None
                    if '_tree' in value_type:
                        for nested_col in nested_df.columns:
                            suffix = value_type.replace('_tree', '')
                            secondary_ix = f"{nested_col.replace('.', '_').replace('>', '_')}_{suffix}"
                            value = nested_row[nested_col]
                            output_df.loc[(type_type, secondary_ix),] = float(value)
                    else:
                        secondary_ix = f"{nested_row.index[0]}__{value_type}"
                        value = nested_row[0]
                        output_df.loc[(type_type, secondary_ix),] = float(value)

        output_df = output_df.drop(index=dropping_indices).rename(
            columns={output_df.columns[0]: self.name}
        )

        output_df.index.set_names(['type', 'value'], inplace=True)

        return output_df.sort_index()

    def _create_output_type(
        self,
        characteristics: Characteristics,
        metric: Metric,
        group_behavior: bool,
        raw_stats: RawStats,
        result: AnalyzerResultType,
    ) -> OutputType:
        bottlenecks = self._read_bottlenecks(
            bottleneck_dir=result.bottleneck_dir,
            group_behavior=group_behavior,
            metric=metric,
        )

        complexity = 0
        io_time = 0
        job_time = float(raw_stats["job_time"])
        num_apps = 0
        num_files = 0
        num_nodes = 0
        num_ops = 0
        num_procs = 0
        num_time_periods = 0
        for characteristic in characteristics:
            characteristic_value = characteristics[characteristic].value
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

        per_io_time = 0
        if job_time > 0:
            per_io_time = io_time / job_time

        output_characteristics = OutputCharacteristicsType(
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

        main_view_count = len(result.main_view)
        raw_count = int(raw_stats["total_count"])

        perspective_count_tree = {}
        perspective_critical_count_tree = {}
        perspective_record_count_tree = {}
        num_metrics = 0
        perspectives = set()
        for metric in result.view_results:
            perspective_count_tree[metric] = {}
            perspective_critical_count_tree[metric] = {}
            perspective_record_count_tree[metric] = {}
            num_metrics = num_metrics + 1
            for view_key, view_result in result.view_results[metric].items():
                count_key = format_view_name(view_key, '>')
                bot_important_count = view_result.view.reduction(len, sum)
                view_critical_count = view_result.critical_view.reduction(len, sum)
                view_record_count = view_result.records.reduction(len, sum)
                perspective_count_tree[metric][count_key] = bot_important_count
                perspective_critical_count_tree[metric][count_key] = view_critical_count
                perspective_record_count_tree[metric][count_key] = view_record_count
                perspectives.add(view_key)
        num_metrics = num_metrics
        num_perspectives = len(perspectives)
        (
            perspective_count_tree,
            perspective_critical_count_tree,
            perspective_record_count_tree,
        ) = dask.compute(
            perspective_count_tree,
            perspective_critical_count_tree,
            perspective_record_count_tree,
        )
        root_view_type_counts = {}
        for metric in result.view_results:
            root_view_type_counts[metric] = []
            for view_key, view_result in result.view_results[metric].items():
                if len(view_key) == 1:
                    view_critical_count = perspective_critical_count_tree[metric][
                        count_key
                    ]
                    root_view_type_counts[metric].append(view_critical_count)

        avg_perspective_count = {}
        avg_perspective_count_std = {}
        avg_perspective_critical_count = {}
        avg_perspective_critical_count_std = {}
        per_records_discarded = {}
        per_records_retained = {}
        perspective_skewness = {}
        root_perspective_skewness = {}
        for metric in perspective_count_tree:
            perspective_counts = [
                perspective_count_tree[metric][count_key]
                for count_key in perspective_count_tree[metric]
            ]
            perspective_avg = np.average(perspective_counts)
            perspective_std = np.std(perspective_counts)

            perspective_critical_counts = [
                perspective_critical_count_tree[metric][count_key]
                for count_key in perspective_critical_count_tree[metric]
            ]
            perspective_critical_avg = np.average(perspective_critical_counts)
            perspective_critical_std = np.std(perspective_critical_counts)

            avg_perspective_count[metric] = perspective_avg
            avg_perspective_count_std[metric] = perspective_std
            avg_perspective_critical_count[metric] = perspective_critical_avg
            avg_perspective_critical_count_std[metric] = perspective_critical_std

            perspective_critical_per = perspective_critical_avg / raw_count
            per_records_discarded[metric] = 1 - perspective_critical_per
            per_records_retained[metric] = perspective_critical_per

            perspective_skewness[metric] = abs(skew(perspective_counts))
            root_perspective_skewness[metric] = abs(skew(root_view_type_counts[metric]))

        num_rules = len(result.bottleneck_rules)
        view_names = list(bottlenecks['view_name'].unique())

        bot_total_count = {}
        bot_critical_count = {}
        bot_very_high_count = {}
        bot_high_count = {}
        bot_medium_count = {}
        bot_low_count = {}
        bot_very_low_count = {}
        bot_trivial_count = {}
        bot_none_count = {}

        bot_total_count[metric] = len(bottlenecks)
        bot_critical_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.CRITICAL.value]
        )
        bot_very_high_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.VERY_HIGH.value]
        )
        bot_high_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.HIGH.value]
        )
        bot_medium_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.MEDIUM.value]
        )
        bot_low_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.LOW.value]
        )
        bot_very_low_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.VERY_LOW.value]
        )
        bot_trivial_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.TRIVIAL.value]
        )
        bot_none_count[metric] = len(
            bottlenecks[bottlenecks[f"{metric}_score"] == Score.NONE.value]
        )

        bot_root_critical_count = {}
        bot_root_very_high_count = {}
        bot_root_high_count = {}
        bot_root_medium_count = {}
        bot_root_low_count = {}
        bot_root_very_low_count = {}
        bot_root_trivial_count = {}
        bot_root_none_count = {}

        bot_root_critical_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.CRITICAL.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )
        bot_root_very_high_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.VERY_HIGH.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )
        bot_root_high_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.HIGH.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )
        bot_root_medium_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.MEDIUM.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )
        bot_root_low_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.LOW.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )
        bot_root_very_low_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.VERY_LOW.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )
        bot_root_trivial_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.TRIVIAL.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )
        bot_root_none_count[metric] = len(
            bottlenecks[
                (bottlenecks[f"{metric}_score"] == Score.NONE.value)
                & (bottlenecks['view_depth'] == 1)
            ]
        )

        bot_critical_tree = {}
        bot_very_high_tree = {}
        bot_high_tree = {}
        bot_medium_tree = {}

        bot_critical_tree[metric] = {
            view_name: len(
                bottlenecks[
                    (bottlenecks[f"{metric}_score"] == Score.CRITICAL.value)
                    & (bottlenecks['view_name'] == view_name)
                ]
            )
            for view_name in view_names
        }
        bot_very_high_tree[metric] = {
            view_name: len(
                bottlenecks[
                    (bottlenecks[f"{metric}_score"] == Score.VERY_HIGH.value)
                    & (bottlenecks['view_name'] == view_name)
                ]
            )
            for view_name in view_names
        }
        bot_high_tree[metric] = {
            view_name: len(
                bottlenecks[
                    (bottlenecks[f"{metric}_score"] == Score.HIGH.value)
                    & (bottlenecks['view_name'] == view_name)
                ]
            )
            for view_name in view_names
        }
        bot_medium_tree[metric] = {
            view_name: len(
                bottlenecks[
                    (bottlenecks[f"{metric}_score"] == Score.MEDIUM.value)
                    & (bottlenecks['view_name'] == view_name)
                ]
            )
            for view_name in view_names
        }

        severities = OutputSeveritiesType(
            critical_count=bot_critical_count,
            very_high_count=bot_very_high_count,
            high_count=bot_high_count,
            medium_count=bot_medium_count,
            low_count=bot_low_count,
            very_low_count=bot_very_low_count,
            trivial_count=bot_trivial_count,
            none_count=bot_none_count,
            critical_tree=bot_critical_tree,
            very_high_tree=bot_very_high_tree,
            high_tree=bot_high_tree,
            medium_tree=bot_medium_tree,
            root_critical_count=bot_root_critical_count,
            root_very_high_count=bot_root_very_high_count,
            root_high_count=bot_root_high_count,
            root_medium_count=bot_root_medium_count,
            root_low_count=bot_root_low_count,
            root_very_low_count=bot_root_very_low_count,
            root_trivial_count=bot_root_trivial_count,
            root_none_count=bot_root_none_count,
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
        save_bottlenecks = {}
        attach_reasons[metric] = float(elapsed_times.get(EVENT_ATT_REASONS, 0))
        compute_hlm[metric] = float(elapsed_times.get(EVENT_COMP_HLM, 0))
        compute_main_view[metric] = float(elapsed_times.get(EVENT_COMP_MAIN_VIEW, 0))
        compute_perspectives[metric] = float(elapsed_times.get(EVENT_COMP_PERS, 0))
        detect_bottlenecks[metric] = float(elapsed_times.get(EVENT_DET_BOT, 0))
        read_traces[metric] = float(elapsed_times.get(EVENT_READ_TRACES, 0))
        save_bottlenecks[metric] = float(elapsed_times.get(EVENT_SAVE_BOT, 0))
        timings = OutputTimingsType(
            attach_reasons=attach_reasons,
            compute_hlm=compute_hlm,
            compute_main_view=compute_main_view,
            compute_perspectives=compute_perspectives,
            detect_bottlenecks=detect_bottlenecks,
            read_traces=read_traces,
            save_bottlenecks=save_bottlenecks,
        )

        evaluated_record_dict = {}
        slope_filtered_record_dict = {}
        evaluated_record_dict[metric] = {}
        slope_filtered_record_dict[metric] = {}
        for view_key in result.evaluated_views[metric]:
            scoring = result.evaluated_views[metric][view_key]
            evaluated_record_dict[metric][view_key] = scoring.critical_view.reduction(
                len, sum
            )
            slope_filtered_record_dict[metric][view_key] = (
                scoring.records_index.reduction(len, sum)
            )
        (
            evaluated_record_dict,
            slope_filtered_record_dict,
        ) = dask.compute(
            evaluated_record_dict,
            slope_filtered_record_dict,
        )
        evaluated_records = {}
        reasoned_records = {}
        slope_filtered_records = {}
        evaluated_records[metric] = 0
        reasoned_records[metric] = 0
        slope_filtered_records[metric] = 0
        for view_key in result.evaluated_views[metric]:
            view_evaluated_records = evaluated_record_dict[metric][view_key]
            view_slope_filtered_records = slope_filtered_record_dict[metric][view_key]
            evaluated_records[metric] = (
                evaluated_records[metric] + view_evaluated_records
            )
            reasoned_records[metric] = evaluated_records[metric] * num_rules
            slope_filtered_records[metric] = (
                slope_filtered_records[metric] + view_slope_filtered_records
            )

        counts = OutputCountsType(
            avg_perspective_count=avg_perspective_count,
            avg_perspective_count_std=avg_perspective_count_std,
            avg_perspective_critical_count=avg_perspective_critical_count,
            avg_perspective_critical_count_std=avg_perspective_critical_count_std,
            evaluated_records=evaluated_records,
            hlm_count=main_view_count,
            main_view_count=main_view_count,
            num_bottlenecks=bot_total_count,
            num_metrics=num_metrics,
            num_perspectives=num_perspectives,
            num_rules=num_rules,
            per_records_discarded=per_records_discarded,
            per_records_retained=per_records_retained,
            perspective_count_tree=perspective_count_tree,
            perspective_critical_count_tree=perspective_critical_count_tree,
            perspective_record_count_tree=perspective_record_count_tree,
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
            bottlenecks_tput[metric] = (
                bot_total_count[metric] / detect_bottlenecks[metric]
            )
            evaluated_records_tput[metric] = (
                evaluated_records[metric] / detect_bottlenecks[metric]
            )
            perspectives_tput[metric] = num_perspectives / compute_perspectives[metric]
            reasoned_records_tput[metric] = (
                evaluated_records[metric] * num_rules
            ) / attach_reasons[metric]
            rules_tput[metric] = num_rules / attach_reasons[metric]
            slope_filtered_records_tput[metric] = (
                slope_filtered_records[metric] / compute_perspectives[metric]
            )

        throughputs = OutputThroughputsType(
            bottlenecks=bottlenecks_tput,
            evaluated_records=evaluated_records_tput,
            perspectives=perspectives_tput,
            reasoned_records=reasoned_records_tput,
            rules=rules_tput,
            slope_filtered_records=slope_filtered_records_tput,
        )

        return OutputType(
            _bottlenecks=bottlenecks,
            _characteristics=characteristics,
            _raw_stats=raw_stats,
            characteristics=output_characteristics,
            counts=counts,
            severities=severities,
            throughputs=throughputs,
            timings=timings,
        )

    def _read_bottlenecks(
        self,
        metric: Metric,
        group_behavior: bool,
        bottleneck_dir: str,
    ):
        bottlenecks = dd.read_parquet(bottleneck_dir).query(
            'metric == @metric', local_dict={'metric': metric}
        )

        if group_behavior:
            cols = bottlenecks.columns

            agg_dict = {}
            if group_behavior:
                agg_dict['behavior'] = 'count'  # count behaviors
            for col in cols:
                if 'num_' in col:
                    agg_dict[col] = 'sum'
                elif is_numeric_dtype(bottlenecks[col]):
                    agg_dict[col] = 'mean'
                else:
                    agg_dict[col] = 'first'

            if group_behavior:
                bottlenecks = (
                    bottlenecks.groupby(['view_name', f"{metric}_score", 'behavior'])
                    .agg(agg_dict)
                    .compute()
                )
            else:
                bottlenecks = (
                    bottlenecks.groupby(['view_name', f"{metric}_score"])
                    .agg(agg_dict)
                    .compute()
                )

            # TODO find a better way
            reason_cols = cols[cols.str.contains('reason')]
            rule_cols = [reason.split('.')[0] for reason in reason_cols]

            for col in [*reason_cols, *rule_cols]:
                bottlenecks[col] = bottlenecks[col].astype(bool)

            for col in cols:
                if 'count' in col or 'num_' in col:
                    bottlenecks[col] = bottlenecks[col].astype(int)

        else:
            bottlenecks = bottlenecks.compute()

        bottlenecks['id'] = np.arange(len(bottlenecks)) + 1  # set ids

        return bottlenecks


class ConsoleOutput(Output):
    def __init__(
        self,
        compact: bool = False,
        group_behavior: bool = False,
        max_bottlenecks: int = 3,
        name: str = "",
        root_only: bool = False,
        show_characteristics: bool = True,
        show_debug: bool = False,
        show_header: bool = True,
        view_names: List[str] = [],
    ):
        super().__init__(compact, group_behavior, name, root_only, view_names)
        self.max_bottlenecks = max_bottlenecks
        self.show_characteristics = show_characteristics
        self.show_debug = show_debug
        self.show_header = show_header

    def handle_result(self, metrics: List[Metric], result: AnalyzerResultType):
        (characteristics,) = dask.compute(result.characteristics)
        (raw_stats,) = dask.compute(result.raw_stats)

        print_objects = []

        if self.show_characteristics:
            char_table = self._create_characteristics_table(
                characteristics=characteristics,
                compact=self.compact,
                job_time=float(raw_stats["job_time"]),
            )
            char_panel = Panel(
                renderable=char_table,
                title=' '.join([self.name, 'I/O Characteristics']).strip()
                if self.show_header
                else None,
                subtitle=(
                    '[bold]R[/bold]: Read - '
                    '[bold]W[/bold]: Write - '
                    '[bold]M[/bold]: Metadata '
                ),
                subtitle_align='left',
                padding=1,
            )
            print_objects.append(char_panel)
            print_objects.append(Padding(''))

        for metric in metrics:
            output = self._create_output_type(
                characteristics=characteristics,
                group_behavior=self.group_behavior,
                metric=metric,
                raw_stats=raw_stats,
                result=result,
            )

            bottlenecks = output._bottlenecks
            if len(self.view_names) > 0:
                bottlenecks = bottlenecks.query(
                    'view_name in @view_names',
                    local_dict={'view_names': self.view_names},
                )
            elif self.root_only:
                bottlenecks = bottlenecks[bottlenecks['view_depth'] == 1]

            bot_table, total_bot_count, total_reason_count = (
                self._create_bottleneck_table(
                    bottleneck_rules=result.bottleneck_rules,
                    bottlenecks=bottlenecks,
                    compact=self.compact,
                    max_bottlenecks=self.max_bottlenecks,
                    metric=metric,
                    pluralize=self.pluralize,
                )
            )

            bot_panel = Panel(
                bot_table,
                title=(
                    f"{humanized_metric_name(metric)}: "
                    f"{total_bot_count} I/O {self.pluralize.plural_noun('Bottleneck', total_bot_count)} with "
                    f"{total_reason_count} {self.pluralize.plural_noun('Reason', total_reason_count)}"
                )
                if self.show_header
                else None,
                padding=1,
            )

            print_objects.append(bot_panel)
            print_objects.append(Padding(''))

            if self.show_debug:
                debug_table = self._create_debug_table(metric=metric, output=output)
                debug_panel = Panel(debug_table, title='Debug', padding=1)
                print_objects.append(debug_panel)
                print_objects.append(Padding(''))

        console = Console(record=True)
        console.print(*print_objects)
        console.save_html(f"{self.output_dir}/result.html", clear=False)
        console.save_text(f"{self.output_dir}/result.txt", clear=False)
        console.save_svg(
            f"{self.output_dir}/result.svg",
            theme=DEFAULT_TERMINAL_THEME,
            title="WisIO",
            clear=True,
        )

    def _create_bottleneck_table(
        self,
        bottleneck_rules: Dict[str, BottleneckRule],
        bottlenecks: pd.DataFrame,
        compact: bool,
        max_bottlenecks: int,
        metric: str,
        pluralize: inflect.engine,
    ):
        view_names = list(bottlenecks['view_name'].unique())

        bot_table = Table(box=None, show_header=False)

        total_bot_count = 0
        total_reason_count = 0

        for view_name2 in view_names:
            view_bottlenecks = bottlenecks[bottlenecks['view_name'] == view_name2]
            bot_count = len(view_bottlenecks)
            total_bot_count = total_bot_count + bot_count
            if bot_count == 0:
                continue
            reason_cols = view_bottlenecks.columns[
                view_bottlenecks.columns.str.contains('reason')
            ]
            reason_rules = [(reason.split('.')[0], reason) for reason in reason_cols]
            reason_count = sum(
                len(
                    view_bottlenecks[
                        (view_bottlenecks[rule] & view_bottlenecks[reason])
                    ]
                )
                for rule, reason in reason_rules
            )
            total_reason_count = total_reason_count + reason_count
            view_key = tuple(view_name2.split('.'))
            view_tree = Tree(
                (
                    f"{humanized_view_name(view_key, '>').replace(' Period', '')} View "
                    f"({bot_count} {pluralize.plural_noun('bottleneck', bot_count)} "
                    f"with {reason_count} {pluralize.plural_noun('reason', reason_count)})"
                )
            )
            for _, bottleneck in view_bottlenecks[:max_bottlenecks].iterrows():
                bot_id = getattr(bottleneck, 'id')
                bot_desc = None
                bot_score = getattr(bottleneck, f"{metric}_score")
                reasons = []
                for rule, rule_impl in bottleneck_rules.items():
                    view_type = view_key[-1]

                    num_files = int(getattr(bottleneck, 'num_file_name', 0))
                    num_processes = int(getattr(bottleneck, 'num_proc_name', 0))
                    num_time_periods = int(getattr(bottleneck, 'num_time_range', 0))

                    if view_type in [COL_FILE_NAME, COL_FILE_DIR, COL_FILE_PATTERN]:
                        num_files = int(getattr(bottleneck, f"num_{view_type}", 0))
                    if view_type in [
                        COL_APP_NAME,
                        COL_NODE_NAME,
                        COL_PROC_NAME,
                        COL_RANK,
                    ]:
                        num_processes = int(getattr(bottleneck, f"num_{view_type}", 0))

                    # TODO move to upper level
                    if bot_desc is None:
                        bot_desc = rule_impl.describe_bottleneck(
                            compact=compact,
                            metric=getattr(bottleneck, 'metric'),
                            num_files=num_files,
                            num_ops=int(bottleneck['count']),
                            num_processes=num_processes,
                            num_time_periods=num_time_periods,
                            subject=getattr(bottleneck, 'subject'),
                            time=float(getattr(bottleneck, 'time')),
                            time_overall=float(getattr(bottleneck, 'time_overall')),
                            view_type=view_key[-1],
                        )

                    humanized_rule = HUMANIZED_KNOWN_RULES[rule]

                    # Check if rule applies
                    if getattr(bottleneck, rule, False):
                        num_reasons = len(rule_impl.rule.reasons)

                        # Check if any reason is found
                        if any(
                            getattr(bottleneck, f"{rule}.reason.{i}", False)
                            for i in range(num_reasons)
                        ):
                            for i in range(num_reasons):
                                # Check if reason applies
                                if getattr(bottleneck, f"{rule}.reason.{i}", False):
                                    reason = rule_impl.describe_reason(
                                        bottleneck=dict(bottleneck),
                                        reason_index=i,
                                    )
                                    reasons.append(f"[{humanized_rule}] {reason}")
                        else:
                            # TODO give details
                            reasons.append(
                                f"[{humanized_rule}] No reason found, investigation needed!"
                            )

                nice_bot_desc = f"[{SCORE_INITIALS[bot_score]}{bot_id}] {bot_desc}"
                bot_tree = Tree(self._colored_description(nice_bot_desc, bot_score))
                for reason in reasons:
                    bot_tree.add(self._colored_description(reason, bot_score))
                view_tree.add(bot_tree)

            if max_bottlenecks > 0 and bot_count > max_bottlenecks:
                remaining_count = bot_count - max_bottlenecks
                view_tree.add(f"({remaining_count} more)")

            bot_table.add_row(view_tree)

        return bot_table, total_bot_count, total_reason_count

    def _create_characteristics_table(
        self,
        characteristics: Characteristics,
        compact: bool,
        job_time: float,
    ):
        char_table = Table(box=None, show_header=False)
        char_table.add_column(style="cyan")
        char_table.add_column()

        if compact:
            char_table.add_row('Runtime', f"{job_time:.2f} s")
        else:
            char_table.add_row('Runtime', f"{job_time:.2f} seconds")

        # Add each key-value pair to the table as a row
        for char in characteristics.values():
            if compact or char.detail_list is None or len(char.detail_list) == 0:
                if compact:
                    char_table.add_row(char.description, char.compact_desc)
                else:
                    char_table.add_row(char.description, char.value_fmt)
            else:
                detail_tree = Tree(char.value_fmt)
                for detail in char.detail_list:
                    detail_tree.add(detail)
                char_table.add_row(char.description, detail_tree)

        return char_table

    def _create_debug_table(metric: Metric, output: OutputType):
        main_view_count = output.counts.main_view_count
        raw_total_count = output.counts.raw_count

        debug_table = Table(box=None, show_header=False)
        debug_table.add_column(style="cyan")
        debug_table.add_column()

        retained_tree = Tree(f"raw: {raw_total_count} records (100%)")
        main_view_tree = retained_tree.add(
            f"aggregated view: {main_view_count} ({main_view_count/raw_total_count*100:.2f}% 100%)"
        )

        for metric in output.counts.perspective_count_tree:
            metric_tree = Tree(
                (
                    f"{metric}: "
                    f"{output.counts.avg_perspective_count[metric]:.2f}±{output.counts.avg_perspective_count_std[metric]:.2f} "
                    f"({output.counts.avg_perspective_count[metric]/raw_total_count*100:.2f}% "
                    f"{output.counts.avg_perspective_count[metric]/main_view_count*100:.2f}%)"
                    " -S> "
                    f"{output.counts.avg_perspective_critical_count[metric]:.2f}±{output.counts.avg_perspective_critical_count_std[metric]:.2f} "
                    f"({output.counts.avg_perspective_critical_count[metric]/raw_total_count*100:.2f}% "
                    f"{output.counts.avg_perspective_critical_count[metric]/main_view_count*100:.2f}%)"
                )
            )
            for count_key in output.counts.perspective_count_tree[metric]:
                view_count = output.counts.perspective_count_tree[metric][count_key]
                view_critical_count = output.counts.perspective_critical_count_tree[
                    metric
                ][count_key]
                metric_tree.add(
                    (
                        f"{count_key}: "
                        f"{view_count} ({view_count/raw_total_count*100:.2f}% {view_count/main_view_count*100:.2f}%)"
                        " -S> "
                        f"{view_critical_count} ({view_critical_count/raw_total_count*100:.2f}% {view_critical_count/main_view_count*100:.2f}%)"
                    )
                )
            main_view_tree.add(metric_tree)

        debug_table.add_row('Retained Records', retained_tree)

        count_table = Table(box=None, show_header=True)
        count_table.add_column()
        count_table.add_column('Count')
        count_table.add_column('Processed Record Count')
        count_table.add_row(
            'Perspectives',
            f"{output.counts.num_perspectives}",
            f"{output.counts.slope_filtered_records[metric]}",
        )
        count_table.add_row(
            'Bottlenecks',
            f"{output.counts.num_bottlenecks[metric]}",
            f"{output.counts.evaluated_records[metric]}",
        )
        count_table.add_row(
            'Rules',
            f"{output.counts.num_rules}",
            f"{output.counts.reasoned_records[metric]}",
        )

        debug_table.add_row('Counts', count_table)

        tput_table = Table(box=None, show_header=True)
        tput_table.add_column()
        tput_table.add_column('Throughput')
        tput_table.add_column('Record Throughput')
        tput_table.add_row(
            'Perspectives',
            f"{output.throughputs.perspectives[metric]:.2f} perspectives/sec",
            f"{output.throughputs.slope_filtered_records[metric]:.2f} records/sec",
        )
        tput_table.add_row(
            'Bottlenecks',
            f"{output.throughputs.bottlenecks[metric]:.2f} bottlenecks/sec",
            f"{output.throughputs.evaluated_records[metric]:.2f} records/sec",
        )
        tput_table.add_row(
            'Rules',
            f"{output.throughputs.rules[metric]:.2f} rules/sec",
            f"{output.throughputs.reasoned_records[metric]:.2f} records/sec",
        )

        debug_table.add_row('Throughputs', tput_table)

        tot_bottlenecks = 0
        for metric in output.counts.num_bottlenecks:
            num_bottlenecks = output.counts.num_bottlenecks[metric]
            tot_bottlenecks = tot_bottlenecks + num_bottlenecks
        severity_tree = Tree(f"total: {tot_bottlenecks} bottlenecks (100%)")
        for metric in output.counts.num_bottlenecks:
            num_bottlenecks = output.counts.num_bottlenecks[metric]
            severity_metric_tree = severity_tree.add(
                (
                    f"{metric}: "
                    f"{num_bottlenecks} "
                    f"({num_bottlenecks/tot_bottlenecks*100:.2f}% 100%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"critical: "
                    f"{output.severities.critical_count[metric]} "
                    f"({output.severities.critical_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.critical_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"very high: "
                    f"{output.severities.very_high_count[metric]} "
                    f"({output.severities.very_high_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.very_high_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"high: "
                    f"{output.severities.high_count[metric]} "
                    f"({output.severities.high_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.high_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"medium: "
                    f"{output.severities.medium_count[metric]} "
                    f"({output.severities.medium_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.medium_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"low: "
                    f"{output.severities.low_count[metric]} "
                    f"({output.severities.low_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.low_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"very low: "
                    f"{output.severities.very_low_count[metric]} "
                    f"({output.severities.very_low_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.very_low_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"trivial: "
                    f"{output.severities.trivial_count[metric]} "
                    f"({output.severities.trivial_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.trivial_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )
            severity_metric_tree.add(
                (
                    f"none: "
                    f"{output.severities.none_count[metric]} "
                    f"({output.severities.none_count[metric]/tot_bottlenecks*100:.2f}% "
                    f"{output.severities.none_count[metric]/num_bottlenecks*100:.2f}%)"
                )
            )

        debug_table.add_row('Severities', severity_tree)

        return debug_table

    @staticmethod
    def _colored_description(description: str, score: str = None):
        if score is None:
            return description
        if score == SCORE_NAMES[0]:
            return description
        elif score == SCORE_NAMES[1]:
            return f"[light_cyan3]{description}"
        elif score == SCORE_NAMES[2]:
            return f"[chartreuse2]{description}"
        elif score == SCORE_NAMES[3]:
            return f"[yellow4]{description}"
        elif score == SCORE_NAMES[4]:
            return f"[yellow3]{description}"
        elif score == SCORE_NAMES[5]:
            return f"[orange3]{description}"
        elif score == SCORE_NAMES[6]:
            return f"[dark_orange3]{description}"
        elif score == SCORE_NAMES[7]:
            return f"[red3]{description}"


class CSVOutput(Output):
    def handle_result(self, metrics: List[Metric], result: AnalyzerResultType):
        # TODO(izzet): make it work with multiple metrics
        metric = metrics[0]
        self._create_output_df(
            bottleneck_dir=result.bottleneck_dir, metric=metric
        ).to_csv(f"{self.output_dir}/result.csv", encoding='utf8')

        # timings_df, timings_raw_df = self._create_timings_df()
        # timings_df.sort_values(['type', 'key']).to_csv(f"{run_dir}/timings.csv", encoding='utf8')
        # timings_df.sort_values(['time_start']).to_csv(f"{run_dir}/timings_ordered.csv", encoding='utf8')
        # timings_raw_df.to_csv(f"{run_dir}/timings_raw.csv", encoding='utf8')

        view_name = 'time_range'
        view_key = tuple([view_name])
        if view_key in self.view_results[metric]:
            view_result = self.view_results[metric][view_key]
            view_result.view[
                [
                    'count',
                    'count_per',
                    'time',
                    'time_per',
                    f"{metric}_slope",
                ]
            ].compute().to_csv(
                f"{self.output_dir}/slope_{view_name}.csv", encoding='utf8'
            )


class SQLiteOutput(Output):
    def __init__(
        self,
        compact: bool = False,
        group_behavior: bool = False,
        name: str = "",
        root_only: bool = False,
        run_db_path: str = "",
        view_names: List[str] = [],
    ):
        super().__init__(compact, group_behavior, name, root_only, view_names)
        self.run_db_path = run_db_path

    def handle_result(self, metrics: List[Metric], result: AnalyzerResultType):
        con = sqlite3.connect(f"{self.output_dir}/result.db")

        # TODO(izzet): make it work with multiple metrics
        metric = metrics[0]

        output_df = self._create_output_df(
            bottleneck_dir=result.bottleneck_dir, metric=metric
        ).reset_index()
        output_df['key'] = output_df['type'] + '_' + output_df['value']
        output_df = output_df.drop(columns=['type', 'value'])
        output_sql_df = output_df.set_index('key').T

        if self.run_db_path is None:
            output_sql_df.to_sql('run', con=con, index_label='key')
        else:
            run_con = sqlite3.connect(self.run_db_path)
            output_sql_df.to_sql(
                'run', con=run_con, index_label='key', if_exists='append'
            )
            run_con.close()

        # timings_df, timings_raw_df = self._create_timings_df()
        # timings_df.sort_values(['type', 'key']).to_sql('timings', con=con)
        # timings_df.sort_values(['time_start']).to_sql('timings_ordered', con=con)
        # timings_raw_df.to_sql('timings_raw', con=con)

        view_name = 'time_range'
        view_key = tuple([view_name])
        if view_key in self.view_results[metric]:
            view_result = self.view_results[metric][view_key]
            view_result.view[
                [
                    'count',
                    'count_per',
                    'time',
                    'time_per',
                    f"{metric}_slope",
                ]
            ].compute().to_sql(f"slope_{view_name}", con=con)

        con.close()
