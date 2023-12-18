import math
import matplotlib.pyplot as plt
import numpy as np
import venn
from distributed import get_client
from matplotlib import ticker
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from typing import List, Set, Tuple

from .analysis import DELTA_BINS
from .constants import (
    COL_PROC_NAME,
    COL_TIME_RANGE,
    EVENT_ATT_REASONS,
    EVENT_COMP_PERS,
    EVENT_DET_BOTT,
)
from .types import (
    BottlenecksPerViewPerMetric,
    Characteristics,
    MainView,
    Metric,
    RawStats,
    RuleResultsPerViewPerMetricPerRule,
    ViewKey,
    ViewResultsPerViewPerMetric,
    view_name,
)


class AnalyzerResultOutput(object):

    def __init__(
        self,
        bottlenecks: RuleResultsPerViewPerMetricPerRule,
        characteristics: Characteristics,
        debug: bool,
        evaluated_views: BottlenecksPerViewPerMetric,
        main_view: MainView,
        raw_stats: RawStats,
        view_results: ViewResultsPerViewPerMetric,
    ) -> None:
        self.bottlenecks = bottlenecks
        self.characteristics = characteristics
        self.debug = debug
        self.evaluated_views = evaluated_views
        self.main_view = main_view
        self.raw_stats = raw_stats
        self.view_results = view_results

    def console(self, max_bottlenecks_per_view_type=3):
        raw_job_time = self.raw_stats.job_time.compute()

        char_table = Table(box=None, show_header=False)
        char_table.add_column(style="cyan")
        char_table.add_column()

        char_table.add_row('Job Time', f"{raw_job_time:.2f} seconds")

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
        severity_dict = {}
        for rule in tree_dict:
            rule_tree = Tree(rule)
            total_count = 0
            for view_key in tree_dict[rule]:
                results = tree_dict[rule][view_key]
                view_record_count = len(results)
                total_count = total_count + view_record_count
                view_tree = Tree(
                    f"{view_name(view_key, ' > ')} ({view_record_count} bottlenecks)")
                if view_record_count == 0:
                    continue
                for result in results:
                    severity = result.extra_data[f"{metric}_score"]
                    severity_dict[severity] = severity_dict[severity] if severity in severity_dict else 0
                    severity_dict[severity] = severity_dict[severity] + 1
                    if result.reasons is None or len(result.reasons) == 0:
                        view_tree.add(result.description)
                    else:
                        bott_tree = Tree(result.description)
                        for reason in result.reasons:
                            bott_tree.add(reason.description)
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

        if self.debug:
            raw_total_count = self.raw_stats.total_count.compute()

            elapsed_times = {}
            for (_, event) in get_client().get_events('elapsed_times'):
                elapsed_times[event['key']] = event['elapsed_time']

            debug_table = Table(box=None, show_header=False)
            debug_table.add_column(style="cyan")
            debug_table.add_column()

            reduction_tree = Tree(f"raw - {raw_total_count} (100%)")
            view_count = 0
            total_record_count = 0
            for metric in self.view_results:
                metric_tree = Tree(metric)
                counts = []
                for view_key, view_result in self.view_results[metric].items():
                    view_count = view_count + 1
                    view_record_count = len(view_result.view)
                    view_record_per = view_record_count/raw_total_count
                    metric_tree.add(
                        f"{view_name(view_key, ' > ')} - {view_record_count} ({view_record_per*100:.2f}%)")
                    counts.append(view_record_count)
                avg_count = int(np.average(counts))
                max_count = int(np.max(counts))
                min_count = int(np.min(counts))
                sum_count = int(np.sum(counts))
                total_record_count = total_record_count + sum_count
                metric_tree.label = (
                    f"{metric}"
                    f" - avg. {avg_count} ({avg_count/raw_total_count*100:.2f}%)"
                    f" - max. {max_count} ({max_count/raw_total_count*100:.2f}%)"
                    f" - min. {min_count} ({min_count/raw_total_count*100:.2f}%)"
                    f" - sum. {sum_count} ({sum_count/raw_total_count*100:.2f}%)"
                )
                reduction_tree.add(metric_tree)
            debug_table.add_row('Data Reduction', reduction_tree)

            bott_record_count = 0
            for metric in self.evaluated_views:
                for view_key in self.evaluated_views[metric]:
                    bott_record_count = bott_record_count + \
                        len(self.evaluated_views[metric][view_key].bottlenecks)

            rule_count = len(self.bottlenecks)

            count_table = Table(box=None, show_header=True)
            count_table.add_column()
            count_table.add_column('Count')
            count_table.add_column('Processed Record Count')

            count_table.add_row(
                'Perspectives', f"{view_count}", f"{total_record_count}")
            count_table.add_row(
                'Bottlenecks', f"{bott_count}", f"{bott_record_count}")
            count_table.add_row(
                'Rules', f"{rule_count}", f"{bott_record_count*rule_count}")

            debug_table.add_row('Counts', count_table)

            thruput_table = Table(box=None, show_header=True)
            thruput_table.add_column()
            thruput_table.add_column('Throughput')
            thruput_table.add_column('Record Throughput')

            thruput_table.add_row(
                'Perspectives',
                f"{view_count/elapsed_times[EVENT_COMP_PERS]:.2f} perspectives/sec",
                f"{total_record_count/elapsed_times[EVENT_COMP_PERS]:.2f} records/sec",
            )
            thruput_table.add_row(
                'Bottlenecks',
                f"{bott_count/elapsed_times[EVENT_DET_BOTT]:.2f} bottlenecks/sec",
                f"{bott_record_count/elapsed_times[EVENT_DET_BOTT]:.2f} records/sec",
            )
            thruput_table.add_row(
                'Rules',
                f"{rule_count/(elapsed_times[EVENT_ATT_REASONS]):.2f} rules/sec",
                f"{(bott_record_count*rule_count)/(elapsed_times[EVENT_ATT_REASONS]):.2f} records/sec",
            )

            debug_table.add_row('Throughputs', thruput_table)

            severity_tree = Tree(f"total - {bott_count} bottlenecks (100%)")
            severity_total = 0
            for severity in severity_dict:
                severity_tree.add((
                    f"{severity} - "
                    f"{severity_dict[severity]} bottlenecks "
                    f"({severity_dict[severity]/bott_count*100:.2f}%)"
                ))
                severity_total = severity_total + severity_dict[severity]
            severity_tree.add((
                f"rest - "
                f"{bott_count - severity_total} bottlenecks "
                f"({(bott_count - severity_total)/bott_count*100:.2f}%)"
            ))

            debug_table.add_row('Severities', severity_tree)

            debug_panel = Panel(debug_table, title='Debug')

            Console().print(char_panel, bott_panel, debug_panel)
        else:
            Console().print(char_panel, bott_panel)


class AnalysisResultPlots(object):

    def __init__(
        self,
        main_view: MainView,
        view_results: ViewResultsPerViewPerMetric,
        bottlenecks: BottlenecksPerViewPerMetric,
    ):
        self.main_view = main_view
        self.view_results = view_results
        self.bottlenecks = bottlenecks
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
        y_col = 'index_cs_per_rev'
        for i, view_key in enumerate(view_keys):
            view_result = self.view_results[metric][view_key]
            group_view = view_result.group_view.compute()
            slope_cond = group_view[f"{metric}_slope"] < slope_threshold
            group_view.loc[slope_cond, f"{x_col}_line"] = group_view[x_col]
            line = group_view[f"{x_col}_line"].to_numpy()
            x = group_view[x_col].to_numpy()
            y = group_view[y_col].to_numpy()
            last_non_nan_index = np.where(~np.isnan(line))[0][-1]
            dotted = np.copy(line)
            if np.all(np.isnan(line[last_non_nan_index + 1:])):
                # complete dotted line if all values after last non-nan are nan
                dotted[last_non_nan_index + 1:] = x[last_non_nan_index + 1:]
            mask = np.isfinite(dotted)
            color = f"C{i}" if color is None else color
            ax.plot(dotted[mask], y[mask], c=color, ls='--')
            ax.plot(line, y, c=color)
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
        return ax, group_view

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
        bottlenecks: RuleResultsPerViewPerMetricPerRule,
        characteristics: Characteristics,
        debug: bool,
        evaluated_views: BottlenecksPerViewPerMetric,
        main_view: MainView,
        metric_boundaries,
        raw_stats: RawStats,
        view_results: ViewResultsPerViewPerMetric,
    ):
        self.bottlenecks = bottlenecks
        self.characteristics = characteristics
        self.debug = debug
        self.evaluated_views = evaluated_views
        self.main_view = main_view
        self.metric_boundaries = metric_boundaries
        self.raw_stats = raw_stats
        self.view_results = view_results

        self.output = AnalyzerResultOutput(
            characteristics=characteristics,
            bottlenecks=bottlenecks,
            debug=debug,
            evaluated_views=evaluated_views,
            main_view=main_view,
            raw_stats=raw_stats,
            view_results=view_results,
        )

        self.plots = AnalysisResultPlots(
            bottlenecks=evaluated_views,
            main_view=main_view,
            view_results=view_results,
        )
