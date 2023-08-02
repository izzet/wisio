import math
import matplotlib.pyplot as plt
import numpy as np
import venn
from matplotlib import ticker
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
from typing import List, Set, Tuple
from .types import (
    COL_PROC_NAME,
    COL_TIME_RANGE,
    Metric,
    ResultBottlenecks,
    ResultMainView,
    ResultViews,
    ViewKey,
    _view_name,
)

DELTA_BINS = [
    0,
    0.001,
    0.01,
    0.1,
    0.25,
    0.5,
    0.75,
    0.9
]


class AnalysisResultPlot(object):

    def __init__(
        self,
        main_view: ResultMainView,
        views: ResultViews,
        bottlenecks: ResultBottlenecks,
    ):
        self.main_view = main_view
        self.bottlenecks = bottlenecks
        self.views = views
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

        dur_mlv = self.bottlenecks['duration'][(COL_PROC_NAME,)]['mid_level_view']
        dur_col = next(col for col in dur_mlv.columns if 'duration' in col)
        dur_data = dur_mlv.compute()

        fig, ax = plt.subplots(figsize=figsize)
        
        bar_h = 1

        for y, proc_name in enumerate(proc_names):
            try:
                ax.broken_barh(dur_data.loc[proc_name][dur_col].to_dict().items(), (y, bar_h), facecolors='C0', alpha=0.8)
            except KeyError:
                continue

        for m, metric in enumerate(metrics):
            data = self.bottlenecks[metric][(COL_PROC_NAME,)]['mid_level_view'].compute()
            for y, proc_name in enumerate(proc_names):
                try:
                    for time_range, threshold in data.loc[proc_name][f"{metric}_th"].to_dict().items():
                        # print(proc_name, y, time_range, threshold)
                        if threshold >= thresholds[m]:
                            ax.scatter(
                                x=time_range,
                                y=y + (bar_h / 2),
                                s=marker_size,
                                c=colors[m],
                                marker=markers[m],
                                alpha=0.6,
                            )
                except KeyError:
                    continue

        ax.set_ylim(0, len(proc_names))  # len(bot_dur_proc_ml.index.get_level_values(0).unique()))
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

        return fig, ax

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
        mappable = plt.cm.ScalarMappable(norm=norm, cmap=cmap)  # Create the mappable
        mappable.set_array(DELTA_BINS)  # Set the data for the colorbar

        # Position the colorbar within the figure
        # Left, bottom, width, and height are in fractions of the figure size (0 to 1)
        colorbar_ax = fig.add_axes([0.68, 0.1, 0.2, 0.03])  # You can adjust these values
        colorbar = plt.colorbar(mappable, cax=colorbar_ax, orientation='horizontal')
        colorbar.set_ticklabels(['critical', 'medium', 'trivial'])
        colorbar.ax.tick_params(labelsize=12, pad=2)  # Adjust the font size as needed

        # Add a label to the colorbar
        # colorbar.set_label('Colorbar Label')
        colorbar_label = 'Bottleneck Severity'
        colorbar.ax.xaxis.set_label_position('top')  # Position the label at the top of the colorbar
        colorbar.ax.set_xlabel(colorbar_label, fontsize=12, labelpad=4)  # Adjust font size and labelpad as needed

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
        hlv = self.bottlenecks[metric][(COL_TIME_RANGE,)]['high_level_view']
        metric_col = next(col for col in hlv.columns if metric in col)
        data = hlv.compute()
        ax_line = data[metric_col].plot(ax=ax, color=color, figsize=figsize, title=title, alpha=0.8)
        if yaxis_formatter is not None:
            ax_line.yaxis.set_major_formatter(yaxis_formatter)
        if yaxis_label is not None:
            ax_line.yaxis.set_label(yaxis_label)
        filtered_data = data.query(f"{metric}_th >= {threshold}").reset_index()
        if sample_count > 0:
            filtered_data = filtered_data.sort_values(f"{metric}_th", ascending=False).head(sample_count)
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

    def _metric_relations(self, view_key: ViewKey, metrics: List[Metric], labels: List[str]):
        sets = [set(self.views[metric][view_key]['id'].unique().compute()) for metric in metrics]
        labels = self._venn_labels(sets, labels, metrics)
        fig, ax = venn.venn3(labels, names=metrics, figsize=(5, 5))
        # ax.get_legend().remove()
        # fig.tight_layout()
        return fig, ax

    def _view_relations(self, metric: Metric, view_keys: List[ViewKey], labels: List[str]):
        names = [_view_name(view_key).replace('_', '\_') for view_key in view_keys]
        sets = [set(self.views[metric][view_key]['id'].unique().compute()) for view_key in view_keys]
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
            fixed_labels[pos] = f"{fixed_labels[pos]}\n" + rf"$\bf{{{bold_label}}}$"
        return fixed_labels


class AnalysisResult(object):

    def __init__(
        self,
        main_view: ResultMainView,
        views: ResultViews,
        bottlenecks: ResultBottlenecks,
    ):
        self.main_view = main_view
        self.views = views
        self.bottlenecks = bottlenecks

        self.plot = AnalysisResultPlot(
            main_view=main_view,
            bottlenecks=bottlenecks,
            views=views,
        )
