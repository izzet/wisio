import dask.dataframe as dd
from typing import Dict, List
from .analysis import IS_NORMALIZED, set_bound_columns, set_metric_percentages, set_metric_scores
from .types import (
    BottleneckResult,
    BottlenecksPerViewPerMetric,
    Metric,
    ViewKey,
    ViewResult,
    ViewResultsPerViewPerMetric,
)


BOTTLENECK_ORDER = dict(
    app_name=('app_name', 'time_range', 'file_name'),
    file_dir=('file_dir', 'proc_name', 'time_range'),
    file_name=('file_name', 'proc_name', 'time_range'),
    file_pattern=('file_pattern', 'proc_name', 'time_range'),
    node_name=('node_name', 'time_range', 'file_name'),
    proc_name=('proc_name', 'time_range', 'file_name'),
    rank=('rank', 'time_range', 'file_name'),
    time_range=('time_range', 'proc_name', 'file_name'),
)


class BottleneckDetector(object):

    def evaluate_views(
        self,
        view_results: ViewResultsPerViewPerMetric,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
    ) -> BottlenecksPerViewPerMetric:
        # Keep bottleneck views
        bottlenecks = {}
        # Run through views for each metric
        for metric in metrics:
            bottlenecks[metric] = {}
            for view_key, view_result in view_results[metric].items():
                # Generate bottleneck views
                bottlenecks[metric][view_key] = self._generate_bottlenecks_views(
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    view_key=view_key,
                    view_result=view_result,
                )
        # Return bottleneck views
        return bottlenecks

    def _generate_bottlenecks_views(
        self,
        view_key: ViewKey,
        view_result: ViewResult,
        metric: str,
        metric_boundary: dd.core.Scalar,
    ):
        # Get view type
        view_type = view_key[-1]

        col = metric if IS_NORMALIZED[metric] else f"{metric}_pero"

        bottlenecks = view_result.group_view \
            .map_partitions(set_bound_columns) \
            .map_partitions(set_metric_percentages, metric=metric, metric_boundary=metric_boundary) \
            .map_partitions(set_metric_scores, view_type=view_type, metric=metric, value_col=col, metric_boundary=metric_boundary) \
            .persist()

        return BottleneckResult(
            bottlenecks=bottlenecks,
            details=view_result.view,
        )
