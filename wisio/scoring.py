import dask.dataframe as dd
from typing import Dict, List

from .analysis import (
    IS_NORMALIZED,
    set_bound_columns,
    set_metric_percentages,
    set_metric_scores,
)
from .types import (
    Metric,
    ScoringPerViewPerMetric,
    ScoringResult,
    ViewKey,
    ViewResult,
    ViewResultsPerViewPerMetric,
)

SCORING_ORDER = dict(
    app_name=('app_name', 'time_range', 'file_name'),
    file_dir=('file_dir', 'proc_name', 'time_range'),
    file_name=('file_name', 'proc_name', 'time_range'),
    file_pattern=('file_pattern', 'proc_name', 'time_range'),
    node_name=('node_name', 'time_range', 'file_name'),
    proc_name=('proc_name', 'time_range', 'file_name'),
    rank=('rank', 'time_range', 'file_name'),
    time_range=('time_range', 'proc_name', 'file_name'),
)


class ViewEvaluator(object):

    def evaluate_views(
        self,
        view_results: ViewResultsPerViewPerMetric,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        threshold: float,
    ) -> ScoringPerViewPerMetric:
        # Keep evaluated views
        evaluated_views = {}
        # Run through views for each metric
        for metric in metrics:
            evaluated_views[metric] = {}
            for view_key, view_result in view_results[metric].items():
                # Generate evaluated views
                evaluated_views[metric][view_key] = self._generate_evaluated_views(
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    view_key=view_key,
                    view_result=view_result,
                    threshold=threshold,
                )
        # Return bottleneck views
        return evaluated_views

    def _generate_evaluated_views(
        self,
        view_key: ViewKey,
        view_result: ViewResult,
        metric: str,
        metric_boundary: dd.core.Scalar,
        threshold: float,
    ):
        # Get view type
        view_type = view_key[-1]

        col = metric if IS_NORMALIZED[metric] else f"{metric}_pero"

        evaluated_groups = view_result.group_view \
            .map_partitions(set_bound_columns) \
            .map_partitions(set_metric_percentages, metric=metric, metric_boundary=metric_boundary) \
            .map_partitions(set_metric_scores, view_type=view_type, metric=metric, value_col=col, metric_boundary=metric_boundary)

        potential_bottlenecks = evaluated_groups \
            .query(f"{metric}_threshold >= {threshold}") \
            .persist()

        return ScoringResult(
            attached_records=view_result.view,
            evaluated_groups=evaluated_groups,
            potential_bottlenecks=potential_bottlenecks,
        )
