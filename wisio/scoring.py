import dask.dataframe as dd
from typing import Dict, List

from .analysis import set_metric_scores
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
        is_slope_based: bool,
    ) -> ScoringPerViewPerMetric:
        # Keep evaluated views
        evaluated_views = {}
        # Run through views for each metric
        for metric in metrics:
            evaluated_views[metric] = {}
            for view_key, view_result in view_results[metric].items():
                # Generate evaluated views
                evaluated_views[metric][view_key] = self._generate_evaluated_views(
                    is_slope_based=is_slope_based,
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    view_key=view_key,
                    view_result=view_result,
                )
        # Return bottleneck views
        return evaluated_views

    def _generate_evaluated_views(
        self,
        view_key: ViewKey,
        view_result: ViewResult,
        metric: str,
        metric_boundary: dd.core.Scalar,
        is_slope_based: bool,
    ):
        # Get view type
        view_type = view_key[-1]

        records_index = view_result.records.index.persist()

        scored_view = (
            view_result.critical_view.map_partitions(
                set_metric_scores,
                view_type=view_type,
                metric=metric,
                metric_boundary=metric_boundary,
                is_slope_based=is_slope_based,
            )
            .sort_values(f"{metric}_slope", ascending=True)
            .persist()
        )

        return ScoringResult(
            critical_view=view_result.critical_view,
            records_index=records_index,
            scored_view=scored_view,
        )
