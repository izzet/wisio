import dask.dataframe as dd
import pandas as pd
from dask import compute, delayed
from logging import Logger
from typing import Dict
from ..base import ViewKey
from ..bottlenecks import BottleneckDetector
from ..utils.logger import ElapsedTimeLogger
from .analysis import (
    DELTA_BINS,
    PROC_COL,
    TRANGE_COL,
    set_metric_percentages,
    set_metric_scores,
)
from .constants import (
    LOGICAL_VIEW_TYPES,
    VIEW_TYPES
)


BOTTLENECK_ORDER = dict(
    app_name=('app_name', 'trange', 'file_name'),
    file_dir=('file_dir', 'proc_name', 'trange'),
    file_name=('file_name', 'proc_name', 'trange'),
    file_regex=('file_regex', 'proc_name', 'trange'),
    node_name=('node_name', 'trange', 'file_name'),
    proc_name=('proc_name', 'trange', 'file_name'),
    rank=('rank', 'trange', 'file_name'),
    trange=('trange', 'proc_name', 'file_name'),
)


@delayed
def _process_bottleneck_view(
    view_key: ViewKey,
    threshold: float,
    low_level_view: pd.DataFrame,
    mid_level_view: pd.DataFrame,
    high_level_view: pd.DataFrame,
):
    # Get view type
    view_type = view_key[-1]
    # Get ordered bottleneck columns
    _, ml_col, ll_col = BOTTLENECK_ORDER[view_type]
    # Init bottlenecks
    bottlenecks = {}
    # Loop through index tuples
    ids_tuple = low_level_view.index
    for hl_id, ml_id, ll_id in ids_tuple:
        hl_row = high_level_view.loc[hl_id]
        ml_row = mid_level_view.loc[(hl_id, ml_id)]
        ll_row = low_level_view.loc[(hl_id, ml_id, ll_id)]
        if hl_id not in bottlenecks:
            bottlenecks[hl_id] = {}
            bottlenecks[hl_id]['llc'] = dict(hl_row)
            bottlenecks[hl_id][ml_col] = {}
        if ml_id not in bottlenecks[hl_id][ml_col]:
            bottlenecks[hl_id][ml_col][ml_id] = {}
            bottlenecks[hl_id][ml_col][ml_id]['llc'] = dict(ml_row)
            bottlenecks[hl_id][ml_col][ml_id][ll_col] = {}
        if ll_id not in bottlenecks[hl_id][ml_col][ml_id][ll_col]:
            bottlenecks[hl_id][ml_col][ml_id][ll_col][ll_id] = {}
            bottlenecks[hl_id][ml_col][ml_id][ll_col][ll_id]['llc'] = dict(ll_row)
    # Return view key & bottlenecks
    return view_key, threshold, bottlenecks


class RecorderBottleneckDetector(BottleneckDetector):

    def __init__(self, logger: Logger):
        super().__init__(logger)

    def detect_bottlenecks(
        self,
        views: Dict[ViewKey, dd.DataFrame],
        max_io_time: dd.core.Scalar,
        metric='duration',
    ) -> Dict[ViewKey, dd.DataFrame]:
        # Keep bottleneck views
        bottlenecks = {}
        # Run through views
        for view_key, view in views.items():
            # Generate bottleneck views
            bottlenecks[view_key] = self._generate_bottlenecks_views(
                view_key=view_key,
                view=view,
                max_io_time=max_io_time,
                metric=metric,
            )
        # Return bottleneck views
        return bottlenecks

    def bottlenecks_to_json(self, bottlenecks: Dict[ViewKey, dd.DataFrame], metric='duration'):
        # Init bottlenecks
        bottleneck_tasks = []
        bottlenecks_dict = {}
        # Run through bottleneck views
        for view_key, view_dict in bottlenecks.items():
            # For given thresholds
            for threshold in DELTA_BINS[1:-1]:  # [0.001, 0.01, 0.1, 0.25, 0.5, 0.75]
                threshold_col = f"{metric}_th"
                low_level_view = view_dict['low_level_view']
                mid_level_view = view_dict['mid_level_view']
                high_level_view = view_dict['high_level_view']
                bottleneck_tasks.append(_process_bottleneck_view(
                    view_key=view_key,
                    threshold=threshold,
                    low_level_view=low_level_view,
                    mid_level_view=mid_level_view,
                    high_level_view=high_level_view.query(f"{threshold_col} >= @th", local_dict={'th': threshold})
                ))
        # Compute all bottlenecks
        with ElapsedTimeLogger(logger=self.logger, message='Compute bottlenecks'):
            bottleneck_results = compute(*bottleneck_tasks)
        # Create bottlenecks dict
        for view_key, threshold, result in bottleneck_results:
            bottlenecks_dict[view_key] = bottlenecks_dict[view_key] if view_key in bottlenecks_dict else {}
            bottlenecks_dict[view_key][f"{threshold:.3f}"] = result
        # Return all bottlenecks
        return bottlenecks_dict

    def _generate_bottlenecks_views(
        self,
        view_key: ViewKey,
        view: dd.DataFrame,
        max_io_time: dd.core.Scalar,
        metric: str,
    ):
        # Get view type
        view_type = view_key[-1]

        # Create lower level view
        low_level_view = view \
            .groupby(list(BOTTLENECK_ORDER[view_type])) \
            .first()

        # Non-proc agg columns
        non_proc_agg_dict = self._get_agg_dict(view_columns=low_level_view.columns, is_proc=False)
        proc_agg_dict = self._get_agg_dict(view_columns=low_level_view.columns, is_proc=True)

        # Create mid and high level views
        if view_type is not PROC_COL:
            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([view_type, PROC_COL]) \
                .agg(non_proc_agg_dict)

            high_level_view = mid_level_view \
                .reset_index() \
                .groupby([view_type]) \
                .agg(proc_agg_dict)
        else:
            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([view_type, TRANGE_COL]) \
                .agg(non_proc_agg_dict)

            high_level_view = mid_level_view \
                .reset_index() \
                .groupby([view_type]) \
                .agg(non_proc_agg_dict)

        low_level_view = low_level_view \
            .map_partitions(set_metric_percentages, metric=metric, max_io_time=max_io_time) \
            .map_partitions(set_metric_scores, metric=metric, col=f"{metric}_pero")

        mid_level_view = mid_level_view \
            .map_partitions(set_metric_percentages, metric=metric, max_io_time=max_io_time) \
            .map_partitions(set_metric_scores, metric=metric, col=f"{metric}_pero")

        high_level_view = high_level_view \
            .map_partitions(set_metric_percentages, metric=metric, max_io_time=max_io_time) \
            .map_partitions(set_metric_scores, metric=metric, col=f"{metric}_pero")

        return dict(
            low_level_view=low_level_view,
            mid_level_view=mid_level_view,
            high_level_view=high_level_view,
        )

    def _get_agg_dict(self, view_columns: list, is_proc=False):
        if is_proc:
            agg_dict = {col: max if any(x in col for x in 'duration time'.split()) else sum for col in view_columns}
        else:
            agg_dict = {col: sum for col in view_columns}
        agg_dict['size_min'] = min
        agg_dict['size_max'] = max
        for view_type in [*VIEW_TYPES, *LOGICAL_VIEW_TYPES]:
            if view_type in agg_dict:
                agg_dict.pop(view_type)
        return agg_dict
