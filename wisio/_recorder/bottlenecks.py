import dask.dataframe as dd
import pandas as pd
from dask import compute, delayed
from dask.distributed import get_client
from logging import Logger
from typing import Dict
from ..bottlenecks import BottleneckDetector
from ..utils.dask_agg import unique_flatten
from ..utils.logger import ElapsedTimeLogger
from .constants import VIEW_TYPES


BOTTLENECK_ORDER = dict(
    file_id=('file_name', 'proc_name', 'trange'),
    proc_id=('proc_name', 'trange', 'file_name'),
    trange=('trange', 'proc_name', 'file_name'),
)
BOTTLENECK_TYPE = dict(
    file_id='file_name',
    proc_id='proc_name',
    trange='trange'
)


def _calculate_llc(level_row: pd.Series):
    llc = dict(level_row)
    for view_type in VIEW_TYPES:
        if view_type in llc:
            llc.pop(view_type)
        bottleneck_type = BOTTLENECK_TYPE[view_type]
        if bottleneck_type in llc:
            llc.pop(bottleneck_type)
    return llc


@delayed
def _process_bottleneck_view(
    view_key: tuple,
    ll_view: pd.DataFrame,
    ml_view: pd.DataFrame,
    hl_view: pd.DataFrame,
):
    # Get view type
    view_type = view_key[-1]
    # Get ordered bottleneck columns
    _, ml_col, ll_col = BOTTLENECK_ORDER[view_type]
    # Init bottlenecks
    bottlenecks = {}
    # Loop through index tuples
    ids_tuple = ll_view.index
    for hl_id, ml_id, ll_id in ids_tuple:
        hl_row = hl_view.loc[hl_id]
        ml_row = ml_view.loc[(hl_id, ml_id)]
        ll_row = ll_view.loc[(hl_id, ml_id, ll_id)]
        if hl_id not in bottlenecks:
            bottlenecks[hl_id] = {}
            bottlenecks[hl_id]['llc'] = _calculate_llc(hl_row)
            bottlenecks[hl_id][ml_col] = {}
        if ml_id not in bottlenecks[hl_id][ml_col]:
            bottlenecks[hl_id][ml_col][ml_id] = {}
            bottlenecks[hl_id][ml_col][ml_id]['llc'] = _calculate_llc(ml_row)
            bottlenecks[hl_id][ml_col][ml_id][ll_col] = {}
        if ll_id not in bottlenecks[hl_id][ml_col][ml_id][ll_col]:
            bottlenecks[hl_id][ml_col][ml_id][ll_col][ll_id] = {}
            bottlenecks[hl_id][ml_col][ml_id][ll_col][ll_id]['llc'] = _calculate_llc(ll_row)
    # Return view key & bottlenecks
    return view_key, bottlenecks


class RecorderBottleneckDetector(BottleneckDetector):

    def __init__(self, logger: Logger, log_dir: str):
        super().__init__(logger, log_dir)

    def detect_bottlenecks(self, views: Dict[tuple, dd.DataFrame], view_types: list) -> Dict[tuple, object]:
        # Keep bottleneck views
        bottleneck_views = {}
        # Run through views
        for view_key, view_dict in views.items():
            # Generate bottleneck views
            bottleneck_views[view_key] = self._generate_bottlenecks_views(
                view_key=view_key,
                view_dict=view_dict,
                view_types=view_types,
            )
        # Generate bottlenecks
        bottlenecks = self._process_bottleneck_views(bottleneck_views=bottleneck_views)
        # Return bottleneck views
        return bottlenecks

    def _process_bottleneck_views(self, bottleneck_views: Dict[tuple, pd.DataFrame]):
        # Init bottlenecks
        bottlenecks = {}
        bottlenecks_delayed = []
        # Run through bottleneck views
        for view_key, view_dict in bottleneck_views.items():
            bottlenecks_delayed.append(_process_bottleneck_view(
                view_key=view_key,
                ll_view=view_dict['low_level_view'],
                ml_view=view_dict['mid_level_view'],
                hl_view=view_dict['high_level_view']
            ))
        # Compute all bottlenecks
        with ElapsedTimeLogger(logger=self.logger, message='Compute bottlenecks'):
            futures = compute(*bottlenecks_delayed, sync=False)
            results = get_client().gather(list(futures))
            for view_key, result in results:
                bottlenecks[view_key] = result
        # Return all bottlenecks
        return bottlenecks

    def _generate_bottlenecks_views(
        self,
        view_key: tuple,
        view_dict: Dict[str, dd.DataFrame],
        view_types: list
    ):
        # Get view type
        view_type = view_key[-1]
        bottleneck_type = BOTTLENECK_TYPE[view_type]

        # Get parent view
        bottleneck_view = view_dict['bottleneck_view']

        # Create lower level view
        low_level_view = bottleneck_view \
            .groupby(list(BOTTLENECK_ORDER[view_type])) \
            .first() \
            .drop(columns=['acc_pat', 'io_cat'], errors='ignore')

        # Non-proc agg columns
        non_proc_agg_dict = self._get_agg_dict(view_types=view_types, view_columns=low_level_view.columns, is_proc=False)
        proc_agg_dict = self._get_agg_dict(view_types=view_types, view_columns=low_level_view.columns, is_proc=True)

        if bottleneck_type is not 'proc_name':

            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([bottleneck_type, 'proc_name']) \
                .agg(non_proc_agg_dict)

            high_level_view = mid_level_view \
                .reset_index() \
                .groupby([bottleneck_type]) \
                .agg(proc_agg_dict)

        else:

            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([bottleneck_type, 'trange']) \
                .agg(non_proc_agg_dict)

            high_level_view = mid_level_view \
                .reset_index() \
                .groupby([bottleneck_type]) \
                .agg(non_proc_agg_dict)

        return dict(
            low_level_view=low_level_view.persist(),
            mid_level_view=mid_level_view.persist(),
            high_level_view=high_level_view.persist()
        )

    def _get_agg_dict(self, view_types: list, view_columns: list, is_proc=False):
        if is_proc:
            agg_dict = {col: max if any(x in col for x in 'duration time'.split()) else sum for col in view_columns}
        else:
            agg_dict = {col: sum for col in view_columns}
        agg_dict['func_id'] = unique_flatten()
        agg_dict['size_min'] = min
        agg_dict['size_max'] = max
        for view_type in view_types:
            if view_type in agg_dict:
                agg_dict.pop(view_type)
            bottleneck_type = BOTTLENECK_TYPE[view_type]
            if bottleneck_type in agg_dict:
                agg_dict.pop(bottleneck_type)
        return agg_dict
