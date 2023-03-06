import dask.dataframe as dd
import pandas as pd
from dask import compute, delayed
from dask.distributed import get_client
from typing import Any, Dict
from ..bottlenecks import BottleneckDetector
from ..utils.dask_agg import unique_flatten
from ..utils.logger import ElapsedTimeLogger
from .constants import VIEW_TYPES


BOTTLENECK_ORDER = dict(
    file_id=('file_id', 'proc_id', 'trange'),
    proc_id=('proc_id', 'trange', 'file_id'),
    trange=('trange', 'proc_id', 'file_id')
)
BOTTLENECK_TREE_NAME = dict(
    file_id='file_name',
    proc_id='proc_name',
    trange='trange'
)


def _get_level_value(level_row: pd.Series, level_col: str, level_id: Any):
    tree_name = BOTTLENECK_TREE_NAME[level_col]
    if tree_name in level_row.values:
        return tree_name, level_row[tree_name]
    return tree_name, level_id


def _calculate_llc(level_row: pd.Series, level_name: str):
    llc = dict(level_row)
    for level in VIEW_TYPES:
        if level in llc:
            llc.pop(level)
    if level_name in llc:
        llc.pop(level_name)
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
    hl_col, ml_col, ll_col = BOTTLENECK_ORDER[view_type]
    # Init bottlenecks
    bottlenecks = {}
    # Loop through index tuples
    ids_tuple = ll_view.index
    for hl_id, ml_id, ll_id in ids_tuple:
        hl_row = hl_view.loc[hl_id]
        hl_name, hl_val = _get_level_value(hl_row, hl_col, hl_id)
        ml_row = ml_view.loc[(hl_id, ml_id)]
        ml_name, ml_val = _get_level_value(ml_row, ml_col, ml_id)
        ll_row = ll_view.loc[(hl_id, ml_id, ll_id)]
        ll_name, ll_val = _get_level_value(ll_row, ll_col, ll_id)
        if hl_val not in bottlenecks:
            bottlenecks[hl_val] = {'llc': None, ml_col: {}}
            bottlenecks[hl_val]['llc'] = _calculate_llc(hl_row, hl_name)
        if ml_val not in bottlenecks[hl_val][ml_col]:
            bottlenecks[hl_val][ml_col][ml_val] = {'llc': None, ll_col: {}}
            bottlenecks[hl_val][ml_col][ml_val]['llc'] = _calculate_llc(ml_row, ml_name)
        if ll_val not in bottlenecks[hl_val][ml_col][ml_val][ll_col]:
            bottlenecks[hl_val][ml_col][ml_val][ll_col][ll_val] = {'llc': None}
            bottlenecks[hl_val][ml_col][ml_val][ll_col][ll_val]['llc'] = _calculate_llc(ll_row, ll_name)
    # Return view key & bottlenecks
    return view_key, bottlenecks


class RecorderBottleneckDetector(BottleneckDetector):

    def __init__(
        self,
        logger,
        log_dir: str,
        unique_file_names: dd.DataFrame,
        unique_proc_names: dd.DataFrame,
    ):
        super().__init__(logger, log_dir)
        self.unique_file_names = unique_file_names
        self.unique_proc_names = unique_proc_names

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

        # Get parent view
        bottleneck_view = view_dict['bottleneck_view']

        # Create lower level view
        low_level_view = bottleneck_view \
            .groupby(list(BOTTLENECK_ORDER[view_type])) \
            .first() \
            .drop(columns=['acc_pat', 'io_cat', 'file_name', 'proc_name'], errors='ignore')

        # Non-proc agg columns
        non_proc_agg_dict = self._get_agg_dict(view_types=view_types, view_columns=low_level_view.columns, is_proc=False)
        proc_agg_dict = self._get_agg_dict(view_types=view_types, view_columns=low_level_view.columns, is_proc=True)

        if view_type is not 'proc_id':

            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([view_type, 'proc_id']) \
                .agg(non_proc_agg_dict)

            high_level_view = mid_level_view \
                .reset_index() \
                .groupby([view_type]) \
                .agg(proc_agg_dict)

        else:

            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([view_type, 'trange']) \
                .agg(non_proc_agg_dict)

            high_level_view = mid_level_view \
                .reset_index() \
                .groupby([view_type]) \
                .agg(non_proc_agg_dict)

        if view_type is not 'proc_id':
            if view_type is 'file_id':
                mid_level_view = mid_level_view \
                    .merge(self.unique_file_names, left_index=True, right_index=True) \
                    .merge(self.unique_proc_names, left_index=True, right_index=True)
                high_level_view = high_level_view.merge(self.unique_file_names, left_index=True, right_index=True)
            else:
                mid_level_view = mid_level_view.merge(self.unique_proc_names, left_index=True, right_index=True)
        else:
            mid_level_view = mid_level_view.merge(self.unique_proc_names, left_index=True, right_index=True)
            high_level_view = high_level_view.merge(self.unique_proc_names, left_index=True, right_index=True)

        low_level_view = low_level_view \
            .merge(self.unique_file_names, left_index=True, right_index=True) \
            .merge(self.unique_proc_names, left_index=True, right_index=True)

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
        return agg_dict
