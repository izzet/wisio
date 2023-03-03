import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import compute, delayed
from dask.distributed import Client,as_completed, futures_of, wait
from itertools import permutations
from pathlib import PurePath
from typing import Dict
from ..bottlenecks import BottleneckDetector
from ..utils.collection_utils import deepflatten
from ..utils.dask_agg import unique_flatten
from ..utils.logger import ElapsedTimeLogger


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

def _get_level_value(row, col_name, col_id):
    tree_name = BOTTLENECK_TREE_NAME[col_name]
    # print('_get_level_name', col_name, col_id, bool(tree_name in row), bool(tree_name in dict(row)))
    if tree_name in dict(row):
        return tree_name, row[tree_name]
    return tree_name, col_id


def _calculate_llc(level_row: dd.DataFrame, level_id: list, level_col: str, level_name:str):
    #print(level_col, level_ids)
    for level in ["proc_id", "trange", "file_id"]:
        if level in level_row:
            level_row.pop(level)
    if level_name in level_row:
        level_row.pop(level_name)
    return dict(level_row)

@delayed
def _process_bottleneck_view(
    view_key: tuple,
    ll_view: dd.DataFrame,
    ml_view: dd.DataFrame,
    hl_view: dd.DataFrame,
):
    # Get view type
    view_type = view_key[-1]
    # Get ordered bottleneck columns
    hl_col, ml_col, ll_col = BOTTLENECK_ORDER[view_type]
    # Init bottlenecks
    bottlenecks = {}
    ids_tuple = ll_view.index
    for hl, ml, ll in ids_tuple:
        hl_row =  hl_view.loc[hl]
        hl_name, hl_val = _get_level_value(hl_row, hl_col, hl)
        ml_row =  ml_view.loc[(hl,ml)]
        ml_name, ml_val = _get_level_value(ml_row, ml_col, ml)
        ll_row =  ll_view.loc[(hl,ml,ll)]
        ll_name, ll_val = _get_level_value(ll_row, ll_col, ll)
        if hl_val not in bottlenecks:
            bottlenecks[hl_val] = {'llc':None, ml_col:{}}
            bottlenecks[hl_val]['llc'] = _calculate_llc(hl_row, hl, hl_col, hl_name)
        if ml_val not in bottlenecks[hl_val][ml_col]:
            bottlenecks[hl_val][ml_col][ml_val] = {'llc':None, ll_col:{}}
            bottlenecks[hl_val][ml_col][ml_val]['llc'] = _calculate_llc(ml_row, ml, ml_col, ml_name)
        if ll_val not in bottlenecks[hl_val][ml_col][ml_val][ll_col]:
            bottlenecks[hl_val][ml_col][ml_val][ll_col][ll_val] = {'llc':None}
            bottlenecks[hl_val][ml_col][ml_val][ll_col][ll_val]['llc'] = _calculate_llc(ll_row, ll, ll_col, ll_name)
    return view_key, bottlenecks


class RecorderBottleneckDetector(BottleneckDetector):

    def __init__(
        self,
        logger,
        log_dir: str,
        views: Dict[tuple, dd.DataFrame],
        view_types: list,
        unique_file_names: dd.DataFrame,
        unique_proc_names: dd.DataFrame,
    ):
        super().__init__(logger, log_dir, views, view_types)
        self.unique_file_names = unique_file_names
        self.unique_proc_names = unique_proc_names

    def detect_bottlenecks(self, max_io_time: dd.core.Scalar, cut=0.5) -> Dict[tuple, pd.DataFrame]:
        # Keep bottleneck views
        all_bottleneck_views = {}
        all_bottleneck_views_futures = []
        # Run through views
        for view_key, view_dict in self.views.items():
            # Generate bottleneck views
            bottleneck_views = self._generate_bottlenecks_views(
                view_key=view_key,
                view_dict=view_dict,
            )
            # Set bottleneck views
            all_bottleneck_views[view_key] = bottleneck_views
            # Get futures
            all_bottleneck_views_futures.extend(map(futures_of, bottleneck_views.values()))
        # Wait all bottleneck views to be done
        wait(all_bottleneck_views_futures)
        # Generate bottlenecks
        bottlenecks = self._process_bottleneck_views(all_bottleneck_views=all_bottleneck_views)
        # Return bottleneck views
        return bottlenecks

    

    def _process_bottleneck_views(
        self,
        all_bottleneck_views: Dict[tuple, pd.DataFrame]
    ):
        # Init bottlenecks
        all_bottlenecks = {}
        all_bottlenecks_d = []
        # Run through bottleneck views
        for view_key, bottleneck_views in all_bottleneck_views.items():
            
            all_bottlenecks_d.append(_process_bottleneck_view(
                view_key=view_key,
                ll_view=bottleneck_views['low_level_view'],
                ml_view=bottleneck_views['mid_level_view'],
                hl_view=bottleneck_views['high_level_view']
            ))

            # all_bottlenecks[view_key] = bottlenecks
        with ElapsedTimeLogger(logger=self.logger, message='Compute bottlenecks'):
            futures = compute(*all_bottlenecks_d, sync=False)
            vals = Client.current().gather(list(futures))
            for view_key, bottlenecks in vals:
                all_bottlenecks[view_key] = bottlenecks

        return all_bottlenecks

    

    def _generate_bottlenecks_views(
        self,
        view_key: tuple,
        view_dict: Dict[str, dd.DataFrame]
    ):
        # Read types
        parent_type = view_key[:-1]
        view_type = view_key[-1]
        # Get parent view
        parent_view = view_dict['bottleneck_view']
        view_type = view_key[-1]

        # Create lower level view
        low_level_view = parent_view \
            .groupby(list(BOTTLENECK_ORDER[view_type])) \
            .first() \
            .drop(columns=['acc_pat', 'io_cat', 'file_name', 'proc_name'], errors='ignore')

        # Non-proc agg columns
        non_proc_agg_dict = self._get_agg_dict(view_columns=low_level_view.columns, is_proc=False)
        proc_agg_dict = self._get_agg_dict(view_columns=low_level_view.columns, is_proc=True)

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

    def _get_agg_dict(self, view_columns: list, is_proc=False):
        if is_proc:
            agg_dict = {col: max if any(x in col for x in 'duration time'.split()) else sum for col in view_columns}
        else:
            agg_dict = {col: sum for col in view_columns}
        agg_dict['func_id'] = unique_flatten()
        agg_dict['size_min'] = min
        agg_dict['size_max'] = max
        for view_type in self.view_types:
            if view_type in agg_dict:
                agg_dict.pop(view_type)
        return agg_dict
