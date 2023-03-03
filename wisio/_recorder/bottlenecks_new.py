import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import compute, delayed
from dask.distributed import Client, as_completed, futures_of, wait
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

@delayed
def _calculate_llc(level_row: dd.DataFrame, ids: list, level_col: str, view_key:tuple):
    #print(level_col, level_ids)
    level_name, level_val = _get_level_value(level_row, level_col, ids[:-1])
    for level in ["proc_id", "trange", "file_id"]:
        if level in level_row:
            level_row.pop(level)
    if level_name in level_row:
        level_row.pop(level_name)
    level_row['name'] = level_val
    return view_key, ids, dict(level_row)
    
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
    # Run through leveled views
    ids_tuple = ll_view.index
    delayed_tasks = []
    #print(ids_tuple)
    #hl_view = hl_view.reset_index()
    for hl, ml, ll in ids_tuple:
        if hl not in bottlenecks:
            bottlenecks[hl] = {'llc':None, ml_col:{}}
            hl_row =  hl_view.loc[hl]
            delayed_tasks.append(_calculate_llc(hl_row, [hl], hl_col, view_key))
        if ml not in bottlenecks[hl][ml_col]:
            bottlenecks[hl][ml_col][ml] = {'llc':None, ll_col:{}}
            ml_row = ml_view.loc[(hl,ml)]
            delayed_tasks.append(_calculate_llc(ml_row, [hl, ml], ml_col, view_key))
        if ll not in bottlenecks[hl][ml_col][ml][ll_col]:
            bottlenecks[hl][ml_col][ml][ll_col][ll] = {'llc':None}
            ll_row = ll_view.loc[(hl,ml,ll)]
            delayed_tasks.append(_calculate_llc(ll_row, [hl, ml, ll], ll_col, view_key))
    return bottlenecks, delayed_tasks


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
            #all_bottleneck_views_futures.extend(map(futures_of, bottleneck_views.values()))
        # Wait all bottleneck views to be done
        #wait(all_bottleneck_views_futures)
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
        with ElapsedTimeLogger(logger=self.logger, message='Compute Delayed funcs'):
            for view_key, bottleneck_views in all_bottleneck_views.items():
                bottlenecks, delayed_tasks = _process_bottleneck_view(
                    view_key=view_key,
                    ll_view=bottleneck_views['low_level_view'],
                    ml_view=bottleneck_views['mid_level_view'],
                    hl_view=bottleneck_views['high_level_view']
                )
                all_bottlenecks_d.extend(delayed_tasks)
                all_bottlenecks[view_key] = bottlenecks
            futures = compute(*all_bottlenecks_d, sync=False)
        with ElapsedTimeLogger(logger=self.logger, message='Compute bottlenecks'):
            total = len(futures)
            print(f"processing {total} tasks")
            completed = 0
            vals = Client.current().gather(list(futures))
            print(f"gathered {total} tasks")
            for view_key, level_ids, llc in vals:
                completed += 1
                #view_key, level_ids, llc = future.result()
                if len(level_ids) == 1:
                     all_bottlenecks[view_key][level_ids[0]]['llc'] = llc
                if len(level_ids) == 2:
                     all_bottlenecks[view_key][level_ids[0]][level_ids[1]]['llc'] = llc
                if len(level_ids) == 3:
                     all_bottlenecks[view_key][level_ids[0]][level_ids[1]][level_ids[2]]['llc'] = llc
                if completed % 100 == 0:
                    print(f"completed {completed} of {total} tasks", end='\r')
            print(f"\nCompleted {total} tasks")
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

        if view_type is 'trange':
            non_proc_agg_dict['file_id'] = unique_flatten()
        elif view_type is 'file_id':
            non_proc_agg_dict['trange'] = unique_flatten()

        if view_type is not 'proc_id':
            proc_agg_dict['file_id'] = unique_flatten()
            proc_agg_dict['proc_id'] = unique_flatten()
            proc_agg_dict['trange'] = unique_flatten()
            proc_agg_dict.pop(view_type)

            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([view_type, 'proc_id']) \
                .agg(non_proc_agg_dict)

            high_level_view = mid_level_view \
                .reset_index() \
                .groupby([view_type]) \
                .agg(proc_agg_dict)

        else:
            non_proc_agg_dict['file_id'] = unique_flatten()

            mid_level_view = low_level_view \
                .reset_index() \
                .groupby([view_type, 'trange']) \
                .agg(non_proc_agg_dict)

            non_proc_agg_dict['trange'] = unique_flatten()

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
            low_level_view=low_level_view.compute(),
            mid_level_view=mid_level_view.compute(),
            high_level_view=high_level_view.compute()
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
