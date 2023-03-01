import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import as_completed, futures_of, wait
from itertools import permutations
from pathlib import PurePath
from typing import Dict
from ..bottlenecks import BottleneckGenerator
from ..utils.collection_utils import deepflatten
from ..utils.dask_agg import unique_flatten


BOTTLENECK_ORDER = dict(
    file_id=('file_id', 'proc_id', 'trange'),
    proc_id=('proc_id', 'trange', 'file_id'),
    trange=('trange', 'proc_id', 'file_id')
)


class RecorderBottleneckGenerator(BottleneckGenerator):

    def __init__(
        self,
        log_dir: str,
        views: Dict[tuple, dd.DataFrame],
        view_types: list,
        unique_filenames: Dict[int, dict],
        unique_processes: Dict[int, dict],
    ):
        super().__init__(log_dir, views, view_types)
        self.unique_filenames = unique_filenames
        self.unique_processes = unique_processes

    def generate_bottlenecks(self, max_io_time: dd.core.Scalar, cut=0.5) -> Dict[tuple, pd.DataFrame]:
        # Keep bottleneck views
        all_bottleneck_views = {}
        all_bottleneck_views_f = []

        low_level_views = {}

        # Run through views
        for view_key, view_dict in self.views.items():
            # Generate bottleneck views
            bottleneck_views = self._generate_bottlenecks_views(
                low_level_views=low_level_views,
                view_key=view_key,
                view_dict=view_dict,
                max_io_time=max_io_time,
                cut=cut
            )
            # Set bottleneck views
            all_bottleneck_views[view_key] = bottleneck_views
            low_level_views[view_key] = bottleneck_views['low_level_view']
            # Get futures
            all_bottleneck_views_f.extend(map(futures_of, bottleneck_views.values()))

        # Wait all bottleneck views to be done
        wait(all_bottleneck_views_f)

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
        # Run through bottleneck views
        for view_key, bottleneck_views in all_bottleneck_views.items():
            # Get view type
            view_type = view_key[-1]
            # Get ordered bottleneck columns
            high_level_col, mid_level_col, low_level_col = BOTTLENECK_ORDER[view_type]
            # Load leveled views
            low_level_view = bottleneck_views['low_level_view'].compute()
            mid_level_view = bottleneck_views['mid_level_view'].compute()
            high_level_view = bottleneck_views['high_level_view'].compute()
            # Init bottlenecks
            bottlenecks = {}
            # Run through leveled views
            # TODO remove iterrows
            for high_level_id, high_level_row in high_level_view.iterrows():
                high_level_name = self._get_level_name(col_name=high_level_col, col_id=high_level_id)
                mid_level_ids = high_level_row[mid_level_col].copy()
                high_level_row.pop(mid_level_col)
                high_level_row.pop(low_level_col)
                bottlenecks[high_level_name] = {}
                bottlenecks[high_level_name]['llc'] = dict(high_level_row)
                bottlenecks[high_level_name][mid_level_col] = {}
                for mid_level_id in mid_level_ids:
                    mid_level_name = self._get_level_name(col_name=mid_level_col, col_id=mid_level_id)
                    mid_level_row = mid_level_view.loc[high_level_id, mid_level_id]
                    low_level_ids = mid_level_row[low_level_col].copy()
                    mid_level_row.pop(low_level_col)
                    bottlenecks[high_level_name][mid_level_col][mid_level_name] = {}
                    bottlenecks[high_level_name][mid_level_col][mid_level_name]['llc'] = dict(mid_level_row)
                    bottlenecks[high_level_name][mid_level_col][mid_level_name][low_level_col] = {}
                    for low_level_id in low_level_ids:
                        low_level_name = self._get_level_name(col_name=low_level_col, col_id=low_level_id)
                        low_level_row = low_level_view.loc[high_level_id, mid_level_id, low_level_id]
                        bottlenecks[high_level_name][mid_level_col][mid_level_name][low_level_col][low_level_name] = {}
                        bottlenecks[high_level_name][mid_level_col][mid_level_name][low_level_col][low_level_name]['llc'] = dict(low_level_row)

            all_bottlenecks[view_key] = bottlenecks

        return all_bottlenecks

    def _get_level_name(self, col_name: str, col_id):
        if col_name == 'proc_id':
            return self.unique_processes[col_id]['rank']
        elif col_name == 'file_id':
            return self.unique_filenames[col_id]['filename']
        return col_id

    def _generate_bottlenecks_views(
        self,
        low_level_views: Dict[tuple, dd.DataFrame],
        view_key: tuple,
        view_dict: Dict[str, dd.DataFrame],
        max_io_time: dd.core.Scalar,
        cut: float
    ):
        # Read types
        parent_type = view_key[:-1]
        view_type = view_key[-1]
        # Get parent view
        parent_view = view_dict['cut_view']
        view_type = view_key[-1]

        # Create lower level view
        low_level_view = parent_view \
            .groupby(list(BOTTLENECK_ORDER[view_type])) \
            .first() \
            .drop(columns=['acc_pat', 'io_cat'], errors='ignore')

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
