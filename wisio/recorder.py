import dask.dataframe as dd
import itertools as it
import json
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pandas import DataFrame
from typing import Dict, List, Tuple

from ._recorder.analysis import compute_higher_view, compute_llc, compute_subview, compute_unique_processes, compute_view
from .base import Analyzer
from .dask import ClusterManager

N_PERMUTATIONS = 2


class RecorderAnalyzer(Analyzer):

    def __init__(
        self,
        working_dir: str,
        cluster_manager_args: dict = None,
        debug=False
    ):
        super().__init__('recorder', working_dir, debug)

        # Initialize filter groups
        self.filter_groups = ['trange', 'file_id', 'proc_id']

        # Create cluster manager
        self.cluster_manager = ClusterManager(
            working_dir=working_dir,
            n_clusters=1,
            logger=self.logger,
            verbose=self.debug,
            **cluster_manager_args
        )

        self.cluster_manager.boot()

    def analyze_parquet(self, log_dir: str, delta=0.0001):
        # Load global min max
        global_min_max = self.load_global_min_max(log_dir=log_dir)
        # Compute higher view
        higher_view = compute_higher_view(
            # client=self.cluster_manager.clients[0],
            log_dir=log_dir,
            global_min_max=global_min_max,
            view_types=self.filter_groups
        )
        # Compute hlm
        with ThreadPoolExecutor() as executor:
            # Compute views
            views = self._compute_views(
                executor=executor,
                higher_view=higher_view
            )
            # Compute subviews
            subviews = self._compute_subviews(
                executor=executor,
                higher_view=higher_view,
                views=views
            )
            # Compute llcviews
            llcviews = self._compute_llcviews(
                executor=executor,
                higher_view=higher_view,
                views=views,
                subviews=subviews
            )

        return views, subviews, llcviews

    def load_global_min_max(self, log_dir: str) -> dict:
        with open(f"{log_dir}/global.json") as file:
            global_min_max = json.load(file)
        return global_min_max

    def _compute_views(self, executor: ThreadPoolExecutor, higher_view: pd.DataFrame):
        views = {}
        for view_type, view in zip(
            self.filter_groups,
            executor.map(
                compute_view,
                # self.cluster_manager.clients,
                it.repeat(higher_view),
                self.filter_groups
            )
        ):
            print('view', view_type, len(view))
            views[view_type] = view
        return views

    def _compute_subviews(
        self,
        executor: ThreadPoolExecutor,
        higher_view: pd.DataFrame,
        views: List[pd.DataFrame]
    ):
        subviews = {}
        for view_perm, subview in zip(
            it.permutations(self.filter_groups, N_PERMUTATIONS),
            executor.map(
                compute_subview,
                it.repeat(views),
                it.permutations(self.filter_groups, N_PERMUTATIONS),
            )
        ):
            view_type, subview_type = view_perm
            print('view', view_type, 'subview', subview_type, len(views[view_type]), len(subview))
            subviews[view_type, subview_type] = subview
        return subviews

    def _compute_llcviews(
        self,
        executor: ThreadPoolExecutor,
        higher_view: pd.DataFrame,
        views: List[pd.DataFrame],
        subviews: List[pd.DataFrame]
    ):
        llcviews = {}
        for view_perm, llcview in zip(
            it.permutations(self.filter_groups, N_PERMUTATIONS + 1),
            executor.map(
                compute_llc,
                it.repeat(subviews),
                it.repeat(self.filter_groups),
                it.permutations(self.filter_groups, N_PERMUTATIONS + 1),
            )
        ):
            view_type, subview_type, llc_type = view_perm
            print('view', view_type, 'subview', subview_type, 'llc', llc_type, len(views[view_type]), len(subviews[view_type, subview_type]), len(llcview))
            llcviews[view_type, subview_type, llc_type] = llcview
        return llcviews

    def _compute_unique_names(self, executor: ThreadPoolExecutor, log_dir: str, llc_dfs: Dict[Tuple[str, str, str], DataFrame]):
        # Find all unique file and proc IDs
        unique_file_ids = list(set(it.chain.from_iterable([llc_ddf.index.unique(level='file_id') for llc_ddf in llc_dfs.values()])))
        unique_proc_ids = list(set(it.chain.from_iterable([llc_ddf.index.unique(level='proc_id') for llc_ddf in llc_dfs.values()])))
        # Compute unique names
        unique_filenames_f = executor.submit(
            compute_unique_filenames,
            client=self.cluster_manager.clients[0],
            log_dir=log_dir,
            unique_file_ids=unique_file_ids
        )
        unique_processes_f = executor.submit(
            compute_unique_processes,
            client=self.cluster_manager.clients[1],
            log_dir=log_dir,
            unique_proc_ids=unique_proc_ids
        )
        # Wait for results
        unique_filenames = unique_filenames_f.result()
        unique_processes = unique_processes_f.result()
        print('unique_filenames', len(unique_filenames))
        print('unique_processes', len(unique_processes))
        # Return results
        return unique_filenames, unique_processes

    def _save_views(self, log_dir: str, views: Dict[Tuple, dd.DataFrame]):
        for view_perm, view in views.items():
            view.columns = ['_'.join(tup).rstrip('_') for tup in view.columns.values]
            view.to_parquet(f"{log_dir}/stage1/{'_'.join(view_perm)}/")
