import dask.dataframe as dd
import itertools as it
import json
from typing import Dict, Tuple
from ._recorder.analysis import compute_main_view, compute_max_io_time, compute_view
from ._recorder.rules import RecorderRuleEngine
from .base import Analyzer
from .dask import ClusterManager
from .rules import Rule

VIEW_TYPES = ['trange', 'file_id', 'proc_id']


class RecorderAnalyzer(Analyzer):

    def __init__(
        self,
        working_dir: str,
        cluster_manager_args: dict = None,
        debug=False
    ):
        super().__init__('recorder', working_dir, debug)
        # Create cluster manager
        self.cluster_manager = ClusterManager(
            working_dir=working_dir,
            n_clusters=1,
            logger=self.logger,
            verbose=self.debug,
            **cluster_manager_args
        )
        # Boot cluster
        self.cluster_manager.boot()

    def analyze_parquet(self, log_dir: str, delta=0.0001):
        # Load global min max
        global_min_max = self.load_global_min_max(log_dir=log_dir)
        # Compute main view
        main_view = compute_main_view(
            log_dir=log_dir,
            global_min_max=global_min_max,
            view_types=VIEW_TYPES
        )
        # Compute `max_io_time`
        max_io_time = compute_max_io_time(main_view=main_view)
        # Compute multifaceted views
        views = {}
        for view_permutation in it.chain.from_iterable(map(self._view_permutations, range(len(VIEW_TYPES)))):
            views[view_permutation] = compute_view(
                main_view=main_view,
                view_types=VIEW_TYPES,
                views=views,
                view_permutation=view_permutation,
                max_io_time=max_io_time,
                delta=delta
            )
            # print(view_permutation, len(views[view_permutation]))

        # Return views
        return views

    def load_global_min_max(self, log_dir: str) -> dict:
        with open(f"{log_dir}/global.json") as file:
            global_min_max = json.load(file)
        return global_min_max

    def _save_views(self, log_dir: str, views: Dict[Tuple, dd.DataFrame]):
        for view_perm, view in views.items():
            copy_view = view.copy()
            copy_view.columns = ['_'.join(tup).rstrip('_') for tup in copy_view.columns.values]
            copy_view.to_parquet(f"{log_dir}/{'_'.join(view_perm) if isinstance(view_perm, tuple) else view_perm}")

    @staticmethod
    def _view_permutations(r: int):
        return it.permutations(VIEW_TYPES, r + 1)
