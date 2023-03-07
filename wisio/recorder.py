import dask.dataframe as dd
import itertools as it
import json
from typing import Dict, Tuple
from ._recorder.analysis import (
    compute_main_view,
    compute_max_io_time,
    compute_unique_file_names,
    compute_unique_proc_names,
    compute_view
)
from ._recorder.bottlenecks import RecorderBottleneckDetector
from ._recorder.constants import VIEW_TYPES
from .base import Analyzer
from .dask import ClusterManager
from .utils.file_utils import ensure_dir
from .utils.json_encoders import NpEncoder
from .utils.logger import ElapsedTimeLogger


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

    def analyze_parquet(self, log_dir: str, delta=0.0001, cut=0.5):
        # Load global min max
        with ElapsedTimeLogger(logger=self.logger, message='Load global min/max'):
            global_min_max = self.load_global_min_max(log_dir=log_dir)

        # Compute main view
        with ElapsedTimeLogger(logger=self.logger, message='Compute main view'):
            main_view = compute_main_view(
                log_dir=log_dir,
                global_min_max=global_min_max,
                view_types=VIEW_TYPES
            )

        # Compute `max_io_time`
        with ElapsedTimeLogger(logger=self.logger, message='Compute max I/O time'):
            max_io_time = compute_max_io_time(main_view=main_view)

        # Compute multifaceted views
        views = {}
        with ElapsedTimeLogger(logger=self.logger, message='Compute multifaceted view'):
            # Loop through view permutations
            for view_permutation in it.chain.from_iterable(map(self._view_permutations, range(len(VIEW_TYPES)))):
                # Compute view
                views[view_permutation] = compute_view(
                    main_view=main_view,
                    views=views,
                    view_permutation=view_permutation,
                    max_io_time=max_io_time,
                    delta=delta,
                    cut=cut
                )

        # Detect bottlenecks
        bottleneck_detector = RecorderBottleneckDetector(logger=self.logger, log_dir=log_dir)
        with ElapsedTimeLogger(logger=self.logger, message='Detect bottlenecks'):
            bottlenecks = bottleneck_detector.detect_bottlenecks(
                views=views,
                view_types=VIEW_TYPES
            )

        with ElapsedTimeLogger(logger=self.logger, message='Save bottlenecks'):
            self.save_bottlenecks(log_dir=log_dir, bottlenecks=bottlenecks)

        # Return views
        return views, bottlenecks

    def load_global_min_max(self, log_dir: str) -> dict:
        with open(f"{log_dir}/global.json") as file:
            global_min_max = json.load(file)
        return global_min_max

    def save_bottlenecks(self, log_dir: str, bottlenecks: Dict[tuple, object]):
        bottleneck_dir = f"{log_dir}/bottlenecks"
        ensure_dir(bottleneck_dir)
        for view_key, bottleneck_dict in bottlenecks.items():
            file_name = '_'.join(view_key) if isinstance(view_key, tuple) else view_key
            with open(f"{bottleneck_dir}/{file_name}.json", 'w') as json_file:
                json.dump(bottleneck_dict, json_file, cls=NpEncoder, sort_keys=True)

    def _save_views(self, log_dir: str, views: Dict[Tuple, dd.DataFrame]):
        for view_perm, view in views.items():
            copy_view = view.copy()
            copy_view.columns = ['_'.join(tup).rstrip('_') for tup in copy_view.columns.values]
            copy_view.to_parquet(f"{log_dir}/{'_'.join(view_perm) if isinstance(view_perm, tuple) else view_perm}")

    @staticmethod
    def _view_permutations(r: int):
        return it.permutations(VIEW_TYPES, r + 1)
