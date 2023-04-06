import dask.dataframe as dd
import itertools as it
import json
import os
from typing import Dict
from ._recorder.analysis import (
    compute_main_view,
    compute_max_io_time,
    compute_view,
    set_logical_columns
)
from ._recorder.bottlenecks import RecorderBottleneckDetector
from ._recorder.constants import LOGICAL_VIEW_TYPES, VIEW_TYPES
from .base import Analyzer
from .dask import ClusterManager
from .utils.file_utils import ensure_dir
from .utils.json_encoders import NpEncoder
from .utils.logger import ElapsedTimeLogger


CHECKPOINT_MAIN_VIEW = '_main_view'


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
        # Create dirs
        checkpoint_dir = f"{log_dir}/checkpoints"
        ensure_dir(checkpoint_dir)

        # Load global min max
        with ElapsedTimeLogger(logger=self.logger, message='Load global min/max'):
            global_min_max = self.load_global_min_max(log_dir=log_dir)

        # Compute main view
        if os.path.exists(f"{checkpoint_dir}/{CHECKPOINT_MAIN_VIEW}/_metadata"):
            with ElapsedTimeLogger(logger=self.logger, message='Read saved main view'):
                main_view = dd.read_parquet(f"{checkpoint_dir}/{CHECKPOINT_MAIN_VIEW}")
        else:
            with ElapsedTimeLogger(logger=self.logger, message='Compute main view'):
                main_view = compute_main_view(
                    log_dir=log_dir,
                    global_min_max=global_min_max,
                    view_types=VIEW_TYPES
                )
            with ElapsedTimeLogger(logger=self.logger, message='Save main view'):
                main_view.to_parquet(f"{checkpoint_dir}/{CHECKPOINT_MAIN_VIEW}")

        # Compute `max_io_time`
        with ElapsedTimeLogger(logger=self.logger, message='Compute max I/O time'):
            max_io_time = compute_max_io_time(main_view=main_view)

        # Compute multifaceted views
        views = {}
        views_need_checkpoint = []

        # Loop through view permutations
        for view_permutation in it.chain.from_iterable(map(self._view_permutations, range(len(VIEW_TYPES)))):
            view_name = '_'.join(view_permutation) if isinstance(view_permutation, tuple) else view_permutation
            if os.path.exists(f"{checkpoint_dir}/{view_name}/_metadata"):
                with ElapsedTimeLogger(logger=self.logger, message=f"Read saved {view_name} view"):
                    views[view_permutation] = dd.read_parquet(f"{checkpoint_dir}/{view_name}")
            else:
                with ElapsedTimeLogger(logger=self.logger, message=f"Compute {view_name} view"):
                    # Read types
                    parent_type = view_permutation[:-1]
                    logical_view_type = view_permutation[-1]
                    # Get parent view
                    parent_view = views[parent_type] if parent_type in views else main_view
                    # Compute view
                    views[view_permutation] = compute_view(
                        parent_view=parent_view,
                        view_type=logical_view_type,
                        max_io_time=max_io_time,
                        delta=delta,
                    )
                    views_need_checkpoint.append(view_permutation)

        for view_permutation in it.chain.from_iterable(map(self._view_permutations, range(len(VIEW_TYPES)))):
            view_name = '_'.join(view_permutation) if isinstance(view_permutation, tuple) else view_permutation
            if view_permutation in views_need_checkpoint:
                with ElapsedTimeLogger(logger=self.logger, message=f"Save {view_name} view"):
                    views[view_permutation].to_parquet(f"{checkpoint_dir}/{view_name}")

        # Compute logical views
        main_view_with_logical_columns = set_logical_columns(view=main_view)
        for logical_view_type in LOGICAL_VIEW_TYPES:
            views[(logical_view_type,)] = compute_view(
                parent_view=main_view_with_logical_columns,
                view_type=logical_view_type,
                max_io_time=max_io_time,
                delta=delta,
            )

        # Detect bottlenecks
        bottleneck_detector = RecorderBottleneckDetector(logger=self.logger)
        with ElapsedTimeLogger(logger=self.logger, message='Detect bottlenecks'):
            bottlenecks = bottleneck_detector.detect_bottlenecks(
                views=views,
                max_io_time=max_io_time,
            )

        # Return views
        return main_view, views, bottlenecks

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

    @staticmethod
    def _view_permutations(r: int):
        return it.permutations(VIEW_TYPES, r + 1)
