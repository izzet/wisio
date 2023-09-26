import dask.dataframe as dd
import itertools as it
import json
import os
from dask import compute
from typing import Dict, Union
from ._recorder.analysis import compute_main_view, compute_view, set_logical_columns
from ._recorder.bottlenecks import RecorderBottleneckDetector
from ._recorder.constants import LOGICAL_VIEW_TYPES, METRIC_COLS, VIEW_TYPES
from .analysis_result import AnalysisResult
from .base import Analyzer
from .dask import ClusterManager
from .types import _view_name
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

    def analyze_parquet(
        self,
        log_dir: str,
        metrics=['duration_sum'],
        view_names=[],
        slope_threshold=45,
        checkpoint=False,
        checkpoint_dir='',
        persist=True,
    ):
        # Ensure checkpoint dir
        checkpoint_dir = None
        if checkpoint:
            checkpoint_dir = self._ensure_checkpoint_dir(log_dir=log_dir)

        # Load global min max
        with ElapsedTimeLogger(logger=self.logger, message='Load global min/max'):
            global_min_max = self.load_global_min_max(log_dir=log_dir)

        # Compute main view
        if checkpoint and self._has_checkpoint(checkpoint_dir=checkpoint_dir, view_name=CHECKPOINT_MAIN_VIEW):
            raise NotImplementedError
            with ElapsedTimeLogger(logger=self.logger, message='Read saved main view'):
                main_view = self._read_checkpoint(
                    checkpoint_dir=checkpoint_dir, view_name=CHECKPOINT_MAIN_VIEW)
        else:
            with ElapsedTimeLogger(logger=self.logger, message='Compute main view'):
                # Compute main view
                main_view = compute_main_view(
                    log_dir=log_dir,
                    global_min_max=global_min_max,
                    view_types=VIEW_TYPES,
                )
            if checkpoint:
                with ElapsedTimeLogger(logger=self.logger, message='Save main view'):
                    self._checkpoint(checkpoint_dir=checkpoint_dir,
                                     view_name=CHECKPOINT_MAIN_VIEW, view=main_view).compute()

        # Keep views & tasks

        checkpoint_tasks = []
        views_need_checkpoint = []

        norm_data = {}
        view_results = {}

        # Compute multifaceted views for each metric
        for metric in metrics:

            norm_data[metric] = {}
            view_results[metric] = {}

            for view_permutation in it.chain.from_iterable(map(self._view_permutations, range(len(VIEW_TYPES)))):
                view_name = _view_name(view_permutation)
                if len(view_names) > 0 and view_name not in view_names:
                    continue
                if checkpoint and self._has_checkpoint(checkpoint_dir=checkpoint_dir, view_name=view_name):
                    raise NotImplementedError
                    with ElapsedTimeLogger(logger=self.logger, message=f"Read saved {view_name} view"):
                        views[metric][view_permutation] = self._read_checkpoint(
                            checkpoint_dir=checkpoint_dir, view_name=view_name)
                else:
                    with ElapsedTimeLogger(logger=self.logger, message=f"Compute {view_name} view"):
                        # Read types
                        view_type = view_permutation[-1]
                        norm_data_type = (view_type,)
                        parent_type = view_permutation[:-1]

                        # Get parent view & normalization data
                        parent_norm_data = norm_data[metric][norm_data_type] if norm_data_type in norm_data[metric] else None
                        parent_view = view_results[metric][parent_type].view if parent_type in view_results[metric] else main_view

                        # Compute view
                        view_result = compute_view(
                            parent_view=parent_view,
                            view_type=view_type,
                            metric_col=METRIC_COLS[metric],
                            norm_data=parent_norm_data,
                            slope_threshold=slope_threshold,
                        )

                        norm_data[metric][view_permutation] = view_result.norm_data
                        view_results[metric][view_permutation] = view_result

                        views_need_checkpoint.append(view_permutation)

        # Compute logical views for each metric
        main_view_with_logical_columns = set_logical_columns(view=main_view)
        for metric in metrics:
            for parent_type, logical_view_type in LOGICAL_VIEW_TYPES:
                view_permutation = (logical_view_type,)
                view_name = _view_name(view_permutation)
                if len(view_names) > 0 and view_name not in view_names:
                    continue
                if checkpoint and self._has_checkpoint(checkpoint_dir=checkpoint_dir, view_name=view_name):
                    raise NotImplementedError
                    with ElapsedTimeLogger(logger=self.logger, message=f"Read saved {view_name} view"):
                        views[metric][view_permutation] = self._read_checkpoint(
                            checkpoint_dir=checkpoint_dir, view_name=view_name)
                else:
                    with ElapsedTimeLogger(logger=self.logger, message=f"Compute {view_name} view"):
                        view_result = compute_view(
                            parent_view=main_view_with_logical_columns,
                            view_type=logical_view_type,
                            metric_col=METRIC_COLS[metric],
                            norm_data=norm_data[metric][(parent_type,)],
                            slope_threshold=slope_threshold,
                        )

                        norm_data[metric][view_permutation] = view_result.norm_data
                        view_results[metric][view_permutation] = view_result

                        views_need_checkpoint.append(view_permutation)

        # Checkpoint views
        if checkpoint:
            raise NotImplementedError
            for view_permutation, view_result in view_results.items():
                if view_permutation in views_need_checkpoint:
                    view_name = _view_name(view_permutation)
                    checkpoint_task = self._checkpoint(
                        checkpoint_dir=checkpoint_dir, view_name=view_name, view=view_result.view)
                    checkpoint_tasks.append(checkpoint_task)

            with ElapsedTimeLogger(logger=self.logger, message=f"Checkpoint views"):
                compute(*checkpoint_tasks)

            # Detect bottlenecks
        bottleneck_detector = RecorderBottleneckDetector(logger=self.logger)
        with ElapsedTimeLogger(logger=self.logger, message='Detect bottlenecks'):
            bottlenecks = bottleneck_detector.detect_bottlenecks(
                view_results=view_results,
                metrics=metrics,
            )

        # Return views
        return AnalysisResult(
            main_view=main_view,
            view_results=view_results,
            bottlenecks=bottlenecks
        )

    def load_global_min_max(self, log_dir: str) -> dict:
        with open(f"{log_dir}/global.json") as file:
            global_min_max = json.load(file)
        return global_min_max

    def save_bottlenecks(self, log_dir: str, bottlenecks: Dict[tuple, object]):
        bottleneck_dir = f"{log_dir}/bottlenecks"
        ensure_dir(bottleneck_dir)
        for view_key, bottleneck_dict in bottlenecks.items():
            file_name = '_'.join(view_key) if isinstance(
                view_key, tuple) else view_key
            with open(f"{bottleneck_dir}/{file_name}.json", 'w') as json_file:
                json.dump(bottleneck_dict, json_file,
                          cls=NpEncoder, sort_keys=True)

    def _checkpoint(self, checkpoint_dir: str, view_name: str, view: dd.DataFrame, partition_size='100MB') -> dd.core.Scalar:
        return view \
            .repartition(partition_size) \
            .to_parquet(f"{checkpoint_dir}/{view_name}", compute=False)

    def _ensure_checkpoint_dir(self, log_dir):
        checkpoint_dir = f"{log_dir}/checkpoints"
        ensure_dir(checkpoint_dir)
        return checkpoint_dir

    def _has_checkpoint(self, checkpoint_dir: str, view_name: str):
        return os.path.exists(f"{checkpoint_dir}/{view_name}/_metadata")

    def _read_checkpoint(self, checkpoint_dir: str, view_name: str):
        return dd.read_parquet(f"{checkpoint_dir}/{view_name}")

    @staticmethod
    def _view_permutations(r: int):
        return it.permutations(VIEW_TYPES, r + 1)
