import json
from typing import Dict, List
from ._recorder.analysis import (
    compute_high_level_metrics,
    compute_main_view,
    compute_view,
    set_logical_columns,
)
from ._recorder.bottlenecks import RecorderBottleneckDetector
from ._recorder.constants import LOGICAL_VIEW_TYPES, METRIC_COLS
from .analysis_result import AnalysisResult
from .analyzer import Analyzer
from .cluster_management import ClusterConfig
from .types import AnalysisAccuracy, ViewType
from .utils.file_utils import ensure_dir
from .utils.json_encoders import NpEncoder
from .utils.logger import ElapsedTimeLogger


CHECKPOINT_MAIN_VIEW = '_main_view'
CHECKPOINT_HLM = '_hlm'


class RecorderAnalyzer(Analyzer):

    def __init__(
        self,
        working_dir: str,
        checkpoint: bool = False,
        checkpoint_dir: str = '',
        cluster_config: ClusterConfig = None,
        debug=False
    ):
        super().__init__(
            name='Recorder',
            working_dir=working_dir,
            checkpoint=checkpoint,
            checkpoint_dir=checkpoint_dir,
            cluster_config=cluster_config,
            debug=debug,
        )

    def analyze_parquet(
        self,
        trace_path: str,
        metrics=['duration'],
        accuracy: AnalysisAccuracy = 'pessimistic',
        slope_threshold: int = 45,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ):

        # Load global min max
        with ElapsedTimeLogger(message='Load global min/max'):
            global_min_max = self.load_global_min_max(trace_path=trace_path)

        # Compute high-level metrics
        with ElapsedTimeLogger(message='Compute high-level metrics'):
            hlm = self.load_view(
                view_name=CHECKPOINT_HLM,
                fallback=lambda: compute_high_level_metrics(
                    trace_path=trace_path,
                    global_min_max=global_min_max,
                    view_types=view_types,
                ),
                force=True,
            )

        # Compute main view
        with ElapsedTimeLogger(message='Compute main view'):
            main_view = self.load_view(
                view_name=CHECKPOINT_MAIN_VIEW,
                fallback=lambda: compute_main_view(
                    hlm=hlm,
                    view_types=view_types,
                ),
                force=True,
            )

        # Keep views
        norm_data = {}
        view_results = {}

        # Compute multifaceted views for each metric
        for metric in metrics:

            norm_data[metric] = {}
            view_results[metric] = {}

            for view_permutation in self.view_permutations(view_types=view_types):

                view_type = view_permutation[-1]
                norm_data_type = (view_type,)
                parent_view_type = view_permutation[:-1]

                parent_norm_data = norm_data[metric].get(norm_data_type, None)
                parent_view_result = view_results[metric].get(
                    parent_view_type, None)
                parent_view = main_view if parent_view_result is None else parent_view_result.view

                view_result = compute_view(
                    metric_col=METRIC_COLS[metric],
                    norm_data=parent_norm_data,
                    parent_view=parent_view,
                    slope_threshold=slope_threshold,
                    view_type=view_type,
                )

                norm_data[metric][view_permutation] = view_result.norm_data
                view_results[metric][view_permutation] = view_result

        # Compute logical views for each metric
        main_view_with_logical_columns = set_logical_columns(view=main_view)

        for metric in metrics:

            for parent_view_type, logical_view_type in LOGICAL_VIEW_TYPES:

                if parent_view_type not in view_types:
                    continue

                norm_data_type = (parent_view_type,)
                view_permutation = (parent_view_type, logical_view_type)
                view_type = view_permutation[-1]

                parent_norm_data = norm_data[metric].get(norm_data_type, None)
                parent_view_result = view_results[metric].get(
                    parent_view_type, None)
                parent_view = main_view_with_logical_columns if parent_view_result is None else parent_view_result.view

                view_result = compute_view(
                    metric_col=METRIC_COLS[metric],
                    norm_data=parent_norm_data,
                    parent_view=parent_view,
                    slope_threshold=slope_threshold,
                    view_type=view_type,
                )

                norm_data[metric][view_permutation] = view_result.norm_data
                view_results[metric][view_permutation] = view_result

            # Detect bottlenecks
        bottleneck_detector = RecorderBottleneckDetector()
        with ElapsedTimeLogger(message='Detect bottlenecks'):
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

    def load_global_min_max(self, trace_path: str) -> dict:
        with open(f"{trace_path}/global.json") as file:
            global_min_max = json.load(file)
        return global_min_max

    def save_bottlenecks(self, trace_path: str, bottlenecks: Dict[tuple, object]):
        bottleneck_dir = f"{trace_path}/bottlenecks"
        ensure_dir(bottleneck_dir)
        for view_key, bottleneck_dict in bottlenecks.items():
            file_name = '_'.join(view_key) if isinstance(
                view_key, tuple) else view_key
            with open(f"{bottleneck_dir}/{file_name}.json", 'w') as json_file:
                json.dump(bottleneck_dict, json_file,
                          cls=NpEncoder, sort_keys=True)
