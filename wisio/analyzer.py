import abc
import copy
import dask.dataframe as dd
import hashlib
import itertools as it
import json
import logging
import math
import numpy as np
import operator
import os
import pandas as pd
from dask import compute, persist
from dask.base import unpack_collections
from dask.delayed import Delayed
from dask.distributed import fire_and_forget, get_client, wait
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .analysis import (
    THRESHOLD_FUNCTIONS,
    compute_time_boundaries,
    is_metric_time_bound,
    metric_time_column,
    # set_metric_scores,
)
from .analysis_utils import (
    set_file_dir,
    set_file_pattern,
    set_proc_name_parts,
    set_unique_counts,
    split_duration_records_vectorized,
)
from .config import CHECKPOINT_VIEWS, HASH_CHECKPOINT_NAMES
from .constants import (
    ACC_PAT_SUFFIXES,
    COL_CATEGORY,
    COL_FILE_NAME,
    COL_PROC_ID,
    COL_PROC_NAME,
    COL_TIME,
    COL_TIME_RANGE,
    DERIVED_MD_OPS,
    LOGICAL_VIEW_TYPES,
    VIEW_TYPES,
    AccessPattern,
    EventType,
    IOCategory,
    Layer,
)
from .metrics import KNOWN_METRICS, set_metrics, set_metric_diffs, set_metric_scores
from .rule_engine import compute_characteristics, reason_bottlenecks
from .types import (
    AnalyzerResultType,
    BottleneckRules,
    Bottlenecks,
    Metric,
    RawStats,
    ViewKey,
    ViewResult,
    ViewType,
    Views,
    view_name as format_view_name,
)
from .utils.dask_agg import unique_set, unique_set_flatten
from .utils.dask_utils import event_logger, flatten_column_names
from .utils.expr_utils import extract_numerator_and_denominators
from .utils.file_utils import ensure_dir
from .utils.json_encoders import NpEncoder


CHECKPOINT_CATEGORIES = '_categories'
CHECKPOINT_MAIN_VIEW = '_main_view'
CHECKPOINT_METRIC_BOUNDARIES = '_metric_boundaries'
CHECKPOINT_HLM = '_hlm'
CHECKPOINT_RAW_STATS = '_raw_stats'
CHECKPOINT_VIEW = '_view'
HLM_AGG = {
    'time': [sum, 'mean'],
    'count': [sum, 'mean'],
    'size': [sum, min, max, 'mean'],
}
HLM_EXTRA_COLS = ['cat', 'io_cat', 'acc_pat', 'func_name']
VERSION_HLM = "v3"
VERSION_LAYERS = "v1"
VERSION_MAIN_VIEW = "v3"
VERSION_METRIC_BOUNDARIES = "v5"
VERSION_STATS = "v1"
VERSION_VIEWS = "v1"
VIEW_PERMUTATIONS = False
WAIT_ENABLED = True


class Analyzer(abc.ABC):
    def __init__(
        self,
        layer_defs: Dict[Layer, str],
        derived_metrics: Dict[Layer, Dict[str, list]],
        additional_metrics: Optional[Dict[str, Union[str, List[str]]]] = {},
        bottleneck_dir: str = "",
        checkpoint: bool = True,
        checkpoint_dir: str = "",
        debug: bool = False,
        discarded_metric_patterns: Optional[List[str]] = [],
        layer_deps: Optional[Dict[Layer, Optional[Layer]]] = {},
        logical_views: Optional[Dict[ViewType, Dict[ViewType, str]]] = {},
        threaded_layers: Optional[List[str]] = [],
        time_approximate: bool = True,
        time_granularity: float = 1e6,
        unscored_metrics: Optional[List[str]] = [],
        verbose: bool = False,
    ):
        if checkpoint:
            assert checkpoint_dir != '', 'Checkpoint directory must be defined'

        self.additional_metrics = additional_metrics
        self.bottleneck_dir = bottleneck_dir
        self.checkpoint = checkpoint
        self.checkpoint_dir = checkpoint_dir
        self.debug = debug
        self.derived_metrics = derived_metrics
        self.discarded_metric_patterns = '|'.join(discarded_metric_patterns)
        self.layer_defs = layer_defs
        self.layer_deps = layer_deps
        self.logical_views = logical_views
        self.root_layers = [
            layer for layer, parent_layer in self.layer_deps.items() if not parent_layer
        ]
        self.threaded_layers = threaded_layers
        self.time_approximate = time_approximate
        self.time_granularity = time_granularity
        self.unscored_metrics = unscored_metrics
        self.verbose = verbose

        # Setup directories
        ensure_dir(self.bottleneck_dir)
        ensure_dir(self.checkpoint_dir)

    def analyze_trace(
        self,
        trace_path: str,
        bottleneck_rules: BottleneckRules,
        view_types: List[ViewType],
        exclude_bottlenecks: List[str] = [],
        exclude_characteristics: List[str] = [],
        logical_view_types: bool = False,
        percentile: Optional[float] = None,
        threshold: Optional[int] = None,
        time_view_type: Optional[ViewType] = None,
        unoverlapped_posix_only: Optional[bool] = False,
    ) -> AnalyzerResultType:
        # Check if both percentile and threshold are none
        if percentile is None and threshold is None:
            raise ValueError('Either percentile or threshold must be defined')
        is_slope_based = threshold is not None

        # Check if high-level metrics are checkpointed
        hlm_view_types = list(sorted(view_types))
        hlm_checkpoint_name = self.get_hlm_checkpoint_name(view_types=hlm_view_types)
        traces = None
        time_sliced_traces = None
        raw_stats = None
        if not self.checkpoint or not self.has_checkpoint(name=hlm_checkpoint_name):
            # Read trace & stats
            traces = self.read_trace(trace_path=trace_path)
            # return traces
            raw_stats = self.read_stats(traces=traces)
            traces = self.postread_trace(traces=traces, view_types=hlm_view_types)
            # return traces
            # time_sliced_traces = traces
            time_sliced_traces = traces.map_partitions(
                split_duration_records_vectorized,
                time_granularity=self.time_granularity / 1e6,
                time_resolution=1e6,
            )
        else:
            # Restore stats
            raw_stats = self.restore_extra_data(
                name=self.get_stats_checkpoint_name(),
                fallback=lambda: None,
            )

        # Compute high-level metrics
        hlm = self.compute_high_level_metrics(
            checkpoint_name=hlm_checkpoint_name,
            traces=time_sliced_traces,
            view_types=hlm_view_types,
        )
        (hlm, raw_stats) = persist(hlm, raw_stats)
        wait([hlm, raw_stats])

        # Validate time granularity
        self.validate_time_granularity(hlm=hlm, view_types=hlm_view_types)

        # Compute layers & views
        hlms = {}
        main_views = {}
        main_indexes = {}
        views = {}
        bottlenecks = {}
        for layer, layer_condition in self.layer_defs.items():
            layer_hlm = hlm.copy()
            if layer_condition:
                layer_hlm = hlm.query(layer_condition)
            layer_main_view = self.compute_main_view(
                layer=layer,
                hlm=layer_hlm,
                view_types=view_types,
            )
            layer_main_index = layer_main_view.index.to_frame().reset_index(drop=True)
            layer_views = self.compute_views(
                layer=layer,
                main_view=layer_main_view,
                view_types=view_types,
                percentile=percentile,
                threshold=threshold,
                is_slope_based=is_slope_based,
            )
            if logical_view_types:
                layer_logical_views = self.compute_logical_views(
                    layer=layer,
                    main_view=layer_main_view,
                    views=layer_views,
                    view_types=view_types,
                    percentile=percentile,
                    threshold=threshold,
                    is_slope_based=is_slope_based,
                )
                layer_views.update(layer_logical_views)
            # layer_bottlenecks = self.detect_bottlenecks(
            #     views=layer_views,
            #     metrics=metrics[layer],
            #     metric_boundaries=layer_metric_boundaries,
            #     is_slope_based=is_slope_based,
            #     percentile=percentile,
            #     threshold=threshold,
            # )
            hlms[layer] = layer_hlm
            main_views[layer] = layer_main_view
            main_indexes[layer] = layer_main_index
            views[layer] = layer_views
            # bottlenecks[layer] = layer_bottlenecks

        (views, raw_stats) = compute(views, raw_stats)

        time_boundary_cols = {
            view_key: [] for layer in views for view_key in views[layer]
        }

        flat_views = {}
        for layer in views:
            for view_key in views[layer]:
                view = views[layer][view_key]
                view.columns = view.columns.map(lambda col: layer.lower() + '_' + col)
                if view_key in flat_views:
                    flat_views[view_key] = flat_views[view_key].merge(
                        view,
                        how='outer',
                        left_index=True,
                        right_index=True,
                    )
                else:
                    flat_views[view_key] = view
                time_boundary_col = f"{layer.lower()}_time_sum"
                if layer.lower() not in self.root_layers:
                    time_boundary_col = f"d_{time_boundary_col}"
                time_boundary_cols[view_key].append(time_boundary_col)

        flat_views = {
            view_key: set_metric_diffs(
                flat_views[view_key],
                layer_deps=self.layer_deps,
            )
            for view_key in flat_views
        }

        time_boundaries = {}
        for view_key in flat_views:
            view_type = view_key[-1]
            if view_type == COL_PROC_NAME or self.is_logical_view_of(
                view_key=view_key,
                parent_view_type=COL_PROC_NAME,
            ):
                time_boundaries[view_key] = (
                    flat_views[view_key][time_boundary_cols[view_key]].max().sum()
                )
            else:
                time_boundaries[view_key] = (
                    flat_views[view_key][time_boundary_cols[view_key]].sum().sum()
                )

        flat_views = {
            view_key: set_metric_scores(
                self.set_additional_metrics(
                    set_metrics(
                        flat_views[view_key],
                        layer_defs=self.layer_defs,
                        layer_deps=self.layer_deps,
                        time_boundary=time_boundaries[view_key],
                    )
                ),
                unscored_metrics=self.unscored_metrics,
            )
            for view_key in flat_views
        }

        for view_key in flat_views:
            view_file_name = '_'.join(list(view_key))
            flat_views[view_key].to_csv(f"{self.checkpoint_dir}/{view_file_name}.csv")

        return (
            traces,
            time_sliced_traces,
            raw_stats,
            hlms,
            main_views,
            time_boundaries,
            views,
            flat_views,
        )

        characteristics = {}
        for layer in self.layer_defs:
            characteristics_view = main_views[layer]
            if time_view_type:
                characteristics_view = views[layer][(time_view_type,)]
            if layer != Layer.APP and unoverlapped_posix_only:
                characteristics_view = main_views[Layer.APP]
                if time_view_type:
                    characteristics_view = views[Layer.APP][(time_view_type,)]
                characteristics_view = characteristics_view.query('u_io_time > 0')
            characteristics[layer] = compute_characteristics(
                layer=layer,
                view=characteristics_view,
                raw_stats=raw_stats,
                app_characteristics=characteristics.get(Layer.APP, None),
                exclude_characteristics=exclude_characteristics,
                unoverlapped_posix_only=unoverlapped_posix_only,
            )

        (main_indexes, bottlenecks, characteristics) = persist(
            main_indexes,
            bottlenecks,
            characteristics,
        )
        wait([main_indexes, bottlenecks, characteristics])

        (bottlenecks,) = persist(
            reason_bottlenecks(
                bottlenecks=bottlenecks,
                exclude_bottlenecks=exclude_bottlenecks,
                main_indexes=main_indexes,
                rules=bottleneck_rules,
                view_types=view_types,
            )
        )
        wait(bottlenecks)

        flat_bottlenecks = self.flatten_bottlenecks(bottlenecks=bottlenecks)

        # Return result
        return AnalyzerResultType(
            bottleneck_dir=self.bottleneck_dir,
            bottleneck_rules=bottleneck_rules,
            bottlenecks=bottlenecks,
            characteristics=characteristics,
            flat_bottlenecks=flat_bottlenecks,
            layers=self.layer_defs.keys(),
            main_indexes=main_indexes,
            main_views=main_views,
            metric_boundaries={},
            raw_stats=raw_stats,
            traces=traces,
            view_types=hlm_view_types,
            views=views,
        )

    def set_additional_metrics(self, view: pd.DataFrame) -> pd.DataFrame:
        for metric, eval_condition in self.additional_metrics.items():
            view = view.eval(f"{metric} = {eval_condition}")
            numerator_denominators = extract_numerator_and_denominators(eval_condition)
            if numerator_denominators:
                numerator, denominators = numerator_denominators
                if denominators:
                    denominator_conditions = [
                        f"({denom}.isna() | {denom} == 0)" for denom in denominators
                    ]
                    mask_condition = " & ".join(denominator_conditions)
                    view[metric] = view[metric].mask(view.eval(mask_condition), pd.NA)
        return view

    def read_stats(self, traces: dd.DataFrame) -> RawStats:
        job_time = self.compute_job_time(traces=traces)
        total_count = self.compute_total_count(traces=traces)
        raw_stats = RawStats(
            **self.restore_extra_data(
                name=self.get_stats_checkpoint_name(),
                fallback=lambda: dict(
                    job_time=job_time,
                    time_granularity=self.time_granularity,
                    total_count=total_count,
                ),
            )
        )
        return raw_stats

    @abc.abstractmethod
    def read_trace(self, trace_path: str) -> dd.DataFrame:
        raise NotImplementedError

    def postread_trace(
        self,
        traces: dd.DataFrame,
        view_types: List[ViewType],
    ) -> dd.DataFrame:
        return traces

    def compute_job_time(self, traces: dd.DataFrame) -> float:
        return traces['tend'].max() - traces['tstart'].min()

    def compute_total_count(self, traces: dd.DataFrame) -> int:
        return traces.index.count().persist()

    @event_logger(key=EventType.COMPUTE_HLM, message='Compute high-level metrics')
    def compute_high_level_metrics(
        self,
        traces: dd.DataFrame,
        view_types: List[ViewType],
        partition_size: str = '128MB',
        checkpoint_name: Optional[str] = None,
    ) -> dd.DataFrame:
        checkpoint_name = checkpoint_name or self.get_hlm_checkpoint_name(view_types)
        return self.restore_view(
            name=checkpoint_name,
            fallback=lambda: self._compute_high_level_metrics(
                partition_size=partition_size,
                traces=traces,
                view_types=view_types,
            ),
        )

    @event_logger(key=EventType.COMPUTE_MAIN_VIEW, message='Compute main view')
    def compute_main_view(
        self,
        layer: Layer,
        hlm: dd.DataFrame,
        view_types: List[ViewType],
        partition_size: str = '128MB',
    ) -> dd.DataFrame:
        return self.restore_view(
            name=self.get_checkpoint_name(
                CHECKPOINT_MAIN_VIEW,
                str(layer),
                *sorted(view_types),
                str(int(self.time_granularity)),
                VERSION_MAIN_VIEW + VERSION_HLM,
            ),
            fallback=lambda: self._compute_main_view(
                hlm=hlm,
                layer=layer,
                partition_size=partition_size,
                view_types=view_types,
            ),
        )

    @event_logger(
        key=EventType.COMPUTE_METRIC_BOUNDARIES, message='Compute metric boundaries'
    )
    def compute_metric_boundaries(
        self,
        layer: Layer,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        view_types: List[ViewType],
    ) -> Dict[Metric, dd.core.Scalar]:
        return self.restore_extra_data(
            name=self.get_checkpoint_name(
                CHECKPOINT_METRIC_BOUNDARIES,
                str(layer),
                *sorted(metrics),
                *sorted(view_types),
                str(int(self.time_granularity)),
                VERSION_METRIC_BOUNDARIES + VERSION_MAIN_VIEW + VERSION_HLM,
            ),
            fallback=lambda: self._compute_metric_boundaries(
                layer=layer,
                main_view=main_view,
                metrics=metrics,
                view_types=view_types,
            ),
        )

    def compute_views(
        self,
        layer: Layer,
        main_view: dd.DataFrame,
        view_types: List[ViewType],
        percentile: Optional[float],
        threshold: Optional[int],
        is_slope_based: bool,
    ) -> Views:
        views = {}
        for view_key in self.view_permutations(view_types=view_types):
            view_type = view_key[-1]
            parent_view_key = view_key[:-1]
            parent_records = main_view
            for parent_view_type in parent_view_key:
                parent_records = parent_records.query(
                    f"{parent_view_type} in @indices",
                    local_dict={'indices': views[(parent_view_type,)].index},
                )
            views[view_key] = self.compute_view(
                is_slope_based=is_slope_based,
                layer=layer,
                records=parent_records,
                view_key=view_key,
                view_type=view_type,
            )
        return views

    def compute_logical_views(
        self,
        layer: Layer,
        main_view: dd.DataFrame,
        views: Dict[ViewKey, dd.DataFrame],
        view_types: List[ViewType],
        percentile: Optional[float],
        threshold: Optional[int],
        is_slope_based: bool,
    ):
        logical_views = {}
        for parent_view_type in self.logical_views:
            parent_view_key = (parent_view_type,)
            if parent_view_key not in views:
                continue
            for view_type in self.logical_views[parent_view_type]:
                view_key = (parent_view_type, view_type)
                parent_records = main_view
                for parent_view_type in parent_view_key:
                    parent_records = parent_records.query(
                        f"{parent_view_type} in @indices",
                        local_dict={'indices': views[(parent_view_type,)].index},
                    )
                view_condition = self.logical_views[parent_view_type][view_type]
                if view_condition is None:
                    if view_type == 'file_dir':
                        parent_records = parent_records.map_partitions(set_file_dir)
                    elif view_type == 'file_pattern':
                        parent_records = parent_records.map_partitions(set_file_pattern)
                    else:
                        raise ValueError("XXX")
                else:
                    parent_records = parent_records.eval(
                        f"{view_type} = {view_condition}"
                    )
                logical_views[view_key] = self.compute_view(
                    is_slope_based=is_slope_based,
                    layer=layer,
                    records=parent_records,
                    view_key=view_key,
                    view_type=view_type,
                )
        return logical_views

        # for view_key in LOGICAL_VIEW_TYPES:
        #     view_type = view_key[-1]
        #     parent_view_key = view_key[:-1]
        #     if parent_view_key not in views:
        #         continue
        #     parent_records = main_view
        #     for parent_view_type in parent_view_key:
        #         parent_records = parent_records.query(
        #             f"{parent_view_type} in @indices",
        #             local_dict={'indices': views[(parent_view_type,)].index},
        #         )
        #     if view_type not in parent_records.columns:
        #         parent_records = self._set_logical_columns(
        #             view=parent_records,
        #             view_types=[parent_view_type],
        #         )
        #     if view_type not in parent_records.columns:
        #         # todo(izzet): log warning
        #         continue
        #     parent_view_type = parent_view_key[-1]
        #     views[view_key] = self.compute_view(
        #         is_slope_based=is_slope_based,
        #         layer=layer,
        #         metric_boundaries=metric_boundaries[parent_view_type],
        #         metrics=metrics,
        #         records=parent_records,
        #         view_key=view_key,
        #         view_type=view_type,
        #     )
        # return views

        # for metric in metrics:
        #     for view_key in LOGICAL_VIEW_TYPES:
        #         view_type = view_key[-1]
        #         parent_view_key = view_key[:-1]
        #         parent_view_type = parent_view_key[0]

        #         if parent_view_type not in view_types:
        #             continue

        #         parent_view_result = view_results[metric].get(parent_view_key, None)
        #         parent_records = (
        #             main_view
        #             if parent_view_result is None
        #             else parent_view_result.records
        #         )

        #         if view_type not in parent_records.columns:
        #             parent_records = self._set_logical_columns(
        #                 view=parent_records,
        #                 view_types=[parent_view_type],
        #             )

        #         view_result = self.compute_view(
        #             metrics=metrics,
        #             metric=metric,
        #             metric_boundary=metric_boundaries[metric],
        #             records=parent_records,
        #             percentile=percentile,
        #             threshold=threshold,
        #             view_key=view_key,
        #             view_type=view_type,
        #         )

        #         view_results[metric][view_key] = view_result

        # return view_results

    @event_logger(key=EventType.COMPUTE_VIEW, message='Compute view')
    def compute_view(
        self,
        layer: Layer,
        view_key: ViewKey,
        view_type: str,
        records: dd.DataFrame,
        is_slope_based: bool,
    ) -> dd.DataFrame:
        return self.restore_view(
            name=self.get_checkpoint_name(
                CHECKPOINT_VIEW,
                str(layer),
                *list(view_key),
                str(int(self.time_granularity)),
                VERSION_VIEWS + VERSION_MAIN_VIEW + VERSION_HLM,
            ),
            fallback=lambda: self._compute_view(
                is_slope_based=is_slope_based,
                layer=layer,
                records=records,
                view_type=view_type,
            ),
            read_from_disk=False,
            write_to_disk=CHECKPOINT_VIEWS,
        )

    @event_logger(key=EventType.DETECT_BOTTLENECKS, message='Detect bottlenecks')
    def detect_bottlenecks(
        self,
        views: Views,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        is_slope_based: bool,
        percentile: Optional[float],
        threshold: Optional[int],
    ) -> Bottlenecks:
        bottlenecks = {}
        for view_key, view in views.items():
            view_type = view_key[-1]
            bottlenecks[view_key] = {}
            view = view.map_partitions(
                set_metric_scores,
                metrics=metrics,
                metric_boundaries=metric_boundaries[view_type],
                is_slope_based=is_slope_based,
            )
            for metric in metrics:
                bottlenecks[view_key][metric] = self.evaluate_view(
                    view=view,
                    metric=metric,
                    percentile=percentile,
                    threshold=threshold,
                    is_slope_based=is_slope_based,
                )
        return bottlenecks

    # @event_logger(key=EventType.EVALUATE_VIEW, message='Evaluate view')
    def evaluate_view(
        self,
        view: dd.DataFrame,
        metric: Metric,
        percentile: Optional[float],
        threshold: Optional[int],
        is_slope_based: bool,
    ):
        metric_col = KNOWN_METRICS[metric].metric_column(slope=is_slope_based)
        if is_slope_based:
            corrected_threshold = THRESHOLD_FUNCTIONS[metric](threshold)
            return view.query(
                f"{metric_col} <= @threshold",
                local_dict={'threshold': corrected_threshold},
            )
        return view.query(
            f"{metric_col} >= @percentile",
            local_dict={'percentile': percentile},
        )

    @staticmethod
    def flatten_bottlenecks(bottlenecks: Dict[Layer, Bottlenecks]) -> dd.DataFrame:
        flattened_bottlenecks = []
        for layer in bottlenecks:
            for view_key in bottlenecks[layer]:
                for metric in bottlenecks[layer][view_key]:
                    view = bottlenecks[layer][view_key][metric].copy()
                    view['layer'] = layer
                    view['metric'] = metric
                    view['view_depth'] = len(view_key)
                    view['view_name'] = format_view_name(view_key, '.')
                    view.index = view.index.rename('subject')
                    view = view.reset_index()
                    view['subject'] = view['subject'].astype(str)
                    flattened_bottlenecks.append(view)
        return dd.concat(flattened_bottlenecks)

    def get_checkpoint_name(self, *args):
        checkpoint_name = "_".join(args).lower()
        if HASH_CHECKPOINT_NAMES:
            return hashlib.md5(checkpoint_name.encode("utf-8")).hexdigest()
        return checkpoint_name

    def get_checkpoint_path(self, name: str):
        return f"{self.checkpoint_dir}/{name}"

    def get_hlm_checkpoint_name(self, view_types: List[ViewType]) -> str:
        return self.get_checkpoint_name(
            CHECKPOINT_HLM,
            *sorted(view_types),
            str(int(self.time_granularity)),
            VERSION_HLM,
        )

    def get_stats_checkpoint_name(self):
        return self.get_checkpoint_name(
            CHECKPOINT_RAW_STATS,
            VERSION_STATS,
            str(int(self.time_granularity)),
        )

    def has_checkpoint(self, name: str):
        checkpoint_path = self.get_checkpoint_path(name=name)
        return os.path.exists(f"{checkpoint_path}/_metadata")

    def is_logical_view_of(self, view_key: ViewKey, parent_view_type: ViewType) -> bool:
        if len(view_key) == 2:
            return view_key[1] in self.logical_views[parent_view_type]
        return False

    def read_bottlenecks(self):
        return dd.read_parquet(self.bottleneck_dir)

    def restore_extra_data(
        self,
        name: str,
        fallback: Callable[[], dict],
        force=False,
    ):
        if self.checkpoint:
            data_path = f"{self.get_checkpoint_path(name=name)}.json"
            if force or not os.path.exists(data_path):
                data = fallback()
                fire_and_forget(
                    get_client().submit(
                        self.store_extra_data,
                        data=get_client().submit(compute, data),
                        data_path=data_path,
                    )
                )
                return data
            with open(data_path, "r") as f:
                return json.load(f)
        return fallback()

    def restore_view(
        self,
        name: str,
        fallback: Callable[[], dd.DataFrame],
        force=False,
        write_to_disk=True,
        read_from_disk=True,
    ):
        if self.checkpoint:
            view_path = self.get_checkpoint_path(name=name)
            if force or not self.has_checkpoint(name=name):
                view = fallback()
                if not write_to_disk:
                    return view
                self.store_view(name=name, view=view)
                if not read_from_disk:
                    return view
                get_client().cancel(view)
            return dd.read_parquet(view_path)
        return fallback()

    def save_bottlenecks(self, bottlenecks: dd.DataFrame, partition_size='64MB'):
        return bottlenecks.repartition(partition_size=partition_size).to_parquet(
            self.bottleneck_dir, compute=True, write_metadata_file=True
        )

    def set_layer_metrics(self, layer: Layer, hlm: dd.DataFrame) -> dd.DataFrame:
        for metric, mask_value in self.derived_metrics[layer].items():
            mask_condition = mask_value[0]
            mask_col = mask_value[1]
            # metric_dtype = 'double[pyarrow]'
            hlm[metric] = 0.0
            # if (
            #     mask_col in ['count', 'size']
            #     or mask_col.endswith('_max')
            #     or mask_col.endswith('_min')
            # ):
            #     hlm[metric] = 0
            #     metric_dtype = 'uint64[pyarrow]'
            hlm[metric] = hlm[metric].mask(hlm.eval(mask_condition), hlm[mask_col])
            # hlm[metric] = hlm[metric].astype(metric_dtype)
        return hlm

    @staticmethod
    def store_extra_data(data: Tuple[Dict], data_path: str):
        with open(data_path, "w") as f:
            return json.dump(data[0], f, cls=NpEncoder)

    def store_view(
        self, name: str, view: dd.DataFrame, compute=True, partition_size='64MB'
    ):
        return view.repartition(partition_size=partition_size).to_parquet(
            self.get_checkpoint_path(name=name),
            compute=compute,
            write_metadata_file=True,
        )

    def validate_time_granularity(self, hlm: dd.DataFrame, view_types: List[ViewType]):
        if 'io_time' in hlm.columns:
            max_io_time = hlm.groupby(view_types)['io_time'].sum().max().compute()
            if max_io_time > (self.time_granularity / 1e6):
                raise ValueError(
                    f"The max 'io_time' exceeds the 'time_granularity' '{int(self.time_granularity / 1e6)}e6'. "
                    f"Please adjust the 'time_granularity' to '{int(2 * max_io_time)}e6' and rerun the analyzer."
                )

    @staticmethod
    def view_permutations(view_types: List[ViewType]):
        if not VIEW_PERMUTATIONS:
            return it.permutations(view_types, 1)

        def _iter_permutations(r: int):
            return it.permutations(view_types, r + 1)

        return it.chain.from_iterable(map(_iter_permutations, range(len(view_types))))

    def write_bottlenecks(
        self,
        flat_bottlenecks: dd.DataFrame,
        compute=True,
        write_metadata_file=True,
    ):
        flat_bottlenecks.to_parquet(
            self.bottleneck_dir,
            compute=compute,
            write_metadata_file=write_metadata_file,
        )

    def _compute_categories(self, hlm: dd.DataFrame) -> List[str]:
        return list(hlm[COL_CATEGORY].unique())

    def _compute_high_level_metrics(
        self,
        traces: dd.DataFrame,
        view_types: list,
        partition_size: str,
    ) -> dd.DataFrame:
        # Add layer columns
        groupby = list(set(view_types).union(HLM_EXTRA_COLS))
        # Build agg_dict
        agg_dict = copy.deepcopy(HLM_AGG)
        for view_type in VIEW_TYPES:
            if view_type in traces.columns:
                agg_dict[view_type] = [unique_set()]
        hlm = (
            traces.groupby(groupby)
            .agg(agg_dict, split_out=math.ceil(math.sqrt(traces.npartitions)))
            .persist()
            .repartition(partition_size=partition_size)
        )
        hlm = flatten_column_names(hlm)
        return hlm.persist()

    def _compute_main_view(
        self,
        layer: Layer,
        hlm: dd.DataFrame,
        view_types: List[ViewType],
        partition_size: str,
    ) -> dd.DataFrame:
        # Set derived columns depending on layer
        hlm_with_metrics = self.set_layer_metrics(layer=layer, hlm=hlm.copy()).drop(
            columns=HLM_EXTRA_COLS, errors='ignore'
        )
        agg_dict = {col: sum for col in hlm_with_metrics.columns}
        for agg_col in agg_dict:
            if agg_col.endswith('_unique'):
                agg_dict[agg_col] = unique_set_flatten()
        # Set groupby
        groupby = list(view_types)
        # Compute agg_view
        main_view = hlm_with_metrics.groupby(groupby).agg(
            agg_dict, split_out=hlm_with_metrics.npartitions
        )
        # Return main_view
        return main_view.persist()

    def _compute_metric_boundaries(
        self,
        layer: Layer,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        view_types: List[ViewType],
    ) -> Dict[Metric, dd.core.Scalar]:
        metric_boundaries = {}
        for view_type in view_types:
            metric_boundaries[view_type] = {}
            for metric in metrics:
                metric_boundaries[view_type][metric] = None
                if is_metric_time_bound(metric):
                    time_col = metric_time_column(metric)
                    metric_boundaries[view_type][metric] = (
                        compute_time_boundaries(
                            view=main_view,
                            view_type=view_type,
                        )[time_col]
                        .sum()
                        .persist()
                    )
                elif metric in ['intensity', 'io_compute_per']:
                    pass
                else:
                    raise NotImplementedError(
                        f"Metric boundary method not found for: {metric}"
                    )
        # print('metric_boundaries:', metric_boundaries)
        return metric_boundaries

    def _compute_view(
        self,
        layer: Layer,
        records: dd.DataFrame,
        view_type: str,
        is_slope_based: bool,
    ) -> dd.DataFrame:
        view_types = records.index._meta.names

        non_proc_agg_dict = self._get_agg_dict(
            layer=layer,
            for_view_type=view_type,
            view_columns=records.columns,
            view_types=view_types,
            is_proc=False,
        )
        proc_agg_dict = self._get_agg_dict(
            layer=layer,
            for_view_type=view_type,
            view_columns=records.columns,
            view_types=view_types,
            is_proc=True,
        )

        # Check view type
        if view_type is not COL_PROC_NAME and COL_PROC_NAME in view_types:
            # if layer in self.threaded_layers:
            #     # thread_agg_dict = self._get_agg_dict(
            #     #     layer=layer,
            #     #     for_view_type=view_type,
            #     #     view_columns=records.columns,
            #     #     view_types=view_types,
            #     #     is_thread=True,
            #     # )
            #     view = (
            #         records.map_partitions(set_proc_name_parts)
            #         .reset_index()
            #         .groupby([view_type, COL_PROC_ID])
            #         .agg(non_proc_agg_dict)
            #         .groupby([view_type])
            #         .max()
            #         .map_partitions(set_unique_counts, layer=layer)
            #     )
            # else:
            view = (
                records.reset_index()
                .groupby([view_type, COL_PROC_NAME])
                .agg(non_proc_agg_dict)
                .groupby([view_type])
                .agg(proc_agg_dict)
                .map_partitions(set_unique_counts, layer=layer)
            )
        else:
            view = (
                records.reset_index()
                .groupby([view_type])
                .agg(non_proc_agg_dict)
                .map_partitions(set_unique_counts, layer=layer)
            )

        # Return view
        return view

    def _get_agg_dict(
        self,
        layer: Layer,
        for_view_type: ViewType,
        view_columns: List[str],
        view_types: List[ViewType],
        is_proc=False,
        is_thread=False,
    ):
        if is_thread:
            agg_dict = {col: max for col in view_columns}
        elif is_proc:
            agg_dict = {col: max if 'time' in col else sum for col in view_columns}
        else:
            agg_dict = {col: sum for col in view_columns}

        # agg_dict['bw'] = max
        # agg_dict['intensity'] = max
        # agg_dict['iops'] = max

        for agg_col in agg_dict:
            if agg_col.endswith('_max'):
                agg_dict[agg_col] = 'max'
            elif agg_col.endswith('_min'):
                agg_dict[agg_col] = 'min'
            elif agg_col.endswith('_mean'):
                agg_dict[agg_col] = 'mean'
            elif agg_col.endswith('_unique'):
                agg_dict[agg_col] = unique_set_flatten()

        unwanted_agg_cols = ['id', for_view_type]
        for agg_col in unwanted_agg_cols:
            if agg_col in agg_dict:
                agg_dict.pop(agg_col)

        return agg_dict

    @staticmethod
    def _wait_all(tasks: Union[dd.DataFrame, Delayed, dict]):
        if WAIT_ENABLED:
            if isinstance(tasks, dd.DataFrame):
                _ = wait(tasks)
            else:
                all_tasks, _ = unpack_collections(tasks)
                _ = wait(all_tasks)
