import abc
import dask.dataframe as dd
import hashlib
import itertools as it
import json
import logging
import math
import os
from dask import compute, persist
from dask.base import unpack_collections
from dask.delayed import Delayed
from dask.distributed import fire_and_forget, get_client, wait
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .analysis import THRESHOLD_FUNCTIONS, set_metrics, set_metric_scores
from .analysis_utils import set_file_dir, set_file_pattern, set_proc_name_parts
from .constants import (
    ACC_PAT_SUFFIXES,
    COL_CATEGORY,
    COL_FILE_NAME,
    COL_PROC_NAME,
    DERIVED_MD_OPS,
    LOGICAL_VIEW_TYPES,
    AccessPattern,
    EventType,
    IOCategory,
    Layer,
)
from .metrics import KNOWN_METRICS
from .rule_engine import RuleEngine, compute_characteristics
from .types import (
    AnalysisAccuracy,
    AnalyzerResultType,
    BottleneckResults,
    Characteristics,
    Metric,
    RawStats,
    ViewKey,
    ViewResult,
    ViewResults,
    ViewResultsPerMetricPerView,
    ViewType,
)
from .utils.dask_utils import EventLogger, event_logger, flatten_column_names
from .utils.file_utils import ensure_dir
from .utils.json_encoders import NpEncoder


CHECKPOINT_MAIN_VIEW = '_main_view'
CHECKPOINT_METRIC_BOUNDARIES = '_metric_boundaries'
CHECKPOINT_HLM = '_hlm'
CHECKPOINT_RAW_STATS = '_raw_stats'
CHECKPOINT_VIEW = '_view'
HLM_AGG = {
    'time': [sum],
    'count': [sum],
    'size': [min, max, sum],
}
HLM_COLS = {
    'count_sum': 'count',
    'size_sum': 'size',
    'time_sum': 'time',
}
HLM_EXTRA_COLS = ['cat', 'io_cat', 'acc_pat', 'func_id']
VIEW_AGG = {
    # 'bw': max,
    'count': sum,
    'data_count': sum,
    # 'intensity': max,
    # 'iops': max,
    'size': sum,
    'time': sum,
}
HASH_CHECKPOINT_NAMES = False
WAIT_ENABLED = True


class Analyzer(abc.ABC):
    def __init__(
        self,
        bottleneck_dir: str = "",
        checkpoint: bool = True,
        checkpoint_dir: str = "",
        debug: bool = False,
        time_approximate: bool = True,
        time_granularity: float = 1e6,
        verbose: bool = False,
    ):
        if checkpoint:
            assert checkpoint_dir != '', 'Checkpoint directory must be defined'

        self.bottleneck_dir = bottleneck_dir
        self.checkpoint = checkpoint
        self.checkpoint_dir = checkpoint_dir
        self.debug = debug
        self.time_approximate = time_approximate
        self.time_granularity = time_granularity
        self.verbose = verbose

        # Setup directories
        ensure_dir(self.bottleneck_dir)
        ensure_dir(self.checkpoint_dir)

    def analyze_trace(
        self,
        trace_path: str,
        accuracy: AnalysisAccuracy = 'pessimistic',
        app_metrics: List[Metric] = ['io_compute_ratio'],
        app_view_types: List[ViewType] = ['time_range', 'proc_name', 'step'],
        exclude_bottlenecks: List[str] = [],
        exclude_characteristics: List[str] = [],
        logical_view_types: bool = False,
        percentile: Optional[float] = None,
        posix_metrics: List[Metric] = ['iops'],
        posix_view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
        threshold: Optional[int] = None,
    ) -> AnalyzerResultType:
        # Check if both percentile and threshold are none
        if percentile is None and threshold is None:
            raise ValueError('Either percentile or threshold must be defined')
        is_slope_based = threshold is not None

        # Read trace & stats
        traces = self.read_trace(trace_path=trace_path)
        raw_stats = self.read_stats(traces=traces)
        traces = self.postread_trace(traces=traces)

        layers = self.compute_layers(traces=traces)

        # Compute high-level metrics
        hlm_view_types = list(set(posix_view_types + app_view_types))
        hlm = self.compute_high_level_metrics(traces=traces, view_types=hlm_view_types)
        wait(hlm)

        # Compute layers & views
        hlms = {}
        main_views = {}
        characteristics = {}
        metric_boundaries = {}
        views = {}
        bottlenecks = {}
        for layer, categories in self.arrange_layers(layers).items():
            # print(f'Processing layer: {layer}')
            # print(f'Categories: {categories}')
            metrics = posix_metrics if layer == Layer.POSIX else app_metrics
            view_types = posix_view_types if layer == Layer.POSIX else app_view_types
            l_hlm = hlm  # [hlm[COL_CATEGORY].isin(categories)]
            l_main_view = self.compute_main_view(
                layer=layer,
                hlm=l_hlm,
                view_types=view_types,
            )
            # l_characteristics = compute_characteristics(
            #     main_view=l_main_view,
            #     raw_stats=raw_stats,
            #     exclude_characteristics=exclude_characteristics,
            # )
            l_metric_boundaries = self.compute_metric_boundaries(
                layer=layer,
                main_view=l_main_view,
                metrics=metrics,
                view_types=view_types,
            )
            l_views = self.compute_views(
                layer=layer,
                main_view=l_main_view,
                view_types=view_types,
                metric_boundaries=l_metric_boundaries,
                metrics=metrics,
                percentile=percentile,
                threshold=threshold,
                is_slope_based=is_slope_based,
            )
            l_bottlenecks = self.detect_bottlenecks(
                views=l_views,
                metrics=metrics,
                metric_boundaries=l_metric_boundaries,
                is_slope_based=is_slope_based,
                percentile=percentile,
                threshold=threshold,
            )
            hlms[layer] = l_hlm
            main_views[layer] = l_main_view
            # characteristics[layer] = l_characteristics
            metric_boundaries[layer] = l_metric_boundaries
            views[layer] = l_views
            bottlenecks[layer] = l_bottlenecks
            break

        return (
            traces,
            hlm,
            hlms,
            main_views,
            metric_boundaries,
            views,
            bottlenecks,
        )

        # Return result
        # return AnalyzerResultType(
        #     bottleneck_dir=self.bottleneck_dir,
        #     bottleneck_rules=bottleneck_rules,
        #     characteristics=characteristics,
        #     evaluated_views=evaluated_views,
        #     main_view=main_view,
        #     metric_boundaries=metric_boundaries,
        #     raw_stats=raw_stats,
        #     view_results=view_results,
        # )

    def arrange_layers(self, layers: List[str]) -> Dict[str, List[str]]:
        arranged_layers = {}
        # arranged_layers[Layer.POSIX] = filter(
        #     lambda x: x == 'POSIX' or x == 'STDIO', layers
        # )
        # arranged_layers[Layer.MPI] = filter(
        #     lambda x: x == 'MPI' or x == 'MPIIO', layers
        # )
        # arranged_layers[Layer.HDF5] = filter(lambda x: x == 'HDF5', layers)
        # arranged_layers[Layer.APP] = filter(
        #     lambda x: x != 'POSIX'
        #     and x != 'STDIO'
        #     and x != 'MPI'
        #     and x != 'MPIIO'
        #     and x != 'HDF5',
        #     layers,
        # )
        arranged_layers[Layer.APP] = layers
        upper_layers = [layer.upper() for layer in layers]
        if 'NETCDF' in upper_layers:
            arranged_layers[Layer.NETCDF] = filter(
                lambda x: x.upper()
                in ['NETCDF', 'HDF5', 'MPI', 'MPIIO', 'POSIX', 'STDIO'],
                layers,
            )
        if 'PNETCDF' in upper_layers:
            arranged_layers[Layer.PNETCDF] = filter(
                lambda x: x.upper() in ['PNETCDF', 'MPI', 'MPIIO', 'POSIX', 'STDIO'],
                layers,
            )
        if 'HDF5' in upper_layers:
            arranged_layers[Layer.HDF5] = filter(
                lambda x: x.upper() in ['HDF5', 'MPI', 'MPIIO', 'POSIX', 'STDIO'],
                layers,
            )
        if 'MPI' in upper_layers:
            arranged_layers[Layer.MPI] = filter(
                lambda x: x.upper() in ['MPI', 'MPIIO', 'POSIX', 'STDIO'], layers
            )
        if 'POSIX' in upper_layers:
            arranged_layers[Layer.POSIX] = filter(
                lambda x: x.upper() in ['POSIX', 'STDIO'], layers
            )
        for layer, categories in arranged_layers.items():
            arranged_layers[layer] = list(categories)
        return arranged_layers

    def read_stats(self, traces: dd.DataFrame) -> RawStats:
        job_time = self.compute_job_time(traces=traces)
        total_count = self.compute_total_count(traces=traces)
        raw_stats: RawStats = self.restore_extra_data(
            name=self.get_checkpoint_name(CHECKPOINT_RAW_STATS),
            fallback=lambda: dict(
                job_time=job_time,
                time_granularity=self.time_granularity,
                total_count=total_count,
            ),
        )
        return raw_stats

    @abc.abstractmethod
    def read_trace(self, trace_path: str) -> dd.DataFrame:
        raise NotImplementedError

    def postread_trace(self, traces: dd.DataFrame) -> dd.DataFrame:
        return traces

    def additional_high_level_metrics(self) -> Tuple[Dict[str, Any], Dict[str, str]]:
        return {}, {}

    def compute_job_time(self, traces: dd.DataFrame) -> float:
        return traces['tend'].max() - traces['tstart'].min()

    def compute_layers(self, traces: dd.DataFrame) -> List[str]:
        return list(traces[COL_CATEGORY].unique().compute())

    def compute_total_count(self, traces: dd.DataFrame) -> int:
        return traces.index.count().persist()

    @event_logger(key=EventType.COMPUTE_HLM, message='Compute high-level metrics')
    def compute_high_level_metrics(
        self,
        traces: dd.DataFrame,
        view_types: List[ViewType],
        partition_size: str = '128MB',
    ) -> dd.DataFrame:
        return self.restore_view(
            name=self.get_checkpoint_name(CHECKPOINT_HLM, *sorted(view_types)),
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
                CHECKPOINT_MAIN_VIEW, str(layer), *sorted(view_types)
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
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        percentile: Optional[float],
        threshold: Optional[int],
        is_slope_based: bool,
    ) -> ViewResults:
        view_results = {}
        for view_key in self.view_permutations(view_types=view_types):
            view_type = view_key[-1]
            parent_view_key = view_key[:-1]
            parent_records = main_view
            for parent_view_type in parent_view_key:
                parent_records = parent_records.query(
                    f"{parent_view_type} in @indices",
                    local_dict={'indices': view_results[(parent_view_type,)].index},
                )
            view_results[view_key] = self.compute_view(
                is_slope_based=is_slope_based,
                layer=layer,
                metric_boundaries=metric_boundaries,
                metrics=metrics,
                records=parent_records,
                view_key=view_key,
                view_type=view_type,
            )
        return view_results

    def compute_logical_views(
        self,
        layer: Layer,
        main_view: dd.DataFrame,
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        metrics: List[Metric],
        percentile: Optional[float],
        threshold: Optional[int],
        view_results: Dict[Metric, Dict[ViewKey, ViewResult]],
        view_types: List[ViewType],
    ):
        for metric in metrics:
            for view_key in LOGICAL_VIEW_TYPES:
                view_type = view_key[-1]
                parent_view_key = view_key[:-1]
                parent_view_type = parent_view_key[0]

                if parent_view_type not in view_types:
                    continue

                parent_view_result = view_results[metric].get(parent_view_key, None)
                parent_records = (
                    main_view
                    if parent_view_result is None
                    else parent_view_result.records
                )

                if view_type not in parent_records.columns:
                    parent_records = self._set_logical_columns(
                        view=parent_records,
                        view_types=[parent_view_type],
                    )

                view_result = self.compute_view(
                    metrics=metrics,
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    records=parent_records,
                    percentile=percentile,
                    threshold=threshold,
                    view_key=view_key,
                    view_type=view_type,
                )

                view_results[metric][view_key] = view_result

        return view_results

    @event_logger(key=EventType.COMPUTE_VIEW, message='Compute view')
    def compute_view(
        self,
        layer: Layer,
        view_key: ViewKey,
        view_type: str,
        records: dd.DataFrame,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        is_slope_based: bool,
    ) -> dd.DataFrame:
        return self.restore_view(
            name=self.get_checkpoint_name(CHECKPOINT_VIEW, str(layer), *list(view_key)),
            fallback=lambda: self._compute_view(
                is_slope_based=is_slope_based,
                layer=layer,
                metric_boundaries=metric_boundaries,
                metrics=metrics,
                records=records,
                view_type=view_type,
            ),
            write_to_disk=False,
        )

    @event_logger(key=EventType.DETECT_BOTTLENECKS, message='Detect bottlenecks')
    def detect_bottlenecks(
        self,
        views: ViewResults,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        is_slope_based: bool,
        percentile: Optional[float],
        threshold: Optional[int],
    ) -> BottleneckResults:
        bottlenecks = {}
        for view_key, view in views.items():
            bottlenecks[view_key] = {}
            view = view.map_partitions(
                set_metric_scores,
                metrics=metrics,
                metric_boundaries=metric_boundaries,
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
        query_col = KNOWN_METRICS[metric].query_column_name(slope=is_slope_based)
        if is_slope_based:
            corrected_threshold = THRESHOLD_FUNCTIONS[metric](threshold)
            return view.query(
                f"{query_col} <= @threshold",
                local_dict={'threshold': corrected_threshold},
            )
        return view.query(
            f"{query_col} >= @percentile",
            local_dict={'percentile': percentile},
        )
        # indices = critical_view.index.unique()
        # records = records.query(
        #     f"{view_type} in @indices",
        #     local_dict={'indices': indices},
        # )
        # return ViewResult(
        #     critical_view=critical_view,
        #     metric=metric,
        #     records=records.persist(),
        #     view=view,
        #     view_type=view_type,
        # )

    def get_checkpoint_name(self, *args):
        checkpoint_name = "_".join(args).lower()
        if HASH_CHECKPOINT_NAMES:
            return hashlib.md5(checkpoint_name.encode("utf-8")).hexdigest()
        return checkpoint_name

    def get_checkpoint_path(self, name: str):
        return f"{self.checkpoint_dir}/{name}"

    def has_checkpoint(self, name: str):
        checkpoint_path = self.get_checkpoint_path(name=name)
        return os.path.exists(f"{checkpoint_path}/_metadata")

    def restore_extra_data(
        self, name: str, fallback: Callable[[], dict], force=False, persist=False
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
    ):
        if self.checkpoint:
            view_path = self.get_checkpoint_path(name=name)
            if force or not self.has_checkpoint(name=name):
                view = fallback()
                if not write_to_disk:
                    return view
                self.store_view(name=name, view=view)
                get_client().cancel(view)
            return dd.read_parquet(view_path)
        return fallback()

    def save_bottlenecks(self, bottlenecks: dd.DataFrame, partition_size='64MB'):
        return bottlenecks.repartition(partition_size=partition_size).to_parquet(
            self.bottleneck_dir, compute=True, write_metadata_file=True
        )

    def set_layer_columns(self, layer: Layer, hlm: dd.DataFrame) -> dd.DataFrame:
        if layer == Layer.POSIX:
            return self._set_posix_columns(hlm=hlm)
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

    @staticmethod
    def view_permutations(view_types: List[ViewType]):
        def _iter_permutations(r: int):
            return it.permutations(view_types, r + 1)

        return it.chain.from_iterable(map(_iter_permutations, range(len(view_types))))

    def _compute_high_level_metrics(
        self,
        traces: dd.DataFrame,
        view_types: list,
        partition_size: str,
    ) -> dd.DataFrame:
        # Add layer columns
        groupby = list(set(view_types).union(HLM_EXTRA_COLS))
        agg_columns, column_names = self.additional_high_level_metrics()
        assert len(agg_columns) == len(column_names), 'Additional columns mismatch'
        agg_columns.update(HLM_AGG)
        hlm = (
            traces.groupby(groupby)
            .agg(agg_columns, split_out=math.ceil(math.sqrt(traces.npartitions)))
            .persist()
            .reset_index()
            .repartition(partition_size=partition_size)
        )
        column_names.update(HLM_COLS)
        hlm = flatten_column_names(hlm).rename(columns=column_names)
        return hlm.persist()

    def _compute_main_view(
        self,
        layer: Layer,
        hlm: dd.DataFrame,
        view_types: List[ViewType],
        partition_size: str,
    ) -> dd.DataFrame:
        # Set derived columns depending on layer
        hlm = self.set_layer_columns(layer=layer, hlm=hlm)
        # Set groupby
        groupby = list(view_types)
        # Compute agg_view
        main_view = (
            hlm.drop(columns=HLM_EXTRA_COLS, errors='ignore')
            .groupby(groupby)
            .sum(split_out=hlm.npartitions)
        )
        # Set hashed ids
        main_view['id'] = main_view.index.map(hash)
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
        for metric in metrics:
            metric_boundary = None
            if metric == 'iops' or metric == 'time':
                if COL_PROC_NAME in view_types:
                    metric_boundary = (
                        main_view.groupby([COL_PROC_NAME]).sum()['time'].max().persist()
                    )
                else:
                    metric_boundary = main_view['time'].sum().persist()
            elif metric == 'bw':
                pass
            metric_boundaries[metric] = metric_boundary
        return metric_boundaries

    def _compute_view(
        self,
        layer: Layer,
        records: dd.DataFrame,
        view_type: str,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        is_slope_based: bool,
    ) -> dd.DataFrame:
        view_types = records.index._meta.names

        non_proc_agg_dict = self._get_agg_dict(
            for_view_type=view_type,
            view_columns=records.columns,
            view_types=view_types,
            is_proc=False,
        )
        proc_agg_dict = self._get_agg_dict(
            for_view_type=view_type,
            view_columns=records.columns,
            view_types=view_types,
            is_proc=True,
        )

        # Check view type
        if view_type is not COL_PROC_NAME and COL_PROC_NAME in view_types:
            view = (
                records.reset_index()
                .groupby([view_type, COL_PROC_NAME])
                .agg(non_proc_agg_dict)
                .groupby([view_type])
                .agg(proc_agg_dict)
            )
        else:
            view = records.reset_index().groupby([view_type]).agg(non_proc_agg_dict)

        # Set metric slope
        view = view.map_partitions(
            set_metrics,
            is_slope_based=is_slope_based,
            metrics=metrics,
            metric_boundaries=metric_boundaries,
        )

        # Return view
        return view

    @staticmethod
    def _get_agg_dict(
        for_view_type: ViewType,
        view_columns: List[str],
        view_types: List[ViewType],
        is_proc=False,
    ):
        if is_proc:
            agg_dict = {col: max if 'time' in col else sum for col in view_columns}
        else:
            agg_dict = {col: sum for col in view_columns}

        # agg_dict['bw'] = max
        # agg_dict['intensity'] = max
        # agg_dict['iops'] = max
        agg_dict['size_min'] = min
        agg_dict['size_max'] = max

        unwanted_agg_cols = ['id', for_view_type]
        for agg_col in unwanted_agg_cols:
            if agg_col in agg_dict:
                agg_dict.pop(agg_col)

        return agg_dict

    def _set_posix_columns(self, hlm: dd.DataFrame) -> dd.DataFrame:
        # Derive `io_cat` columns
        for col in ['time', 'size', 'count']:
            for io_cat in list(IOCategory):
                col_name = f"{io_cat.name.lower()}_{col}"
                hlm[col_name] = 0.0 if col == 'time' else 0
                hlm[col_name] = hlm[col_name].mask(
                    hlm['io_cat'] == io_cat.value, hlm[col]
                )
        for io_cat in list(IOCategory):
            min_name, max_name = (
                f"{io_cat.name.lower()}_min",
                f"{io_cat.name.lower()}_max",
            )
            hlm[min_name] = 0
            hlm[max_name] = 0
            hlm[min_name] = hlm[min_name].mask(
                hlm['io_cat'] == io_cat.value, hlm['size_min']
            )
            hlm[max_name] = hlm[max_name].mask(
                hlm['io_cat'] == io_cat.value, hlm['size_max']
            )
        # Derive `data` columns
        hlm['data_count'] = hlm['write_count'] + hlm['read_count']
        hlm['data_size'] = hlm['write_size'] + hlm['read_size']
        hlm['data_time'] = hlm['write_time'] + hlm['read_time']
        # Derive `acc_pat` columns
        for col_suffix, col_value in zip(
            ACC_PAT_SUFFIXES, ['data_time', 'data_size', 'data_count']
        ):
            for acc_pat in list(AccessPattern):
                col_name = f"{acc_pat.name.lower()}_{col_suffix}"
                hlm[col_name] = 0.0 if col_suffix == 'time' else 0
                hlm[col_name] = hlm[col_name].mask(
                    hlm['acc_pat'] == acc_pat.value, hlm[col_value]
                )
        # Derive metadata operation columns
        for col in ['time', 'count']:
            for md_op in DERIVED_MD_OPS:
                col_name = f"{md_op}_{col}"
                hlm[col_name] = 0.0 if col == 'time' else 0
                if md_op in ['close', 'open']:
                    hlm[col_name] = hlm[col_name].mask(
                        hlm['func_id'].str.contains(md_op)
                        & ~hlm['func_id'].str.contains('dir'),
                        hlm[col],
                    )
                else:
                    hlm[col_name] = hlm[col_name].mask(
                        hlm['func_id'].str.contains(md_op), hlm[col]
                    )
        # Return ddf
        return hlm

    def _set_logical_columns(
        self, view: dd.DataFrame, view_types: List[ViewType]
    ) -> dd.DataFrame:
        # Check if view types include `proc_name`
        if COL_PROC_NAME in view_types:
            view = view.map_partitions(set_proc_name_parts)

        # Check if view types include `file_name`
        if COL_FILE_NAME in view_types:
            view = view.map_partitions(set_file_dir).map_partitions(set_file_pattern)

        return view

    @staticmethod
    def _wait_all(tasks: Union[dd.DataFrame, Delayed, dict]):
        if WAIT_ENABLED:
            if isinstance(tasks, dd.DataFrame):
                _ = wait(tasks)
            else:
                all_tasks, _ = unpack_collections(tasks)
                _ = wait(all_tasks)
