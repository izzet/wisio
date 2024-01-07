import abc
import dask.dataframe as dd
import itertools as it
import json
import logging
import os
from dask.base import compute, unpack_collections
from dask.distributed import fire_and_forget, get_client, wait
from typing import Callable, Dict, List, Tuple

from .analysis import set_bound_columns, set_metric_deltas, set_metric_slope
from .analysis_utils import set_file_dir, set_file_pattern, set_proc_name_parts
from .analyzer_result import AnalysisResult
from .cluster_management import ClusterConfig, ClusterManager
from .constants import (
    ACC_PAT_SUFFIXES,
    COL_FILE_NAME,
    COL_PROC_NAME,
    DERIVED_MD_OPS,
    EVENT_ATT_REASONS,
    EVENT_COMP_HLM,
    EVENT_COMP_MAIN_VIEW,
    EVENT_COMP_METBD,
    EVENT_COMP_PERS,
    EVENT_DET_BOTT,
    LOGICAL_VIEW_TYPES,
    VIEW_TYPES,
    AccessPattern,
    IOCategory,
)
from .rule_engine import RuleEngine
from .scoring import ViewEvaluator
from .types import (
    AnalysisAccuracy,
    AnalysisSetup,
    Metric,
    RawStats,
    ViewKey,
    ViewResult,
    ViewType,
)
from .utils.dask_utils import EventLogger, flatten_column_names
from .utils.file_utils import ensure_dir
from .utils.json_encoders import NpEncoder


CHECKPOINT_MAIN_VIEW = '_main_view'
CHECKPOINT_METRIC_BOUNDARIES = '_metric_boundaries'
CHECKPOINT_HLM = '_hlm'
EXTRA_COLS = ['io_cat', 'acc_pat', 'func_id']
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
VIEW_AGG = {
    'bw': max,
    'count': sum,
    'data_count': sum,
    'intensity': max,
    'iops': max,
    'size': sum,
    'time': sum,
}


class Analyzer(abc.ABC):

    def __init__(
        self,
        name: str,
        working_dir: str,
        checkpoint: bool = False,
        checkpoint_dir: str = '',
        cluster_config: ClusterConfig = None,
        debug=False,
        verbose=False,
    ):
        if checkpoint:
            assert checkpoint_dir != '', 'Checkpoint directory must be defined'

        self.checkpoint = checkpoint
        self.checkpoint_dir = checkpoint_dir
        self.cluster_config = cluster_config
        self.debug = debug
        self.name = name
        self.verbose = verbose

        # Setup logging
        ensure_dir(working_dir)
        setup_logging(
            filename=f"{working_dir}/{name.lower()}_analyzer.log", debug=debug)
        logging.info(f"Initializing {name} analyzer")

        # Init cluster manager
        self.cluster_manager = ClusterManager(
            working_dir=working_dir, config=cluster_config)

        # Boot cluster
        self.cluster_manager.boot()

    def analyze_traces(
        self,
        traces: dd.DataFrame,
        metrics: List[Metric],
        raw_stats: RawStats,
        accuracy: AnalysisAccuracy = 'pessimistic',
        metric_threshold: float = 0.5,
        slope_threshold: int = 45,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ):
        # Compute high-level metrics
        with EventLogger(key=EVENT_COMP_HLM, message='Compute high-level metrics'):
            hlm = self.load_view(
                view_name='_'.join([CHECKPOINT_HLM, *sorted(view_types)]),
                fallback=lambda: self.compute_high_level_metrics(
                    traces=traces,
                    view_types=view_types,
                ),
                force=False,
                persist=True,
            )
            wait(hlm)

        # Compute main view
        with EventLogger(key=EVENT_COMP_MAIN_VIEW, message='Compute main view'):
            main_view = self.load_view(
                view_name='_'.join([CHECKPOINT_MAIN_VIEW, *sorted(view_types)]),
                fallback=lambda: self.compute_main_view(
                    hlm=hlm,
                    view_types=view_types,
                ),
                force=False,
                persist=True,
            )
            wait(main_view)

        # Compute upper bounds
        with EventLogger(key=EVENT_COMP_METBD, message='Compute metric boundaries'):
            metric_boundaries = self.restore_extra_data(
                data_name='_'.join([CHECKPOINT_METRIC_BOUNDARIES, *sorted(metrics), *sorted(view_types)]),
                fallback=lambda: self.compute_metric_boundaries(
                    main_view=main_view,
                    metrics=metrics,
                ),
            )
            metric_boundaries_tasks, _ = unpack_collections(metric_boundaries)
            wait(metric_boundaries_tasks)

        # Compute views
        with EventLogger(key=EVENT_COMP_PERS, message='Compute perspectives'):
            view_results = self.compute_views(
                main_view=main_view,
                metrics=metrics,
                metric_boundaries=metric_boundaries,
                slope_threshold=slope_threshold,
                view_types=view_types,
            )
            logical_view_results = self.compute_logical_views(
                main_view=main_view,
                metrics=metrics,
                metric_boundaries=metric_boundaries,
                slope_threshold=slope_threshold,
                view_types=view_types,
                view_results=view_results,
            )
            view_tasks, _ = unpack_collections(view_results)
            logical_view_tasks, _ = unpack_collections(logical_view_results)
            view_tasks.extend(logical_view_tasks)
            wait(view_tasks)
            view_results.update(logical_view_results)

        # Evaluate views
        view_evaluator = ViewEvaluator()
        with EventLogger(key=EVENT_DET_BOTT, message='Detect I/O bottlenecks'):
            evaluated_views = view_evaluator.evaluate_views(
                metric_boundaries=metric_boundaries,
                metrics=metrics,
                threshold=metric_threshold,
                view_results=view_results,
            )
            evaluated_view_tasks, _ = unpack_collections(evaluated_views)
            wait(evaluated_view_tasks)

        # Execute rules
        rule_engine = RuleEngine(rules=[], raw_stats=raw_stats, verbose=self.verbose)
        with EventLogger(key=EVENT_ATT_REASONS, message='Attach reasons to I/O bottlenecks'):
            characteristics = rule_engine.process_characteristics(
                main_view=main_view
            )
            bottlenecks = rule_engine.process_bottlenecks(
                evaluated_views=evaluated_views,
                characteristics=characteristics,
                metric_boundaries=metric_boundaries,
            )

        # Create result
        result = AnalysisResult(
            analysis_setup=AnalysisSetup(
                accuracy=accuracy,
                checkpoint=self.checkpoint,
                cluster_type=self.cluster_config.cluster_type,
                debug=self.debug,
                memory=self.cluster_config.memory,
                metric_threshold=metric_threshold,
                num_threads_per_worker=self.cluster_config.n_threads_per_worker,
                num_workers=self.cluster_config.n_workers,
                processes=self.cluster_config.processes,
                slope_threshold=slope_threshold,
                verbose=self.verbose,
            ),
            bottlenecks=bottlenecks,
            characteristics=characteristics,
            evaluated_views=evaluated_views,
            main_view=main_view,
            metric_boundaries=metric_boundaries,
            raw_stats=raw_stats,
            view_results=view_results,
        )

        # Return result
        return result

    def compute_high_level_metrics(
        self,
        traces: dd.DataFrame,
        view_types: list,
        partition_size: str = '256MB',
    ) -> dd.DataFrame:
        # Add `io_cat`, `acc_pat`, and `func_id` to groupby
        groupby = view_types.copy()
        groupby.extend(EXTRA_COLS)
        # Compute high-level metrics
        hlm = traces \
            .groupby(groupby) \
            .agg(HLM_AGG, split_out=traces.npartitions) \
            .reset_index() \
            .repartition(partition_size) \
            .persist()
        hlm = flatten_column_names(hlm)
        return hlm.rename(columns=HLM_COLS)

    def compute_main_view(
        self,
        hlm: dd.DataFrame,
        view_types: list,
    ) -> dd.DataFrame:
        # Set derived columns
        hlm = self._set_derived_columns(ddf=hlm)
        # Compute agg_view
        main_view = hlm \
            .drop(columns=EXTRA_COLS) \
            .groupby(view_types) \
            .sum(split_out=hlm.npartitions) \
            .persist()
        # Set hashed ids
        # main_view['id'] = main_view.index.map(set_id)
        main_view['id'] = main_view.index.map(hash)
        # Return main_view
        return main_view

    def compute_metric_boundaries(
        self,
        main_view: dd.DataFrame,
        metrics: List[Metric],
    ) -> Dict[Metric, dd.core.Scalar]:
        metric_boundaries = {}
        for metric in metrics:
            metric_boundary = None
            if metric == 'time':
                metric_boundary = main_view \
                    .groupby([COL_PROC_NAME]) \
                    .sum()[metric] \
                    .max() \
                    .persist()
            elif metric == 'bw':

                pass
            metric_boundaries[metric] = metric_boundary
        return metric_boundaries

    def compute_views(
        self,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        slope_threshold: int,
        view_types: List[ViewType],
    ):
        # Keep view results
        view_results = {}

        # Compute multifaceted views for each metric
        for metric in metrics:

            view_results[metric] = {}

            for view_key in self.view_permutations(view_types=view_types):

                view_type = view_key[-1]
                parent_view_key = view_key[:-1]

                parent_view_result = view_results[metric].get(
                    parent_view_key, None)
                parent_view = main_view if parent_view_result is None else parent_view_result.view

                view_result = self.compute_view(
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    parent_view=parent_view,
                    slope_threshold=slope_threshold,
                    view_key=view_key,
                    view_type=view_type,
                )

                view_results[metric][view_key] = view_result

        return view_results

    def compute_logical_views(
        self,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        slope_threshold: int,
        view_types: List[ViewType],
        view_results: Dict[Metric, Dict[ViewKey, ViewResult]],
    ):

        for metric in metrics:

            for view_key in LOGICAL_VIEW_TYPES:

                view_type = view_key[-1]
                parent_view_key = view_key[:-1]
                parent_view_type = parent_view_key[0]

                if parent_view_type not in view_types:
                    continue

                parent_view_result = view_results[metric].get(
                    parent_view_key, None)
                parent_view = main_view if parent_view_result is None else parent_view_result.view

                if view_type not in parent_view.columns:
                    parent_view = self._set_logical_columns(
                        view=parent_view,
                        view_types=[parent_view_type],
                    )

                view_result = self.compute_view(
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    parent_view=parent_view,
                    slope_threshold=slope_threshold,
                    view_key=view_key,
                    view_type=view_type,
                )

                view_results[metric][view_key] = view_result

        return view_results

    def compute_view(
        self,
        parent_view: dd.DataFrame,
        view_key: ViewKey,
        view_type: str,
        metric: str,
        metric_boundary: dd.core.Scalar,
        slope_threshold=45,
    ) -> ViewResult:
        # Compute slope view
        slope_view = self._compute_slope_view(
            metric=metric,
            metric_boundary=metric_boundary,
            parent_view=parent_view,
            view_type=view_type,
        )

        # Create columns
        # query_col = f"{metric}_norm" if IS_NORMALIZED[metric] else f"{metric}_delta"
        slope_col = f"{metric}_slope"

        # Filter by slope
        filtered_view = slope_view
        if slope_threshold > 0:
            filtered_view = slope_view \
                .query(f"{slope_col} < {slope_threshold}")

        indices = filtered_view.index.unique()

        # Find filtered records
        view = parent_view \
            .query(f"{view_type} in @indices", local_dict={'indices': indices}) \
            .persist()

        # Return views & normalization data
        return ViewResult(
            group_view=filtered_view,
            metric=metric,
            slope_view=slope_view,
            view=view,
            view_type=view_type,
        )

    def load_view(self, view_name: str, fallback: Callable[[], dd.DataFrame], force=False):
        if self.checkpoint:
            view_path = f"{self.checkpoint_dir}/{view_name}"
            if force or not os.path.exists(f"{view_path}/_metadata"):
                view = fallback()
                view.to_parquet(f"{view_path}", compute=True,
                                write_metadata_file=True)
                self.cluster_manager.client.cancel(view)
            return dd.read_parquet(f"{view_path}")
        return fallback()

    def restore_extra_data(self, data_name: str, fallback: Callable[[], dict], force=False, persist=False):
        if self.checkpoint:
            data_path = f"{self.checkpoint_dir}/{data_name}.json"
            if force or not os.path.exists(data_path):
                data = fallback()
                fire_and_forget(
                    get_client().submit(
                        self.store_extra_data,
                        data=get_client().submit(compute, data),
                        data_path=data_path
                    )
                )
                return data
            with open(data_path, 'r') as f:
                return json.load(f)
        return fallback()

    @staticmethod
    def store_extra_data(data: Tuple[Dict], data_path: str):
        with open(data_path, 'w') as f:
            return json.dump(data[0], f, cls=NpEncoder)

    @staticmethod
    def view_permutations(view_types: List[ViewType]):
        def _iter_permutations(r: int):
            return it.permutations(view_types, r + 1)
        return it.chain.from_iterable(map(_iter_permutations, range(len(view_types))))

    def _compute_slope_view(
        self,
        parent_view: dd.DataFrame,
        view_type: str,
        metric: str,
        metric_boundary: dd.core.Scalar,
    ) -> dd.DataFrame:
        non_proc_agg_dict = self._get_agg_dict(
            view_columns=parent_view.columns,
            is_proc=False,
        )
        proc_agg_dict = self._get_agg_dict(
            view_columns=parent_view.columns,
            is_proc=True,
        )

        # Check view type
        if view_type is not COL_PROC_NAME:
            # Compute proc view first
            slope_view = parent_view \
                .groupby([view_type, COL_PROC_NAME]) \
                .agg(non_proc_agg_dict) \
                .map_partitions(set_bound_columns) \
                .groupby([view_type]) \
                .agg(proc_agg_dict)
        else:
            # Compute slope view
            slope_view = parent_view \
                .groupby([view_type]) \
                .agg(non_proc_agg_dict) \
                .map_partitions(set_bound_columns)

        # Set metric deltas & slope
        slope_view = slope_view \
            .sort_values(metric, ascending=False) \
            .map_partitions(set_metric_deltas, metric=metric, metric_boundary=metric_boundary) \
            .map_partitions(set_metric_slope, metric=metric)

        # Return slope view
        return slope_view

    @staticmethod
    def _get_agg_dict(view_columns: list, is_proc=False):
        if is_proc:
            agg_dict = {col: max if any(
                x in col for x in 'duration time'.split()) else sum for col in view_columns}
        else:
            agg_dict = {col: sum for col in view_columns}
        agg_dict.pop('id')
        agg_dict['size_min'] = min
        agg_dict['size_max'] = max
        for view_type in VIEW_TYPES:
            if view_type in agg_dict:
                agg_dict.pop(view_type)
        for _, view_type in LOGICAL_VIEW_TYPES:
            if view_type in agg_dict:
                agg_dict.pop(view_type)
        return agg_dict

    def _set_derived_columns(self, ddf: dd.DataFrame):
        # Derive `io_cat` columns
        for col in ['time', 'size', 'count']:
            for io_cat in list(IOCategory):
                col_name = f"{io_cat.name.lower()}_{col}"
                ddf[col_name] = 0.0 if col == 'time' else 0
                ddf[col_name] = ddf[col_name].mask(
                    ddf['io_cat'] == io_cat.value, ddf[col])
        for io_cat in list(IOCategory):
            min_name, max_name = f"{io_cat.name.lower()}_min", f"{io_cat.name.lower()}_max"
            ddf[min_name] = 0
            ddf[max_name] = 0
            ddf[min_name] = ddf[min_name].mask(
                ddf['io_cat'] == io_cat.value, ddf['size_min'])
            ddf[max_name] = ddf[max_name].mask(
                ddf['io_cat'] == io_cat.value, ddf['size_max'])
        # Derive `data` columns
        ddf['data_count'] = ddf['write_count'] + ddf['read_count']
        ddf['data_size'] = ddf['write_size'] + ddf['read_size']
        ddf['data_time'] = ddf['write_time'] + ddf['read_time']
        # Derive `acc_pat` columns
        for col_suffix, col_value in zip(ACC_PAT_SUFFIXES, ['data_time', 'data_size', 'data_count']):
            for acc_pat in list(AccessPattern):
                col_name = f"{acc_pat.name.lower()}_{col_suffix}"
                ddf[col_name] = 0.0 if col_suffix == 'time' else 0
                ddf[col_name] = ddf[col_name].mask(
                    ddf['acc_pat'] == acc_pat.value, ddf[col_value])
        # Derive metadata operation columns
        for col in ['time', 'count']:
            for md_op in DERIVED_MD_OPS:
                col_name = f"{md_op}_{col}"
                ddf[col_name] = 0.0 if col == 'time' else 0
                if md_op in ['close', 'open']:
                    ddf[col_name] = ddf[col_name].mask(ddf['func_id'].str.contains(
                        md_op) & ~ddf['func_id'].str.contains('dir'), ddf[col])
                else:
                    ddf[col_name] = ddf[col_name].mask(
                        ddf['func_id'].str.contains(md_op), ddf[col])
        # Set bound columns
        set_bound_columns(ddf=ddf, is_initial=True)
        # Return ddf
        return ddf

    def _set_logical_columns(self, view: dd.DataFrame, view_types: List[ViewType]) -> dd.DataFrame:
        # Check if view types include `proc_name`
        if COL_PROC_NAME in view_types:
            view = view.map_partitions(set_proc_name_parts)

        # Check if view types include `file_name`
        if COL_FILE_NAME in view_types:
            view = view \
                .map_partitions(set_file_dir) \
                .map_partitions(set_file_pattern)

        return view
