import abc
import dask.dataframe as dd
import hashlib
import itertools as it
import json
import logging
import math
import os
from dask.base import compute, unpack_collections
from dask.delayed import Delayed
from dask.distributed import fire_and_forget, get_client, wait
from typing import Callable, Dict, List, Optional, Tuple, Union

from .analysis import THRESHOLD_FUNCTIONS, set_metric_slope
from .analysis_utils import set_file_dir, set_file_pattern, set_proc_name_parts
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
    EVENT_DET_BOT,
    EVENT_SAVE_BOT,
    EVENT_SAVE_VIEWS,
    LOGICAL_VIEW_TYPES,
    AccessPattern,
    IOCategory,
)
from .rule_engine import RuleEngine
from .scoring import ViewEvaluator
from .types import (
    AnalysisAccuracy,
    AnalyzerResultType,
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
CHECKPOINT_RAW_STATS = '_raw_stats'
CHECKPOINT_VIEW = '_view'
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
    # 'bw': max,
    'count': sum,
    'data_count': sum,
    # 'intensity': max,
    # 'iops': max,
    'size': sum,
    'time': sum,
}
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
        exclude_bottlenecks: List[str] = [],
        exclude_characteristics: List[str] = [],
        logical_view_types: bool = False,
        metrics: List[Metric] = ['iops'],
        percentile: Optional[float] = None,
        threshold: Optional[int] = None,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ) -> AnalyzerResultType:
        # Check if both percentile and threshold are none
        if percentile is None and threshold is None:
            raise ValueError('Either percentile or threshold must be defined')

        # Read trace & stats
        traces = self.read_trace(trace_path=trace_path)
        raw_stats = self.read_stats(traces=traces)
        traces = self.postread_trace(traces=traces)

        # Create checkpoint names
        main_view_name = self.get_checkpoint_name(
            CHECKPOINT_MAIN_VIEW, *sorted(view_types)
        )

        # Check there is a checkpointed main view
        if not self.checkpoint or not self.has_checkpoint(name=main_view_name):
            # Compute high-level metrics
            with EventLogger(key=EVENT_COMP_HLM, message='Compute high-level metrics'):
                hlm = self.restore_view(
                    name=self.get_checkpoint_name(CHECKPOINT_HLM, *sorted(view_types)),
                    fallback=lambda: self.compute_high_level_metrics(
                        traces=traces,
                        view_types=view_types,
                    ),
                )
                hlm = hlm.persist()
                self._wait_all(tasks=hlm)

        # Compute main view
        with EventLogger(key=EVENT_COMP_MAIN_VIEW, message='Compute main view'):
            main_view = self.restore_view(
                name=main_view_name,
                fallback=lambda: self.compute_main_view(
                    hlm=hlm,
                    view_types=view_types,
                ),
            )
            # TODO remove dropped columns
            main_view = main_view.drop(
                columns=['bw', 'intensity', 'iops', 'att_perf'], errors='ignore'
            ).persist()
            self._wait_all(tasks=main_view)

        # return traces, main_view

        # Compute upper bounds
        with EventLogger(key=EVENT_COMP_METBD, message='Compute metric boundaries'):
            metric_boundaries = self.restore_extra_data(
                name=self.get_checkpoint_name(
                    CHECKPOINT_METRIC_BOUNDARIES,
                    *sorted(metrics),
                    *sorted(view_types),
                ),
                fallback=lambda: self.compute_metric_boundaries(
                    main_view=main_view,
                    metrics=metrics,
                    view_types=view_types,
                ),
            )
            self._wait_all(tasks=metric_boundaries)

        # Compute views
        with EventLogger(key=EVENT_COMP_PERS, message='Compute perspectives'):
            view_results = self.compute_views(
                main_view=main_view,
                metric_boundaries=metric_boundaries,
                metrics=metrics,
                percentile=percentile,
                threshold=threshold,
                view_types=view_types,
            )
            if logical_view_types:
                logical_view_results = self.compute_logical_views(
                    main_view=main_view,
                    metric_boundaries=metric_boundaries,
                    metrics=metrics,
                    percentile=percentile,
                    threshold=threshold,
                    view_results=view_results,
                    view_types=view_types,
                )
                view_results.update(logical_view_results)
            self._wait_all(tasks=view_results)

        if self.checkpoint:
            with EventLogger(
                key=EVENT_SAVE_VIEWS, message='Checkpoint views', level=logging.DEBUG
            ):
                view_checkpoint_tasks = []
                for metric, views in view_results.items():
                    for view_key, view_result in views.items():
                        view_checkpoint_name = self.get_checkpoint_name(
                            CHECKPOINT_VIEW, metric, *list(view_key)
                        )
                        if not self.has_checkpoint(name=view_checkpoint_name):
                            view_checkpoint_tasks.append(
                                self.store_view(
                                    name=view_checkpoint_name,
                                    view=view_result.view,
                                    compute=True,
                                )
                            )
                self._wait_all(tasks=view_checkpoint_tasks)

        # Evaluate views
        view_evaluator = ViewEvaluator()
        with EventLogger(key=EVENT_DET_BOT, message='Detect I/O bottlenecks'):
            evaluated_views = view_evaluator.evaluate_views(
                metric_boundaries=metric_boundaries,
                metrics=metrics,
                view_results=view_results,
            )
            self._wait_all(tasks=evaluated_views)

        # Execute rules
        rule_engine = RuleEngine(rules=[], raw_stats=raw_stats, verbose=self.verbose)
        with EventLogger(
            key=EVENT_ATT_REASONS, message='Attach reasons to I/O bottlenecks'
        ):
            characteristics = rule_engine.process_characteristics(
                exclude_characteristics=exclude_characteristics,
                main_view=main_view,
                view_results=view_results,
            )
            bottlenecks, bottleneck_rules = rule_engine.process_bottlenecks(
                evaluated_views=evaluated_views,
                exclude_bottlenecks=exclude_bottlenecks,
                metric_boundaries=metric_boundaries,
            )
            self._wait_all(tasks=bottlenecks)

        with EventLogger(
            key=EVENT_SAVE_BOT, message='Save I/O bottlenecks', level=logging.DEBUG
        ):
            self.save_bottlenecks(bottlenecks=bottlenecks)

        # Return result
        return AnalyzerResultType(
            bottleneck_dir=self.bottleneck_dir,
            bottleneck_rules=bottleneck_rules,
            characteristics=characteristics,
            evaluated_views=evaluated_views,
            main_view=main_view,
            metric_boundaries=metric_boundaries,
            raw_stats=raw_stats,
            view_results=view_results,
        )

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

    def read_trace(self, trace_path: str) -> dd.DataFrame:
        raise NotImplementedError

    def postread_trace(self, traces: dd.DataFrame) -> dd.DataFrame:
        return traces

    def compute_job_time(self, traces: dd.DataFrame) -> float:
        return traces['tend'].max() - traces['tstart'].min()

    def compute_total_count(self, traces: dd.DataFrame) -> int:
        return traces.index.count().persist()

    def compute_high_level_metrics(
        self,
        traces: dd.DataFrame,
        view_types: list,
        partition_size: str = '256MB',
    ) -> dd.DataFrame:
        # Add `io_cat`, `acc_pat`, and `func_id` to groupby
        groupby = list(view_types)
        groupby.extend(EXTRA_COLS)

        # Compute high-level metrics
        # hlm = traces \
        #     .groupby(groupby) \
        #     .agg(HLM_AGG, split_out=traces.npartitions) \
        #     .persist() \
        #     .reset_index() \
        #     .repartition(partition_size) \
        #     .persist()
        # hlm = traces \
        #     .groupby(groupby) \
        #     .agg(HLM_AGG, split_out=8) \
        #     .reset_index() \
        #     .persist()
        hlm = (
            traces.groupby(groupby)
            .agg(HLM_AGG, split_out=math.ceil(math.sqrt(traces.npartitions)))
            .persist()
            .reset_index()
            .repartition(partition_size=partition_size)
        )
        hlm = flatten_column_names(hlm)
        return hlm.rename(columns=HLM_COLS).persist()

    def compute_main_view(
        self,
        hlm: dd.DataFrame,
        view_types: List[ViewType],
        partition_size: str = '256MB',
    ) -> dd.DataFrame:
        # Set derived columns
        hlm = self._set_derived_columns(ddf=hlm)
        # Compute agg_view
        main_view = (
            hlm.drop(columns=EXTRA_COLS)
            .groupby(list(view_types))
            .sum(split_out=hlm.npartitions)
        )
        # main_view = hlm \
        #     .drop(columns=EXTRA_COLS) \
        #     .groupby(view_types) \
        #     .sum() \
        #     .persist() \
        #     .repartition(partition_size)
        # Set hashed ids
        # main_view['id'] = main_view.index.map(set_id)
        main_view['id'] = main_view.index.map(hash)
        # Return main_view
        return main_view.persist()

    def compute_metric_boundaries(
        self,
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

    def compute_views(
        self,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, dd.core.Scalar],
        percentile: Optional[float],
        threshold: Optional[int],
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

                parent_view_result = view_results[metric].get(parent_view_key, None)
                parent_records = (
                    main_view
                    if parent_view_result is None
                    else parent_view_result.records
                )

                view_result = self.compute_view(
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    percentile=percentile,
                    records=parent_records,
                    threshold=threshold,
                    view_key=view_key,
                    view_type=view_type,
                )

                view_results[metric][view_key] = view_result

        return view_results

    def compute_logical_views(
        self,
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

    def compute_view(
        self,
        metric: str,
        metric_boundary: dd.core.Scalar,
        percentile: Optional[float],
        records: dd.DataFrame,
        threshold: Optional[int],
        view_key: ViewKey,
        view_type: str,
    ) -> ViewResult:
        # Restore view
        view = self.restore_view(
            name=self.get_checkpoint_name(CHECKPOINT_VIEW, metric, *list(view_key)),
            fallback=lambda: self._compute_view(
                records=records,
                view_type=view_type,
                metric=metric,
                metric_boundary=metric_boundary,
            ),
            write_to_disk=False,
        )

        # Filter by slope
        critical_view = view
        if percentile is not None and percentile > 0:
            critical_view = view.query(
                f"{metric}_pth >= @percentile",
                local_dict={'percentile': percentile},
            ).persist()
        elif threshold is not None and threshold > 0:
            corrected_threshold = THRESHOLD_FUNCTIONS[metric](threshold)
            critical_view = view.query(
                f"{metric}_slope <= @threshold",
                local_dict={'threshold': corrected_threshold},
            ).persist()

        indices = critical_view.index.unique()

        # Find filtered records
        records = records.query(
            f"{view_type} in @indices", local_dict={'indices': indices}
        ).persist()

        # Return views & normalization data
        return ViewResult(
            critical_view=critical_view,
            metric=metric,
            records=records,
            view=view,
            view_type=view_type,
        )

    def get_checkpoint_name(self, *args):
        checkpoint_name = "_".join(args)
        if self.verbose:
            return checkpoint_name
        return hashlib.md5(checkpoint_name.encode("utf-8")).hexdigest()

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
            with open(data_path, 'r') as f:
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

    @staticmethod
    def store_extra_data(data: Tuple[Dict], data_path: str):
        with open(data_path, 'w') as f:
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

    def _compute_view(
        self,
        records: dd.DataFrame,
        view_type: str,
        metric: str,
        metric_boundary: dd.core.Scalar,
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
        view = view.map_partitions(set_metric_slope, metric=metric)

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

    def _set_derived_columns(self, ddf: dd.DataFrame):
        # Derive `io_cat` columns
        for col in ['time', 'size', 'count']:
            for io_cat in list(IOCategory):
                col_name = f"{io_cat.name.lower()}_{col}"
                ddf[col_name] = 0.0 if col == 'time' else 0
                ddf[col_name] = ddf[col_name].mask(
                    ddf['io_cat'] == io_cat.value, ddf[col]
                )
        for io_cat in list(IOCategory):
            min_name, max_name = (
                f"{io_cat.name.lower()}_min",
                f"{io_cat.name.lower()}_max",
            )
            ddf[min_name] = 0
            ddf[max_name] = 0
            ddf[min_name] = ddf[min_name].mask(
                ddf['io_cat'] == io_cat.value, ddf['size_min']
            )
            ddf[max_name] = ddf[max_name].mask(
                ddf['io_cat'] == io_cat.value, ddf['size_max']
            )
        # Derive `data` columns
        ddf['data_count'] = ddf['write_count'] + ddf['read_count']
        ddf['data_size'] = ddf['write_size'] + ddf['read_size']
        ddf['data_time'] = ddf['write_time'] + ddf['read_time']
        # Derive `acc_pat` columns
        for col_suffix, col_value in zip(
            ACC_PAT_SUFFIXES, ['data_time', 'data_size', 'data_count']
        ):
            for acc_pat in list(AccessPattern):
                col_name = f"{acc_pat.name.lower()}_{col_suffix}"
                ddf[col_name] = 0.0 if col_suffix == 'time' else 0
                ddf[col_name] = ddf[col_name].mask(
                    ddf['acc_pat'] == acc_pat.value, ddf[col_value]
                )
        # Derive metadata operation columns
        for col in ['time', 'count']:
            for md_op in DERIVED_MD_OPS:
                col_name = f"{md_op}_{col}"
                ddf[col_name] = 0.0 if col == 'time' else 0
                if md_op in ['close', 'open']:
                    ddf[col_name] = ddf[col_name].mask(
                        ddf['func_id'].str.contains(md_op)
                        & ~ddf['func_id'].str.contains('dir'),
                        ddf[col],
                    )
                else:
                    ddf[col_name] = ddf[col_name].mask(
                        ddf['func_id'].str.contains(md_op), ddf[col]
                    )
        # Return ddf
        return ddf

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
