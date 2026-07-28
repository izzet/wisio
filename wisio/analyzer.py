import abc
import dask

dask.config.set({'dataframe.query-planning-warning': False})
import dask.dataframe as dd
import hashlib
import itertools as it
import json
import logging
import math
import os
import pandas as pd
from betterframe import BetterFrame
from dask.base import compute, unpack_collections
from dask.delayed import Delayed, delayed
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
from .utils.dask_utils import (
    EventLogger,
    as_single_dask_partition,
    flatten_column_names,
    repartition_to_size,
)
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
# `compute_main_view` repartitions to this, which is what makes
# `npartitions * PARTITION_SIZE_BYTES` a sound bound on a frame's materialised
# size without asking the scheduler. See `_materialize_if_small`.
PARTITION_SIZE_BYTES = 256 * 1024**2
# Above this, views stay on Dask. Sized so a real trace's main view is not
# pulled into the client, while the tiny ones that dominate runtime are.
VIEW_MATERIALIZE_MAX_BYTES = 64 * 1024**2

HASH_CHECKPOINT_NAMES = False
WAIT_ENABLED = True



def _match_dask_agg_dtypes(view, records, aggregate):
    """Give a pandas aggregation the dtypes Dask would have produced.

    Dask types an aggregation from its *meta*: it runs the same operation over
    a synthetic non-empty frame and takes the resulting schema, which can
    differ from what the real data yields. A `min` over a column that never
    goes null comes back int64 in pandas and float64 through Dask, and the
    reverse happens elsewhere -- same values, different dtype, and so a
    different Parquet schema depending only on how large the trace was.

    Deriving the reference the same way Dask does keeps one schema. Only
    dtypes are touched; values are already identical.

    Args:
        view: The aggregated frame, computed in pandas.
        records: The frame it was aggregated from.
        aggregate: The aggregation, so it can be replayed on a synthetic frame.

    Returns:
        The view, cast to Dask's schema where they disagree.
    """
    if isinstance(view, dd.DataFrame):
        return view
    try:
        from dask.dataframe.utils import meta_nonempty

        reference = aggregate(meta_nonempty(records.iloc[:0]))
    except Exception:
        # Without a reference the honest thing is to leave the frame alone
        # rather than guess at a schema.
        return view
    return view.astype(
        {
            column: dtype
            for column, dtype in reference.dtypes.items()
            if column in view.columns and view.dtypes[column] != dtype
        }
    )


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
        view_materialize_max_bytes: int = VIEW_MATERIALIZE_MAX_BYTES,
    ):
        """Initializes the Analyzer instance.

        Args:
            bottleneck_dir: Directory to save identified bottlenecks.
            checkpoint: Whether to enable checkpointing of intermediate results.
            checkpoint_dir: Directory to store checkpoint data.
            debug: Whether to enable debug mode.
            time_approximate: Whether to use approximate time for I/O operations.
            time_granularity: The time granularity for analysis, in microseconds.
            verbose: Whether to enable verbose logging.
            view_materialize_max_bytes: Main views smaller than this are pulled
                into memory so the view loop runs in pandas. Zero disables it
                and keeps everything on Dask.
        """
        if checkpoint:
            assert checkpoint_dir != '', 'Checkpoint directory must be defined'

        self.bottleneck_dir = bottleneck_dir
        self.checkpoint = checkpoint
        self.checkpoint_dir = checkpoint_dir
        self.debug = debug
        self.time_approximate = time_approximate
        self.time_granularity = time_granularity
        self.verbose = verbose
        self.view_materialize_max_bytes = view_materialize_max_bytes

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
        """Analyzes I/O trace data to identify performance bottlenecks.

        This method orchestrates the entire analysis process, including reading
        trace data, computing various metrics and views, evaluating these views
        to detect bottlenecks, and applying rules to characterize them.

        Args:
            trace_path: Path to the I/O trace file or directory.
            accuracy: The analysis accuracy mode ('optimistic' or 'pessimistic').
            exclude_bottlenecks: A list of bottleneck types to exclude from the analysis.
            exclude_characteristics: A list of I/O characteristics to exclude.
            logical_view_types: Whether to compute views based on logical relationships.
            metrics: A list of metrics to analyze (e.g., 'iops', 'bw', 'time').
            percentile: The percentile to use for identifying critical views.
                        Mutually exclusive with 'threshold'.
            threshold: The threshold value for slope-based bottleneck detection.
                       Mutually exclusive with 'percentile'.
            view_types: A list of view types to compute (e.g., 'file_name', 'proc_name').

        Returns:
            An AnalyzerResultType object containing the analysis results.

        Raises:
            ValueError: If neither 'percentile' nor 'threshold' is defined.
        """
        # Check if both percentile and threshold are none
        if percentile is None and threshold is None:
            raise ValueError('Either percentile or threshold must be defined')
        is_slope_based = threshold is not None

        # Read trace & stats
        traces = self.read_trace(trace_path=trace_path)
        raw_stats = self.read_stats(traces=traces)
        traces = self.postread_trace(traces=traces)

        # Create checkpoint names
        main_view_name = self.get_checkpoint_name(CHECKPOINT_MAIN_VIEW, *sorted(view_types))

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
            main_view = main_view.drop(columns=['bw', 'intensity', 'iops', 'att_perf'], errors='ignore').persist()
            self._wait_all(tasks=main_view)

        # return traces, main_view
        return self._analyze_main_view(
            main_view=main_view,
            metrics=metrics,
            percentile=percentile,
            threshold=threshold,
            view_types=view_types,
            is_slope_based=is_slope_based,
            raw_stats=RawStats(**raw_stats),
            exclude_bottlenecks=exclude_bottlenecks,
            exclude_characteristics=exclude_characteristics,
        )

    def read_stats(self, traces: dd.DataFrame) -> RawStats:
        """Computes and restores raw statistics from the trace data.

        Calculates job time and total event count from the traces.
        It attempts to restore these stats from a checkpoint if available,
        otherwise computes them and checkpoints the result.

        Args:
            traces: A Dask DataFrame containing the I/O trace data.

        Returns:
            A RawStats dictionary containing 'job_time', 'time_granularity',
            and 'total_count'.
        """
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
        """Reads I/O trace data from the specified path.

        This is an abstract method that must be implemented by subclasses
        to handle specific trace formats.

        Args:
            trace_path: Path to the I/O trace file or directory.

        Returns:
            A Dask DataFrame containing the parsed I/O trace data.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError

    def postread_trace(self, traces: dd.DataFrame) -> dd.DataFrame:
        """Performs any post-processing on the raw trace data.

        This method can be overridden by subclasses to perform additional
        transformations or filtering on the trace data after it has been read.
        By default, it returns the traces unmodified.

        Args:
            traces: A Dask DataFrame containing the I/O trace data.

        Returns:
            A Dask DataFrame with any post-processing applied.
        """
        return traces

    def compute_job_time(self, traces: dd.DataFrame) -> float:
        """Computes the total job execution time from the traces.

        Args:
            traces: A Dask DataFrame containing the I/O trace data,
                    expected to have 'tstart' and 'tend' columns.

        Returns:
            The total job time as a float.
        """
        return traces['tend'].max() - traces['tstart'].min()

    def compute_total_count(self, traces: dd.DataFrame) -> int:
        """Computes the total number of I/O events in the traces.

        Args:
            traces: A Dask DataFrame containing the I/O trace data.

        Returns:
            The total count of I/O events as an integer.
        """
        return traces.index.count().persist()

    def compute_high_level_metrics(
        self,
        traces: dd.DataFrame,
        view_types: list,
        partition_size: str = '256MB',
    ) -> dd.DataFrame:
        """Computes high-level metrics by aggregating trace data.

        Groups the trace data by the specified view types and extra columns
        (io_cat, acc_pat, func_id) and aggregates metrics like time, count, and size.

        Args:
            traces: A Dask DataFrame containing the I/O trace data.
            view_types: A list of column names to group by for aggregation.
            partition_size: The desired partition size for the resulting Dask DataFrame.

        Returns:
            A Dask DataFrame containing the computed high-level metrics.
        """
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
        )
        hlm = repartition_to_size(hlm, partition_size)
        hlm = flatten_column_names(hlm)
        return hlm.rename(columns=HLM_COLS).persist()

    def compute_main_view(
        self,
        hlm: dd.DataFrame,
        view_types: List[ViewType],
        partition_size: str = '256MB',
    ) -> dd.DataFrame:
        """Computes the main aggregated view from high-level metrics.

        This method takes the high-level metrics, sets derived columns,
        and then groups by the specified view_types to create a primary
        aggregated view of the I/O performance data.

        Args:
            hlm: A Dask DataFrame containing high-level metrics.
            view_types: A list of view types to group by for the main view.
            partition_size: The desired partition size for the resulting Dask DataFrame.

        Returns:
            A Dask DataFrame representing the main aggregated view.
        """
        # Set derived columns
        hlm = self._set_derived_columns(ddf=hlm)
        hlm_agg = {col: sum for col in hlm.columns if col not in EXTRA_COLS}
        for view_type in view_types:
            if view_type in hlm_agg:
                hlm_agg.pop(view_type)
        # Compute agg_view
        main_view = (
            hlm.groupby(list(view_types))
            .agg(hlm_agg, split_out=hlm.npartitions)
            .persist()
        )
        main_view = repartition_to_size(main_view, partition_size)
        # main_view = hlm \
        #     .drop(columns=EXTRA_COLS) \
        #     .groupby(view_types) \
        #     .sum() \
        #     .persist() \
        #     .repartition(partition_size)
        # Set hashed ids
        # main_view['id'] = main_view.index.map(set_id)
        # main_view['id'] = main_view.index.map(hash)
        # Return main_view
        return main_view

    def compute_metric_boundaries(
        self,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        view_types: List[ViewType],
    ) -> Dict[Metric, "dd.Scalar"]:
        """Computes the upper boundary for each specified metric.

        For metrics like 'iops' or 'time', it calculates the maximum time
        either per process (if 'proc_name' is in view_types) or the total sum.
        Other metrics like 'bw' are currently passed through.

        Args:
            main_view: The main aggregated Dask DataFrame.
            metrics: A list of metrics for which to compute boundaries.
            view_types: A list of view types present in the main_view.

        Returns:
            A dictionary mapping each metric to its computed boundary (a Dask Scalar).
        """
        metric_boundaries = {}
        for metric in metrics:
            metric_boundary = None
            if metric == 'iops' or metric == 'time':
                if COL_PROC_NAME in view_types:
                    metric_boundary = main_view.groupby([COL_PROC_NAME]).sum()['time'].max().persist()
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
        metric_boundaries: Dict[Metric, "dd.Scalar"],
        percentile: Optional[float],
        threshold: Optional[int],
        view_types: List[ViewType],
    ):
        """Computes multifaceted views for each specified metric.

        Iterates through all permutations of view_types for each metric,
        generating different "perspectives" on the data. Each perspective
        is a ViewResult, containing the filtered data and critical items.

        Args:
            main_view: The main aggregated Dask DataFrame.
            metrics: A list of metrics to compute views for.
            metric_boundaries: A dictionary of precomputed metric boundaries.
            percentile: The percentile used to identify critical items in views.
            threshold: The threshold value for slope-based critical item identification.
            view_types: A list of base view types to permute for creating views.

        Returns:
            A dictionary where keys are metrics and values are dictionaries
            mapping ViewKey to ViewResult.
        """
        # Every view descends from the main view, so gating once here covers
        # the whole loop rather than re-deciding per view.
        main_view = self._materialize_if_small(main_view)

        # Keep view results
        view_results = {metric: {} for metric in metrics}

        # `view_permutations` yields keys grouped by increasing length, and a
        # view of length n depends only on its parent of length n-1. So each
        # length is resolved as one batch: every (metric, view_key) in it is
        # independent given the previous one, and their critical indices are
        # gathered in a single `compute` rather than one round-trip per view.
        for _, view_keys in it.groupby(
            self.view_permutations(view_types=view_types), key=len
        ):
            pending = []

            for view_key in list(view_keys):
                view_type = view_key[-1]
                parent_view_key = view_key[:-1]

                for metric in metrics:
                    parent_view_result = view_results[metric].get(parent_view_key, None)
                    parent_records = main_view if parent_view_result is None else parent_view_result.records

                    view, critical_view = self.build_view(
                        metrics=metrics,
                        metric=metric,
                        metric_boundary=metric_boundaries[metric],
                        percentile=percentile,
                        records=parent_records,
                        threshold=threshold,
                        view_key=view_key,
                        view_type=view_type,
                    )

                    pending.append(
                        (metric, view_key, view_type, parent_records, view, critical_view)
                    )

            if not pending:
                continue

            (all_indices,) = compute(
                [critical_view.index.unique() for *_, critical_view in pending]
            )

            for (metric, view_key, view_type, parent_records, view, critical_view), indices in zip(
                pending, all_indices
            ):
                view_results[metric][view_key] = self.finish_view(
                    critical_view=critical_view,
                    indices=indices,
                    metric=metric,
                    records=parent_records,
                    view=view,
                    view_type=view_type,
                )

        return view_results

    def compute_logical_views(
        self,
        main_view: dd.DataFrame,
        metric_boundaries: Dict[Metric, "dd.Scalar"],
        metrics: List[Metric],
        percentile: Optional[float],
        threshold: Optional[int],
        view_results: Dict[Metric, Dict[ViewKey, ViewResult]],
        view_types: List[ViewType],
    ):
        """Computes views based on predefined logical relationships in the data.

        This method extends the existing view_results by adding new views
        derived from logical columns (e.g., file directory from file name).

        Args:
            main_view: The main aggregated Dask DataFrame.
            metric_boundaries: A dictionary of precomputed metric boundaries.
            metrics: A list of metrics to compute logical views for.
            percentile: The percentile used to identify critical items in views.
            threshold: The threshold value for slope-based critical item identification.
            view_results: The existing dictionary of computed views to be updated.
            view_types: A list of base view types available in the main_view.

        Returns:
            The updated view_results dictionary including the computed logical views.
        """
        # Every logical view hangs off an already-computed base view and none
        # depend on each other, so the whole set resolves in one `compute`.
        pending = []

        for metric in metrics:
            for view_key in LOGICAL_VIEW_TYPES:
                view_type = view_key[-1]
                parent_view_key = view_key[:-1]
                parent_view_type = parent_view_key[0]

                if parent_view_type not in view_types:
                    continue

                parent_view_result = view_results[metric].get(parent_view_key, None)
                parent_records = main_view if parent_view_result is None else parent_view_result.records

                if view_type not in parent_records.columns:
                    parent_records = self._set_logical_columns(
                        view=parent_records,
                        view_types=[parent_view_type],
                    )

                view, critical_view = self.build_view(
                    metrics=metrics,
                    metric=metric,
                    metric_boundary=metric_boundaries[metric],
                    records=parent_records,
                    percentile=percentile,
                    threshold=threshold,
                    view_key=view_key,
                    view_type=view_type,
                )

                pending.append(
                    (metric, view_key, view_type, parent_records, view, critical_view)
                )

        if not pending:
            return view_results

        (all_indices,) = compute(
            [critical_view.index.unique() for *_, critical_view in pending]
        )

        for (metric, view_key, view_type, parent_records, view, critical_view), indices in zip(
            pending, all_indices
        ):
            view_results[metric][view_key] = self.finish_view(
                critical_view=critical_view,
                indices=indices,
                metric=metric,
                records=parent_records,
                view=view,
                view_type=view_type,
            )

        return view_results

    def compute_view(
        self,
        metrics: List[Metric],
        metric: Metric,
        metric_boundary: "dd.Scalar",
        percentile: Optional[float],
        records: dd.DataFrame,
        threshold: Optional[int],
        view_key: ViewKey,
        view_type: str,
    ) -> ViewResult:
        """Computes a single view based on the provided parameters.

        This involves restoring a view from a checkpoint or computing it,
        then filtering it to identify critical items based on percentile or threshold.

        Args:
            metrics: The list of all metrics being analyzed.
            metric: The specific metric for this view.
            metric_boundary: The precomputed boundary for the current metric.
            percentile: The percentile to identify critical items.
            records: The Dask DataFrame (parent records) to compute the view from.
            threshold: The threshold for slope-based critical item identification.
            view_key: The key identifying this specific view.
            view_type: The primary dimension/column for this view.

        Returns:
            A ViewResult object containing the computed view, critical items,
            and filtered records.
        """
        view, critical_view = self.build_view(
            metrics=metrics,
            metric=metric,
            metric_boundary=metric_boundary,
            percentile=percentile,
            records=records,
            threshold=threshold,
            view_key=view_key,
            view_type=view_type,
        )

        (indices,) = compute(critical_view.index.unique())

        return self.finish_view(
            critical_view=critical_view,
            indices=indices,
            metric=metric,
            records=records,
            view=view,
            view_type=view_type,
        )

    def build_view(
        self,
        metrics: List[Metric],
        metric: Metric,
        metric_boundary: "dd.Scalar",
        percentile: Optional[float],
        records: dd.DataFrame,
        threshold: Optional[int],
        view_key: ViewKey,
        view_type: str,
    ) -> Tuple[dd.DataFrame, dd.DataFrame]:
        """Builds the lazy half of a view: the view and its critical subset.

        Nothing is computed here. Splitting this off lets callers resolve the
        critical indices of many independent views in a single `compute`
        instead of one blocking round-trip each -- see `compute_views`.

        Args:
            metrics: The list of all metrics being analyzed.
            metric: The specific metric for this view.
            metric_boundary: The precomputed boundary for the current metric.
            percentile: The percentile to identify critical items.
            records: The Dask DataFrame (parent records) to compute the view from.
            threshold: The threshold for slope-based critical item identification.
            view_key: The key identifying this specific view.
            view_type: The primary dimension/column for this view.

        Returns:
            A tuple of the (still lazy) view and its critical view.
        """
        # Restore view
        view = self.restore_view(
            name=self.get_checkpoint_name(CHECKPOINT_VIEW, metric, *list(view_key)),
            fallback=lambda: self._compute_view(
                records=records,
                view_type=view_type,
                metrics=metrics,
                metric=metric,
                metric_boundary=metric_boundary,
            ),
            write_to_disk=False,
        )

        # Filter by slope
        critical_view = self._compute_critical_view(
            view=view,
            metric=metric,
            percentile=percentile,
            threshold=threshold,
        )

        return view, critical_view

    def _materialize_if_small(self, frame):
        """Bring a frame into memory when it is provably small enough.

        The per-view Dask machinery costs seconds per view whatever the size,
        and these frames are tiny: on the dftracer fixture the largest view is
        78 rows and every one is a single partition. Materialising once here
        lets the whole view loop run in pandas.

        The threshold and the fallback bound stay here rather than in
        betterframe: the bound is only sound because `compute_main_view`
        repartitions to `PARTITION_SIZE`, which the library has no way to know.
        The size check never triggers a computation -- `len()` and
        `memory_usage()` would execute the chain on exactly the large frames
        the gate exists to protect.

        Args:
            frame: The main view, Dask or already pandas.

        Returns:
            A pandas frame when it fits, the original otherwise.
        """
        if not self.view_materialize_max_bytes:
            return frame
        return (
            BetterFrame(frame)
            .materialize_if_under(
                self.view_materialize_max_bytes,
                fallback_bound=(
                    frame.npartitions * PARTITION_SIZE_BYTES
                    if isinstance(frame, dd.DataFrame)
                    else None
                ),
            )
            .native
        )

    @staticmethod
    def finish_view(
        critical_view,
        indices,
        metric: Metric,
        records,
        view,
        view_type: str,
    ) -> ViewResult:
        """Applies an already-resolved critical index to the parent records.

        `indices` must be concrete. dask-expr derives a `query`'s meta by
        running the predicate against an empty frame, and pandas' `isin` calls
        `list()` on the operand -- so a lazy operand would be computed during
        graph construction, once per view level.

        Args:
            critical_view: The critical view this index came from.
            indices: The resolved critical index values.
            metric: The specific metric for this view.
            records: The parent records to filter.
            view: The computed view.
            view_type: The primary dimension/column for this view.

        Returns:
            A ViewResult object containing the computed view, critical items,
            and filtered records.
        """
        # Find filtered records. `finalize` persists on Dask and is a no-op on
        # pandas, which is already concrete.
        records = (
            BetterFrame(records)
            .pipe(
                lambda df: df.query(
                    f"{view_type} in @indices", local_dict={'indices': indices}
                )
            )
            .finalize()
        )

        # Return views & normalization data
        return ViewResult(
            critical_view=critical_view,
            metric=metric,
            records=records,
            view=view,
            view_type=view_type,
        )

    def get_checkpoint_name(self, *args) -> str:
        """Generates a standardized name for a checkpoint.

        Joins the provided arguments with underscores. If HASH_CHECKPOINT_NAMES
        is True, it returns an MD5 hash of the name.

        Args:
            *args: String components to form the checkpoint name.

        Returns:
            A string representing the checkpoint name.
        """
        checkpoint_name = "_".join(args)
        if HASH_CHECKPOINT_NAMES:
            return hashlib.md5(checkpoint_name.encode("utf-8")).hexdigest()
        return checkpoint_name

    def get_checkpoint_path(self, name: str) -> str:
        """Constructs the full path for a given checkpoint name.

        Args:
            name: The name of the checkpoint.

        Returns:
            The absolute path to the checkpoint directory/file.
        """
        return f"{self.checkpoint_dir}/{name}"

    def has_checkpoint(self, name: str) -> bool:
        """Checks if a checkpoint with the given name exists.

        A checkpoint is considered to exist if its `_metadata` file is present.

        Args:
            name: The name of the checkpoint.

        Returns:
            True if the checkpoint exists, False otherwise.
        """
        checkpoint_path = self.get_checkpoint_path(name=name)
        return os.path.exists(f"{checkpoint_path}/_metadata")

    def restore_extra_data(self, name: str, fallback: Callable[[], dict], force=False, persist=False) -> dict:
        """Restores extra (non-DataFrame) data from a JSON checkpoint.

        If checkpointing is enabled and the checkpoint file exists (unless 'force'
        is True), it loads the data from the JSON file. Otherwise, it calls the
        'fallback' function to compute the data and then stores it asynchronously.

        Args:
            name: The name of the checkpoint.
            fallback: A callable function that returns the data if not found or forced.
            force: If True, forces recomputation even if a checkpoint exists.
            persist: (Currently unused in the method body, but part of signature)

        Returns:
            A dictionary containing the restored or computed data.
        """
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
    ) -> dd.DataFrame:
        """Restores a Dask DataFrame view from a Parquet checkpoint.

        If checkpointing is enabled and the checkpoint exists (unless 'force' is True),
        it reads the DataFrame from the Parquet store. Otherwise, it calls the
        'fallback' function to compute the DataFrame. If 'write_to_disk' is True,
        the computed DataFrame is then stored as a checkpoint.

        Args:
            name: The name of the checkpoint.
            fallback: A callable function that returns the DataFrame if not found or forced.
            force: If True, forces recomputation even if a checkpoint exists.
            write_to_disk: If True, saves the computed view to disk if it was recomputed.

        Returns:
            A Dask DataFrame representing the restored or computed view.
        """
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
        """Saves the identified bottlenecks to Parquet files.

        The bottlenecks DataFrame is repartitioned and then written to the
        `bottleneck_dir` specified during Analyzer initialization.

        Args:
            bottlenecks: A Dask DataFrame containing the identified bottlenecks.
            partition_size: The desired partition size for the output Parquet files.

        Returns:
            The result of the Dask `to_parquet` operation (typically None or a Dask future).
        """
        # Bottlenecks follow the engine the views were computed on, and a
        # materialised run produces them in pandas. Wrapped back into one Dask
        # partition so the on-disk shape stays a Parquet directory with the
        # `_metadata` file the readers expect. `from_delayed` rather than
        # `from_pandas`: bottlenecks carry a MultiIndex.
        if not isinstance(bottlenecks, dd.DataFrame):
            return as_single_dask_partition(bottlenecks).to_parquet(
                self.bottleneck_dir, compute=True, write_metadata_file=True
            )
        return repartition_to_size(bottlenecks, partition_size).to_parquet(
            self.bottleneck_dir, compute=True, write_metadata_file=True
        )

    @staticmethod
    def store_extra_data(data: Tuple[Dict], data_path: str):
        """Saves extra (non-DataFrame) data to a JSON file.

        This static method is typically used by Dask workers to persist data.

        Args:
            data: A tuple containing a single dictionary of data to be saved.
            data_path: The full path to the JSON file where data will be stored.
        """
        with open(data_path, 'w') as f:
            return json.dump(data[0], f, cls=NpEncoder)

    def store_view(self, name: str, view, compute=True, partition_size='64MB'):
        """Stores a view to a Parquet checkpoint.

        The view is repartitioned and then written to a subdirectory named
        `name` within the `checkpoint_dir`.

        A view may arrive as pandas rather than Dask, since a main view small
        enough to materialise is computed in pandas from there on (see
        `_materialize_if_small`). It is wrapped back into a single Dask
        partition rather than written through pandas directly, so a checkpoint
        keeps one on-disk shape: a Parquet directory carrying the `_metadata`
        file that `has_checkpoint` looks for and `restore_view` reads back.

        The wrapping goes through `from_delayed` rather than `from_pandas`
        because views are indexed by their view type -- a MultiIndex whenever
        there is more than one -- and `from_pandas` rejects a MultiIndex.

        Args:
            name: The name of the checkpoint.
            view: The Dask or pandas DataFrame to store.
            compute: Whether to compute the DataFrame before writing (Dask default is True).
            partition_size: The desired partition size for the output Parquet files.

        Returns:
            The result of the Dask `to_parquet` operation.
        """
        if not isinstance(view, dd.DataFrame):
            return as_single_dask_partition(view).to_parquet(
                self.get_checkpoint_path(name=name),
                compute=compute,
                write_metadata_file=True,
            )
        return repartition_to_size(view, partition_size).to_parquet(
            self.get_checkpoint_path(name=name),
            compute=compute,
            write_metadata_file=True,
        )

    @staticmethod
    def view_permutations(view_types: List[ViewType]):
        """Generates all permutations of view_types for creating multifaceted views.

        For a list of view_types [vt1, vt2, vt3], it will generate permutations
        of length 1, 2, and 3, e.g., (vt1,), (vt2,), (vt1, vt2), (vt2, vt1), ...

        Args:
            view_types: A list of ViewType elements.

        Returns:
            An iterator yielding tuples, where each tuple is a permutation of view_types.
        """

        def _iter_permutations(r: int):
            return it.permutations(view_types, r + 1)

        return it.chain.from_iterable(map(_iter_permutations, range(len(view_types))))

    def _analyze_main_view(
        self,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        percentile: Optional[float],
        threshold: Optional[int],
        view_types: List[ViewType],
        is_slope_based: bool,
        raw_stats: RawStats,
        exclude_bottlenecks: List[str],
        exclude_characteristics: List[str],
        logical_view_types: bool = False,
    ):
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
            # Resolve to plain numbers. `set_metric_slope` divides by this, and
            # dask-expr does not substitute collections passed as map_partitions
            # keyword arguments, so a lazy boundary reaches the partition
            # function as a Scalar object rather than a value. That is latent
            # today because only the `time` metric divides by it, and it is
            # fatal on a pandas frame, which has no scheduler to fall back on.
            (metric_boundaries,) = compute(metric_boundaries)

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

        evaluated_views, bottlenecks, bottleneck_rules, characteristics = self._handle_view_results(
            main_view=main_view,
            metrics=metrics,
            metric_boundaries=metric_boundaries,
            view_results=view_results,
            is_slope_based=is_slope_based,
            raw_stats=raw_stats,
            exclude_bottlenecks=exclude_bottlenecks,
            exclude_characteristics=exclude_characteristics,
        )

        # Return result
        return AnalyzerResultType(
            _bottlenecks=bottlenecks,
            bottleneck_dir=self.bottleneck_dir,
            bottleneck_rules=bottleneck_rules,
            characteristics=characteristics,
            evaluated_views=evaluated_views,
            main_view=main_view,
            metric_boundaries=metric_boundaries,
            raw_stats=raw_stats,
            view_results=view_results,
        )

    def _compute_critical_view(
        self,
        view: dd.DataFrame,
        metric: Metric,
        percentile: Optional[float],
        threshold: Optional[int],
    ):
        """Computes the critical view based on the specified metric.

        This method filters the view DataFrame to identify critical items
        based on either a percentile or a threshold value.

        Args:
            view: The Dask DataFrame representing the view to be filtered.
            metric: The specific metric used for filtering.
            percentile: The percentile to identify critical items.
            threshold: The threshold for slope-based critical item identification.

        Returns:
            A Dask DataFrame containing the filtered critical view.
        """
        frame = BetterFrame(view)
        if percentile is not None and percentile > 0:
            return frame.pipe(
                lambda df: df.query(
                    f"{metric}_pth >= @percentile",
                    local_dict={'percentile': percentile},
                )
            ).finalize()
        elif threshold is not None and threshold > 0:
            corrected_threshold = THRESHOLD_FUNCTIONS[metric](threshold)
            return frame.pipe(
                lambda df: df.query(
                    f"{metric}_slope <= @threshold",
                    local_dict={'threshold': corrected_threshold},
                )
            ).finalize()
        return view

    def _compute_view(
        self,
        records,
        view_type: str,
        metrics: List[Metric],
        metric: Metric,
        metric_boundary: float,
    ):
        """Aggregate records into one view.

        Runs on a Dask or a pandas frame, whichever it is handed. Views are
        routinely a few dozen rows -- the largest across the dftracer fixture is
        78 -- and Dask costs seconds per view regardless of size, so
        `compute_views` materialises the main view when it is provably small
        and this runs in pandas from there.
        """
        frame = BetterFrame(records)
        view_types = frame.index_names()

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
        if view_type != COL_PROC_NAME and COL_PROC_NAME in view_types:

            def aggregate(df):
                return (
                    df.reset_index()
                    .groupby([view_type, COL_PROC_NAME])
                    .agg(non_proc_agg_dict)
                    .groupby([view_type])
                    .agg(proc_agg_dict)
                )

        else:

            def aggregate(df):
                return df.reset_index().groupby([view_type]).agg(non_proc_agg_dict)

        view = frame.pipe(aggregate)
        view = view.pipe(lambda df: _match_dask_agg_dtypes(df, records, aggregate))

        # Set metric slope
        view = view.apply(
            set_metric_slope,
            metrics=metrics,
            metric=metric,
            metric_boundary=metric_boundary,
        )

        # Return view
        return view.finalize()

    @staticmethod
    def _get_agg_dict(
        for_view_type: ViewType,
        view_columns: List[str],
        view_types: List[ViewType],
        is_proc=False,
    ):
        # Named aggregations rather than the builtins. Dask translated
        # `sum`/`min`/`max` for us; pandas takes them literally and warns that a
        # future version will apply the callable directly, which would change
        # the result. The strings are what pandas says preserves behaviour.
        if is_proc:
            agg_dict = {col: 'max' if 'time' in col else 'sum' for col in view_columns}
        else:
            agg_dict = {col: 'sum' for col in view_columns}

        # agg_dict['bw'] = max
        # agg_dict['intensity'] = max
        # agg_dict['iops'] = max
        agg_dict['size_min'] = 'min'
        agg_dict['size_max'] = 'max'

        unwanted_agg_cols = ['id', for_view_type]
        for agg_col in unwanted_agg_cols:
            if agg_col in agg_dict:
                agg_dict.pop(agg_col)

        return agg_dict

    def _handle_view_results(
        self,
        main_view: dd.DataFrame,
        metrics: List[Metric],
        metric_boundaries: Dict[Metric, "dd.Scalar"],
        view_results: Dict[Metric, Dict[ViewKey, ViewResult]],
        is_slope_based: bool,
        raw_stats: RawStats,
        exclude_bottlenecks: List[str],
        exclude_characteristics: List[str],
    ):
        if self.checkpoint:
            with EventLogger(key=EVENT_SAVE_VIEWS, message='Checkpoint views', level=logging.DEBUG):
                view_checkpoint_tasks = []
                for metric, views in view_results.items():
                    for view_key, view_result in views.items():
                        view_checkpoint_name = self.get_checkpoint_name(CHECKPOINT_VIEW, metric, *list(view_key))
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
                is_slope_based=is_slope_based,
            )
            self._wait_all(tasks=evaluated_views)

        # Execute rules
        rule_engine = RuleEngine(rules={}, raw_stats=raw_stats, verbose=self.verbose)
        with EventLogger(key=EVENT_ATT_REASONS, message='Attach reasons to I/O bottlenecks'):
            characteristics = rule_engine.process_characteristics(
                exclude_characteristics=exclude_characteristics,
                main_view=main_view,
                view_results=view_results,
            )
            bottlenecks, bottleneck_rules = rule_engine.process_bottlenecks(
                evaluated_views=evaluated_views,
                exclude_bottlenecks=exclude_bottlenecks,
                group_behavior=False,
                metric_boundaries=metric_boundaries,
            )
            self._wait_all(tasks=bottlenecks)

        with EventLogger(key=EVENT_SAVE_BOT, message='Save I/O bottlenecks', level=logging.DEBUG):
            self.save_bottlenecks(bottlenecks=bottlenecks)

        return evaluated_views, bottlenecks, bottleneck_rules, characteristics

    def _set_derived_columns(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """Adds the derived io_cat, acc_pat and metadata-operation columns.

        Applied through `map_partitions` rather than as chained assignments on
        the Dask frame. The logic is row-local, so the result is identical, but
        under dask-expr each assignment adds a node to the expression tree and
        the optimizer then re-walks that tree: profiling the main view showed
        `simplify_once` called 13,655 times, and `.persist()` taking 6.75s
        against 0.17s on dask 2023.4. One `map_partitions` is one node.

        Args:
            ddf: The high-level metrics Dask DataFrame.

        Returns:
            The same frame with the derived columns added.
        """
        # Derive the meta from the empty frame and pass it explicitly. Left to
        # infer, dask emulates on a synthetic *non-empty* frame whose nullable
        # columns contain <NA>, and the int64 casts below reject those. Real
        # partitions have no NA in those columns, which is why the previous
        # chained-assignment form never hit this.
        meta = self._derive_columns(ddf._meta.copy())
        return ddf.map_partitions(self._derive_columns, meta=meta)

    @staticmethod
    def _derive_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Row-local derivation of the extra metric columns, per partition."""
        # Derive `io_cat` columns
        for col in ['time', 'size', 'count']:
            for io_cat in list(IOCategory):
                col_name = f"{io_cat.name.lower()}_{col}"
                df[col_name] = 0.0 if col == 'time' else 0
                df[col_name] = df[col_name].mask(df['io_cat'] == io_cat.value, df[col])
        for io_cat in list(IOCategory):
            min_name, max_name = (
                f"{io_cat.name.lower()}_min",
                f"{io_cat.name.lower()}_max",
            )
            df[min_name] = 0
            df[max_name] = 0
            df[min_name] = df[min_name].mask(df['io_cat'] == io_cat.value, df['size_min'])
            df[max_name] = df[max_name].mask(df['io_cat'] == io_cat.value, df['size_max'])
        # Derive `data` columns
        df['data_count'] = df['write_count'] + df['read_count']
        df['data_size'] = df['write_size'] + df['read_size']
        df['data_time'] = df['write_time'] + df['read_time']
        # Derive `acc_pat` columns
        for col_suffix, col_value in zip(ACC_PAT_SUFFIXES, ['data_time', 'data_size', 'data_count']):
            for acc_pat in list(AccessPattern):
                col_name = f"{acc_pat.name.lower()}_{col_suffix}"
                df[col_name] = 0.0 if col_suffix == 'time' else 0
                df[col_name] = df[col_name].mask(df['acc_pat'] == acc_pat.value, df[col_value])
        # Derive metadata operation columns
        for col in ['time', 'count']:
            for md_op in DERIVED_MD_OPS:
                col_name = f"{md_op}_{col}"
                df[col_name] = 0.0 if col == 'time' else 0
                if md_op in ['close', 'open']:
                    df[col_name] = df[col_name].mask(
                        df['func_id'].str.contains(md_op) & ~df['func_id'].str.contains('dir'),
                        df[col],
                    )
                else:
                    df[col_name] = df[col_name].mask(df['func_id'].str.contains(md_op), df[col])
        # Cast columns to correct types. `_min`/`_max` are deliberately not
        # cast here: Dask builds meta by running this over a synthetic frame
        # whose nullable columns hold <NA>, and an int64 cast rejects those.
        # They are normalised once on the bottleneck frame instead, where the
        # values are real.
        for col in df.columns:
            if col.endswith('_size') or col.endswith('_count'):
                df[col] = df[col].astype('int64')
            elif col.endswith('_time'):
                df[col] = df[col].astype('float64')
        # Return df
        return df

    def _set_logical_columns(self, view: dd.DataFrame, view_types: List[ViewType]) -> dd.DataFrame:
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
            if isinstance(tasks, pd.DataFrame):
                # Already concrete; there is nothing scheduled to wait on.
                return
            if isinstance(tasks, dd.DataFrame):
                _ = wait(tasks)
            else:
                all_tasks, _ = unpack_collections(tasks)
                _ = wait(all_tasks)
