import glob
import json
import os
import dask.dataframe as dd
from anytree import PostOrderIter
from dask.distributed import Client, as_completed
from time import perf_counter
from vani.common.filter_groups import *
from vani.common.interfaces import *
from vani.common.nodes import AnalysisNode, BinNode, FilterGroupNode
from vani.core.analysis import Analysis, compute_min_max
from vani.core.dask_mgmt import DEFAULT_N_WORKERS_PER_NODE, DaskManager
from vani.utils.file_utils import ensure_dir
from vani.utils.json_encoders import NpEncoder
from vani.utils.logger import ElapsedTimeLogger, create_logger, format_log

PARTITION_FOLDER = "partitioned"


class Analyzer(object):

    # noinspection PyTypeChecker
    def __init__(self, debug=False, cluster_settings: Dict[str, Any] = None, working_dir=".digio"):
        # Create logger
        self.logger = create_logger(__name__, f"{working_dir}/analyzer.log")
        self.logger.info(format_log("main", "Initializing analyzer"))
        # Keep values
        self.cluster_settings = cluster_settings
        self.debug = debug
        self.fg_indices = ['tmid']  # , 'proc_id', 'file_id']
        self.n_workers_per_node = cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        self.working_dir = working_dir
        # Boot Dask clusters & clients
        self.dask_mgr = DaskManager(working_dir=working_dir, fg_indices=self.fg_indices, logger=self.logger,
                                    debug=debug)
        self.dask_mgr.boot(cluster_settings=cluster_settings, n_workers_per_node=self.n_workers_per_node)

    def analyze_parquet_logs(self, log_dir: str, depth=10, persist_stats=True, stats_file_prefix=""):
        # Initialize analysis
        analysis = Analysis(working_dir=self.working_dir,
                            log_dir=log_dir,
                            n_workers_per_node=self.n_workers_per_node,
                            logger=self.logger,
                            debug=self.debug)
        # Initialize futures
        fg_delayed_tasks = {}
        # For all filter group indices
        for fg_index in self.fg_indices:
            # Calculate min max
            fg_range, interval = compute_min_max(log_dir=log_dir, fg_index=fg_index, depth=depth)
            # Create delayed tasks
            logs_delayed = analysis.read_index_logs(fg_index=fg_index)
            metrics_delayed = analysis.compute_metrics(ddf=logs_delayed[-1], fg_index=fg_index, fg_range=fg_range,
                                                       interval=interval)
            scores_delayed = analysis.compute_scores(metrics=metrics_delayed, fg_index=fg_index)
            # Keep delayed tasks
            delayed_tasks = []
            delayed_tasks.extend(logs_delayed)
            delayed_tasks.extend(metrics_delayed)
            delayed_tasks.extend(scores_delayed)

            print(fg_index, fg_range, interval)
            fg_delayed_tasks[fg_index] = delayed_tasks
            break

        # for fg_index in self.fg_indices:
        #     dask.delayed(fg_delayed_tasks[fg_index]).visualize(filename=f"task_graph_{fg_index}")

        fg_futures = {}
        for fg_index in self.fg_indices:
            with self.dask_mgr.clients[fg_index].as_current():
                client = dask.distributed.get_client()
                fg_futures[fg_index] = client.compute(fg_delayed_tasks[fg_index], sync=False)
                break

        for fg_index in self.fg_indices:
            start_time = perf_counter()
            for future in as_completed(fg_futures[fg_index]):
                end_time = perf_counter() - start_time
                print(f"{future.key} {future.status} {end_time / 60}")

        ensure_dir(f"{self.working_dir}/{analysis.id}/")
        for fg_index in self.fg_indices:
            for future in fg_futures[fg_index]:
                if future.key.startswith(f"metrics_{fg_index}_") or future.key.startswith(f"scores_{fg_index}_"):
                    json_result = future.result()
                    with open(f"{self.working_dir}/{analysis.id}/{future.key}.json", "w+") as file:
                        json.dump(json_result, file, cls=NpEncoder)
                pass

        return

    def __partition_logs(self, log_dir: str, partition_size="4GB"):
        # Remove existing files
        existing_files = glob.glob(f"{log_dir}/{PARTITION_FOLDER}/*.parquet", recursive=True)
        for existing_file in existing_files:
            os.remove(existing_file)
        if existing_files:
            self.logger.debug(format_log("main", "Remaining for existing partitions removed"))
        # Get raw files
        files = glob.glob(f"{log_dir}/*.parquet", recursive=True)
        total_size = 0
        for file in files:
            total_size = total_size + os.path.getsize(file)
        self.logger.debug(format_log("main", f"Total file size: {total_size}"))
        # Repartition files
        with ElapsedTimeLogger(logger=self.logger, message="Logs read"):
            ddf = dd.read_parquet(f"{log_dir}/*.parquet")
        with ElapsedTimeLogger(logger=self.logger, message=f"Logs repartitioned into partitions of: {partition_size}"):
            ddf = ddf.repartition(partition_size=partition_size)
        partition_folder = f"{log_dir}/{PARTITION_FOLDER}/"
        with ElapsedTimeLogger(logger=self.logger, message=f"Partitions written into: {partition_folder}"):
            dd.to_parquet(ddf, partition_folder)
        # Return partitioned files
        partitioned_files = glob.glob(f"{log_dir}/{PARTITION_FOLDER}/*.parquet", recursive=True)
        return partitioned_files

    def generate_hypotheses(analysis: AnalysisNode):
        # Get all filter groups
        return analysis.generate_hypotheses()

    def save_filter_group_node_as_flamegraph(self, filter_group_node: FilterGroupNode, output_path: str):
        # Init lines
        lines = []
        # Loop nodes
        for node in PostOrderIter(filter_group_node):
            # When the root is reached
            if node.parent == filter_group_node:
                break  # Stop
            # Read node bin
            start, stop = node.bin
            # Start building columns
            columns = []
            columns.append(repr(node))
            for ancestor in reversed(node.ancestors):
                if isinstance(ancestor, BinNode):
                    columns.append(repr(ancestor))
            # Build line
            lines.append(f"{';'.join(reversed(columns))} {stop - start}")
        # Write lines into output file
        with open(output_path, 'w') as file:
            file.write('\n'.join(lines))

    def shutdown(self):
        self.dask_mgr.shutdown()
