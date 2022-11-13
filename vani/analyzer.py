import dask.dataframe as dd
import glob
import json
import os
from anytree import PostOrderIter
from dask.distributed import Client, as_completed
from time import perf_counter
from vani.common.filter_groups import *
from vani.common.interfaces import *
from vani.common.nodes import AnalysisNode, BinNode, FilterGroupNode
from vani.core.analysis import Analysis
from vani.core.dask_mgmt import DEFAULT_N_WORKERS_PER_NODE, DaskManager
from vani.utils.file_utils import ensure_dir
from vani.utils.json_encoders import NpEncoder
from vani.utils.logger import ElapsedTimeLogger, create_logger, format_log
from vani.utils.visualization import save_graph

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
        self.fg_indices = ['tmid', 'proc_id', 'file_id']  # 'tmid', 'proc_id', 'file_id']
        self.n_workers_per_node = cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        self.working_dir = working_dir
        # Boot Dask clusters & clients
        client_urls = {}
        with open(f"{working_dir}/clients.json") as file:
            client_urls = json.load(file)
        self.dask_mgr = DaskManager(working_dir=working_dir, fg_indices=self.fg_indices, logger=self.logger,
                                    debug=debug)
        self.dask_mgr.boot(cluster_settings=cluster_settings, n_workers_per_node=self.n_workers_per_node)
        # self.dask_mgr.initialize_client_urls(client_urls=client_urls)

    def analyze_parquet_logs(self, log_dir: str, depth=10, persist_stats=True, stats_file_prefix=""):
        # Initialize analysis
        analysis = Analysis(log_dir=log_dir,
                            working_dir=self.working_dir,
                            n_workers_per_node=self.n_workers_per_node,
                            logger=self.logger,
                            depth=depth,
                            debug=self.debug)
        # Load min max
        analysis.load_global_min_max()
        # Initialize futures
        fg_delayed_tasks = {}
        # For all filter group indices
        for fg_index in self.fg_indices:
            # Set filter group
            analysis.set_filter_group(fg_index=fg_index)
            # Create delayed tasks
            delayed_tasks = []
            # Read & index logs
            logs_d = analysis.read_and_index_logs()
            delayed_tasks.extend(logs_d)
            if fg_index == 'tmid':
                metrics_d = analysis.compute_metrics_tmid(ddf=logs_d[-1])
                delayed_tasks.extend(metrics_d)
            elif fg_index == 'proc_id':
                proc_id_metrics_d = analysis.compute_metrics_proc_id(ddf=logs_d[-1])
                metrics_d = proc_id_metrics_d[-1]
                delayed_tasks.extend(proc_id_metrics_d)
            elif fg_index == 'file_id':
                file_id_metrics_d = analysis.compute_metrics_file_id(ddf=logs_d[-1])
                metrics_d = file_id_metrics_d[-1]
                delayed_tasks.extend(file_id_metrics_d)
            else:
                raise
            scores_d = analysis.compute_scores(metrics=metrics_d)
            delayed_tasks.extend(scores_d)

            # Keep delayed tasks
            fg_delayed_tasks[fg_index] = delayed_tasks
            # break

        for fg_index in self.fg_indices:
            save_graph(dask.delayed(fg_delayed_tasks[fg_index])(dask_key_name=f"save-graph-{fg_index}"),
                       filename=f"{self.working_dir}/{analysis.id}/task-graph-{fg_index}")

        fg_futures = {}
        for fg_index in self.fg_indices:
            with self.dask_mgr.clients[fg_index].as_current():
                client = dask.distributed.get_client()
                fg_futures[fg_index] = client.compute(fg_delayed_tasks[fg_index], sync=False)
                # break

        for fg_index in self.fg_indices:
            start_time = perf_counter()
            for future in as_completed(fg_futures[fg_index]):
                end_time = perf_counter() - start_time
                print(f"{future.key} {future.status} {end_time / 60}")

        ensure_dir(f"{self.working_dir}/{analysis.id}/")
        for fg_index in self.fg_indices:
            for future in fg_futures[fg_index]:
                key_name = f"{future.key[0]}-{future.key[1]}" if isinstance(future.key, tuple) else future.key
                # if key_name.startswith(f"metrics_{fg_index}_") or key_name.startswith(f"scores_{fg_index}_"):
                if key_name.startswith("metrics-") or key_name.startswith("scores-") or key_name.startswith("repartition-"):
                    json_result = future.result()
                    with open(f"{self.working_dir}/{analysis.id}/{key_name}.json", "w+") as file:
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
