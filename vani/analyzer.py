import asyncio

import dask
import dask.dataframe as dd
import functools
import glob
import json
import logging
import math
import os
import psutil
import socket
from anytree import PostOrderIter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dask.distributed import Client, LocalCluster, get_task_stream, progress, wait
from distributed.diagnostics.plugin import SchedulerPlugin
from dask_jobqueue import LSFCluster
from time import perf_counter, sleep
from tqdm.auto import tqdm
from typing import Union
from vani.common.filter_groups import *
from vani.common.interfaces import *
from vani.common.nodes import AnalysisNode, BinNode, FilterGroupNode
from vani.utils.data_filtering import filter_non_io_traces, split_io_mpi_trace, split_read_write_metadata
from vani.utils.logger import ElapsedTimeLogger, create_logger, format_log

DEFAULT_N_WORKERS_PER_NODE = 16
DEFAULT_N_THREADS_PER_WORKER = 1
DEFAULT_NODE_MEMORY = 1600
DEFAULT_WORKER_QUEUE = 'pdebug'
DEFAULT_WORKER_TIME = 120
PARTITION_FOLDER = "partitioned"
WORKER_CHECK_INTERVAL = 5.0


class Counter(SchedulerPlugin):

    def __init__(self):
        self.tasks_fingerprints = {}

    async def start(self, scheduler):
        self.scheduler = scheduler

    def transition(self, key, start, finish, *args, **kwargs):
        ts = self.scheduler.tasks[key]
        if finish in ['memory', 'released']:
            if ts.prefix_key not in self.tasks_fingerprints:
                self.tasks_fingerprints[ts.prefix_key] = {
                    'sum_dur': 0,
                    'n_tasks': 0
                }
            self.tasks_fingerprints[ts.prefix_key] = {
                'sum_dur': self.tasks_fingerprints[ts.prefix_key]['sum_dur'] + self.scheduler.get_task_duration(ts),
                'n_tasks': self.tasks_fingerprints[ts.prefix_key]['n_tasks'] + 1
            }

class ProgressBarPlugin(SchedulerPlugin):
    def __init__(self):
        self.tqdm_map = {}
        self.ts_count = {}
        self.ts_done = {}

    def start(self, scheduler):
        self.scheduler = scheduler

    def initialize(self, futures):
        self.tqdm_map = {}
        dask_graph = futures.__dask_graph__()
        self.ts_count = {}
        self.ts_done = {}
        for key, value in dask_graph.layers.items():
            node = key.split("-")[0]
            if node not in self.ts_count:
                self.ts_count[node] = 0
            self.ts_count[node] = self.ts_count[node] + 1
        for key, count in self.ts_count.items():
            if key not in self.tqdm_map:
                self.tqdm_map[key] = tqdm(total=self.ts_count[key], desc=f"{key}")
                self.ts_done[key] = 0

    def transition(self, key, start, finish, *args, **kwargs):
        ts = self.scheduler.tasks[key]
        if finish in ["memory","released"]:
            self.tqdm_map[ts.prefix_key].update(1)
            self.ts_done[ts.prefix_key] = ts.prefix_key + 1

    def finalize(self):
        for key, count in self.ts_count.items():
            self.tqdm_map[key].update(self.ts_count[key] - self.ts_done[key])
            sleep(0.1)
            self.tqdm_map[key].close()
        self.tqdm_map = {}
        self.ts_count = {}
        self.ts_done = {}

    def restart(self, scheduler):
        self.tqdm_map = {}


class Analyzer(object):

    # noinspection PyTypeChecker
    def __init__(self, debug=False, cluster_settings: Dict[str, Any] = None):
        # Create logger
        self.logger = create_logger(__name__, "digio.analyzer.log")
        self.logger.info(format_log("main", "Initializing analyzer"))
        # Keep values
        self.cluster_settings = cluster_settings
        self.debug = debug
        self.filter_group_indices = ['tmid', 'proc_id', 'file_id']
        self.n_workers_per_node = cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        # Initialize clusters
        self.clusters = self.__initialize_clusters(cluster_settings=cluster_settings,
                                                   filter_group_indices=self.filter_group_indices)
        # Initialize clients
        self.clients = self.__initialize_clients(clusters=self.clusters)
        # Scale clusters
        self.__scale_clusters(clusters=self.clusters,
                              n_workers=self.n_workers_per_node)

    def analyze_parquet_logs(self, log_dir: str, depth=3, persist_stats=True, stats_file_prefix=""):
        # Ensure logs partitioned
        files = self.__ensure_logs_partitioned(log_dir=log_dir)
        # TODO Json keys & columns
        json_keys = dict(tmid='tmid', proc_id='processes', file_id='filenames')
        # Run analysis tasks
        with ThreadPoolExecutor(max_workers=len(self.filter_group_indices)) as executor:
            # Submit tasks
            analysis_tasks = {
                executor.submit(
                    self._analyze_filter_group, log_dir, filter_group_index,
                    json_keys[filter_group_index], thread_index): (thread_index, filter_group_index) for
                thread_index, filter_group_index in
                enumerate(self.filter_group_indices)
            }
            # Wait until completion
            for analysis_task in as_completed(analysis_tasks):
                try:
                    result = analysis_task.result()
                except Exception as exc:
                    print(f'generated an exception: {exc}')
                else:
                    print(f'Created index for ')

        return

        analysis = AnalysisNode(filter_group_nodes=filter_group_nodes)
        # Cancel task
        # keep_alive_task.cancel()
        # Return analysis tree
        return analysis

    def _analyze_filter_group(self, log_dir: str, filter_group_index: str, json_key: str, thread_index: int):
        # Keep workers alive
        # keep_alive_task = asyncio.create_task(self.__keep_workers_alive(filter_group_index=filter_group_index,
        #                                                                 cluster=cluster,
        #                                                                 client=client))

        # Get client & cluster
        client = self.clients[filter_group_index]
        cluster = self.clusters[filter_group_index]
        # Wait until workers alive
        self.__wait_until_workers_alive(filter_group_index=filter_group_index,
                                        cluster=cluster,
                                        client=client,
                                        n_workers=self.n_workers_per_node / 2)
        # Compute min & max of given JSON key
        min_val, max_val = self.__compute_min_max(client=client,
                                                  json_key=json_key,
                                                  log_dir=log_dir,
                                                  thread_index=thread_index)
        return
        # Get indexed dataframe
        indexed_ddf = self.__indexed_ddf(client=client,
                                         filter_group_index=filter_group_index,
                                         log_dir=log_dir)
        # Analyze dataframe
        metrics = self.__compute_metrics(client=client,
                                         ddf=indexed_ddf,
                                         filter_group_index=filter_group_index,
                                         min_val=min_val,
                                         max_val=max_val)
        # Track task fingerprints
        self.logger.debug(client.cluster.scheduler.plugins['counter'].tasks_fingerprints)

        x = 1
        print(metrics[0][0])
        # Cancel keep workers alive
        # keep_alive_task.cancel()
        return

    def __indexed_ddf(self, client: Client, log_dir: str, filter_group_index: str):
        with client.as_current():
            with ElapsedTimeLogger(logger=self.logger, message="Logs read", caller=filter_group_index):
                ddf = dd.read_parquet(f"{log_dir}/{PARTITION_FOLDER}/*.parquet", index=False)
            with ElapsedTimeLogger(logger=self.logger, message="Logs indexed", caller=filter_group_index):
                indexed_ddf = ddf.set_index([filter_group_index])
        with ElapsedTimeLogger(logger=self.logger, message="Logs persisted", caller=filter_group_index):
            with get_task_stream(client, plot='save', filename=f'{filter_group_index}-index-ddf.html') as ts:
                indexed_ddf = client.persist(indexed_ddf)
                # TODO: reduces the pressure on the cluster.
                result = wait(indexed_ddf)
            # TODO: cache the index dataset for future use. Utilize md5 hash on logs folder to see if anything is
            #  changed if not use cache if present
        return indexed_ddf

    def __compute_metrics(self, client: Client, ddf: DataFrame, filter_group_index: str, min_val: int, max_val: int):
        def filter(start: int, stop: int, filter_group_index: str):
            # Select dataframes
            target_ddf = ddf.loc[start:stop]
            read_ddf = target_ddf[(target_ddf['io_cat'] == 1)]
            write_ddf = target_ddf[(target_ddf['io_cat'] == 2)]
            metadata_ddf = target_ddf[(target_ddf['io_cat'] == 3)]
            # TODO: on same compute run for all ddf.
            # Create tasks
            read_tasks = [
                read_ddf.index.unique() if filter_group_index == 'proc_id' else read_ddf['proc_id'].unique(),
                read_ddf['duration'].sum(),
                read_ddf['size'].sum(),
                read_ddf.index.unique() if filter_group_index == 'file_id' else read_ddf['file_id'].unique(),
                read_ddf['bandwidth'].sum(),
                read_ddf['index'].count(),
            ]
            write_tasks = [
                write_ddf.index.unique() if filter_group_index == 'proc_id' else write_ddf['proc_id'].unique(),
                write_ddf['duration'].sum(),
                write_ddf['size'].sum(),
                write_ddf.index.unique() if filter_group_index == 'file_id' else write_ddf['file_id'].unique(),
                write_ddf['bandwidth'].sum(),
                write_ddf['index'].count(),
            ]
            metadata_tasks = [
                metadata_ddf.index.unique() if filter_group_index == 'proc_id' else metadata_ddf['proc_id'].unique(),
                metadata_ddf['duration'].sum(),
                metadata_ddf.index.unique() if filter_group_index == 'file_id' else metadata_ddf['file_id'].unique(),
                metadata_ddf['index'].count(),
            ]
            filter_tasks = []
            filter_tasks.extend(read_tasks)
            filter_tasks.extend(write_tasks)
            filter_tasks.extend(metadata_tasks)
            # Compute all
            filter_results = dask.compute(*filter_tasks)
            # Clear dataframes
            del read_ddf
            del write_ddf
            del metadata_ddf
            del target_ddf
            # Arrange results
            read_start, read_end = 0, len(read_tasks)
            write_start, write_end = len(read_tasks), len(read_tasks) + len(write_tasks)
            metadata_start, metadata_end = len(read_tasks) + len(write_tasks), 0
            filter_result = {
                'read': {
                    'uniq_ranks': filter_results[:read_end][0],
                    'agg_dur': filter_results[:read_end][1],
                    'total_io_size': filter_results[:read_end][2],
                    'uniq_filenames': filter_results[:read_end][3],
                    'bw_sum': filter_results[:read_end][4],
                    'ops': filter_results[:read_end][5],
                },
                'write': {
                    'uniq_ranks': filter_results[write_start:write_end][0],
                    'agg_dur': filter_results[write_start:write_end][1],
                    'total_io_size': filter_results[write_start:write_end][2],
                    'uniq_filenames': filter_results[write_start:write_end][3],
                    'bw_sum': filter_results[write_start:write_end][4],
                    'ops': filter_results[write_start:write_end][5],
                },
                'metadata': {
                    'uniq_ranks': filter_results[metadata_start:][0],
                    'agg_dur': filter_results[metadata_start:][1],
                    'uniq_filenames': filter_results[metadata_start:][2],
                    'ops': filter_results[metadata_start:][3],
                }
            }
            # Return results
            return filter_result

        def merge(x: Dict, y: Dict):
            return {
                'read': {
                    'uniq_ranks': np.union1d(x['read']['uniq_ranks'], y['read']['uniq_ranks']),
                    'agg_dur': x['read']['agg_dur'] + y['read']['agg_dur'],
                    'total_io_size': x['read']['total_io_size'] + y['read']['total_io_size'],
                    'uniq_filenames': np.union1d(x['read']['uniq_filenames'], y['read']['uniq_filenames']),
                    'bw_sum': x['read']['bw_sum'] + y['read']['bw_sum'],
                    'ops': x['read']['ops'] + y['read']['ops'],
                },
                'write': {
                    'uniq_ranks': np.union1d(x['write']['uniq_ranks'], y['write']['uniq_ranks']),
                    'agg_dur': x['write']['agg_dur'] + y['write']['agg_dur'],
                    'total_io_size': x['write']['total_io_size'] + y['write']['total_io_size'],
                    'uniq_filenames': np.union1d(x['write']['uniq_filenames'], y['write']['uniq_filenames']),
                    'bw_sum': x['write']['bw_sum'] + y['write']['bw_sum'],
                    'ops': x['write']['ops'] + y['write']['ops'],
                },
                'metadata': {
                    'uniq_ranks': np.union1d(x['metadata']['uniq_ranks'], y['metadata']['uniq_ranks']),
                    'agg_dur': x['metadata']['agg_dur'] + y['metadata']['agg_dur'],
                    'uniq_filenames': np.union1d(x['metadata']['uniq_filenames'], y['metadata']['uniq_filenames']),
                    'ops': x['metadata']['ops'] + y['metadata']['ops'],
                }
            }

        depth = 10
        next_tasks = 2 ** depth
        interval = math.floor(max_val * 1.0 / next_tasks)
        iterations = list(range(0, depth + 1))
        iterations.reverse()
        all_tasks = [0] * (depth + 1)
        time_range = np.arange(min_val, max_val, interval)
        for i in iterations:
            tasks = []
            if i == depth:
                for start in time_range:
                    stop = start + interval
                    tasks.append(dask.delayed(filter)(start, stop, filter_group_index))
            else:
                next_tasks = len(all_tasks[i + 1])
                if next_tasks % 2 == 1:
                    next_tasks = next_tasks - 1
                for t in range(0, next_tasks, 2):
                    tasks.append(dask.delayed(merge)(all_tasks[i + 1][t], all_tasks[i + 1][t + 1]))
                next_tasks = len(all_tasks[i + 1])
                if next_tasks % 2 == 1:
                    tasks.append(all_tasks[i + 1][next_tasks - 1])
                # TODO why are we calling len on everything?
                for t, next_tasks in enumerate(all_tasks[i + 1]):
                    all_tasks[i + 1][t] = dask.delayed(self.__len)(next_tasks)
            all_tasks[i] = tasks
        for t, next_tasks in enumerate(all_tasks[0]):
            all_tasks[0][t] = dask.delayed(self.__len)(next_tasks)

        with ElapsedTimeLogger(logger=self.logger, message="Metrics computed", caller=filter_group_index):
            with get_task_stream(client, plot='save', filename=f'{filter_group_index}-metrics.html') as ts:
                metrics_futures = client.compute(all_tasks)
                result = wait(metrics_futures)
                metrics = client.gather(metrics_futures)

        return metrics

    @staticmethod
    def __len(x: Dict):
        return {
            'read': {
                'uniq_ranks': len(x['read']['uniq_ranks']),
                'agg_dur': x['read']['agg_dur'],
                'total_io_size': x['read']['total_io_size'],
                'uniq_filenames': len(x['read']['uniq_filenames']),
                'bw_sum': x['read']['bw_sum'],
                'ops': x['read']['ops'],
            },
            'write': {
                'uniq_ranks': len(x['write']['uniq_ranks']),
                'agg_dur': x['write']['agg_dur'],
                'total_io_size': x['write']['total_io_size'],
                'uniq_filenames': len(x['write']['uniq_filenames']),
                'bw_sum': x['write']['bw_sum'],
                'ops': x['write']['ops'],
            },
            'metadata': {
                'uniq_ranks': len(x['metadata']['uniq_ranks']),
                'agg_dur': x['metadata']['agg_dur'],
                'uniq_filenames': len(x['metadata']['uniq_filenames']),
                'ops': x['metadata']['ops']
            }
        }

    def __compute_min_max(self, client: Client, log_dir: str, json_key: str, thread_index: int) -> Tuple[int, int]:
        # Define functions
        def min_max_of_array(arr: List):
            return functools.reduce(lambda x, y: (min(x[0], y), max(x[1], y)), arr, (arr[0], arr[0],))

        def min_max_of_global_metric(log_dir: str, json_key: str):
            with open(f"{log_dir}/global.json") as file:
                global_metrics = json.load(file)
                min_val, max_val = 0, math.ceil(global_metrics[json_key])
                return min_val, max_val

        def min_max_of_json_values(json_file: str, json_key: str):
            with open(json_file) as file:
                data = json.load(file)
                min_val, max_val = min_max_of_array(data[json_key])
                return [min_val, max_val]

        # If JSON key is tmid, then compute min & max through global metrics
        if json_key == 'tmid':
            min_val, max_val = min_max_of_global_metric(log_dir=log_dir, json_key='max_tend')
            self.logger.debug(format_log(json_key, f"Min-max of {json_key}: {min_val}-{max_val}"))
            return min_val, max_val
        # Read rank files
        rank_files = glob.glob(f"{log_dir}/rank_*.json", recursive=True)
        n_rank_files = len(rank_files)
        # Do not run distributed tasks if there is only a single rank
        if n_rank_files == 1:
            min_val, max_val = min_max_of_json_values(json_file=rank_files[0], json_key=json_key)
            self.logger.debug(format_log(json_key, f"Min-max of {json_key}: {min_val}-{max_val}"))
            return min_val, max_val
        # Otherwise run it distributed
        with client.as_current():
            depth = math.ceil(math.sqrt(n_rank_files))
            iterations = list(range(0, depth + 1))
            iterations.reverse()
            all_tasks = [0] * (depth + 1)
            # with ElapsedTimeLogger(logger=self.logger, message="Min-max tasks arranged", caller=json_key):
            for i in iterations:
                tasks = []
                if i == depth:
                    for file in rank_files:
                        tasks.append(dask.delayed(min_max_of_json_values)(file, json_key))
                else:
                    next_tasks = len(all_tasks[i + 1])
                    n_next_tasks = next_tasks
                    if next_tasks % 2 == 1:
                        n_next_tasks = n_next_tasks - 1
                    for t in range(0, n_next_tasks, 2):
                        # noinspection PyUnresolvedReferences
                        tasks.append([dask.delayed(min)(all_tasks[i + 1][t][0], all_tasks[i + 1][t + 1][0]),
                                      dask.delayed(max)(all_tasks[i + 1][t][1], all_tasks[i + 1][t + 1][1])])
                    if next_tasks % 2 == 1:
                        tasks.append(all_tasks[i + 1][next_tasks - 1])
                    all_tasks[i + 1] = 0
                all_tasks[i] = tasks
            with ElapsedTimeLogger(logger=self.logger, message="Min-max tasks completed", caller=json_key):
                with get_task_stream(client, plot='save', filename=f'{json_key}-min-max.html') as ts:
                        # dask.visualize(all_tasks, filename=f'{json_key}-min-max.svg')
                    to_be_vis = dask.delayed(all_tasks)()
                    to_be_vis.dask.visualize(filename=f'{json_key}-min-max.svg')
                    client.cluster.scheduler.plugins['progress'].initialize(to_be_vis)
                    values = dask.compute(all_tasks)
                    client.cluster.scheduler.plugins['progress'].finalize()
        min_val, max_val = values[0][0][0][0], values[0][0][0][1]
        self.logger.debug(format_log(json_key, f"Min-max of {json_key}: {min_val}-{max_val}"))
        return min_val, max_val

    def __ensure_logs_partitioned(self, log_dir: str):
        # Ensure logs partitioned
        is_partitioned = os.path.exists(f"{log_dir}/{PARTITION_FOLDER}/_common_metadata")
        self.logger.debug(format_log("main", f"Logs partitioned before: {is_partitioned}"))
        files = glob.glob(f"{log_dir}/{PARTITION_FOLDER}/*.parquet", recursive=True)
        return files if is_partitioned else self.__partition_logs(log_dir=log_dir)

    @staticmethod
    def __min_max_of_array(arr: List):
        return functools.reduce(lambda x, y: (min(x[0], y), max(x[1], y)), arr, (arr[0], arr[0],))

    @staticmethod
    def __min_max_of_global_metric(log_dir: str, json_key: str):
        with open(f"{log_dir}/global.json") as file:
            global_metrics = json.load(file)
            min_val, max_val = 0, math.ceil(global_metrics[json_key])
            return min_val, max_val

    def __min_max_of_json_values(self, json_file: str, json_key: str):
        with open(json_file) as file:
            data = json.load(file)
            min_val, max_val = self.__min_max_of_array(data[json_key])
            return [min_val, max_val]

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

    def shutdown(self) -> None:
        for client in self.clients.values():
            client.close()
        for cluster in self.clusters.values():
            cluster.close()

    def __initialize_clients(self, clusters: Dict[str, Union[LocalCluster, LSFCluster]]):
        # Initialize clients
        clients = {}
        self.counters = {}
        # Loop through clusters
        for cluster_key in clusters.keys():
            # Get cluster instance
            cluster = clusters[cluster_key]
            # Create a client & set it as default if it is a local cluster
            clients[cluster_key] = Client(cluster, set_as_default=isinstance(cluster, LocalCluster))
            self.counters[cluster_key] = Counter()
            clients[cluster_key].register_scheduler_plugin(self.counters[cluster_key], "counter")
            progress = ProgressBarPlugin()
            clients[cluster_key].register_scheduler_plugin(progress, "progress")
            # clients[cluster_key].scheduler.add_plugin(Counter)
            self.logger.debug(format_log(cluster_key, "Client initialized"))
            # Print client information
            if self.debug:
                print(clients[cluster_key])
        # Return clients
        return clients

    def __initialize_clusters(self, filter_group_indices: [str], cluster_settings: Dict[str, Any]):
        # Read required config
        dashboard_port = cluster_settings.get('dashboard_port')
        local_directory = cluster_settings.get('local_directory')
        log_file = cluster_settings.get('log_file')
        # Read optional config
        cores = cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        host = cluster_settings.get('host', socket.gethostname())
        memory = cluster_settings.get('memory', DEFAULT_NODE_MEMORY)
        processes = cluster_settings.get('processes', DEFAULT_N_THREADS_PER_WORKER)
        use_stdin = cluster_settings.get('use_stdin', True)
        worker_queue = cluster_settings.get('worker_queue', DEFAULT_WORKER_QUEUE)
        worker_time = cluster_settings.get('worker_time', DEFAULT_WORKER_TIME)
        # Create clusters
        clusters = {}
        # Create a local cluster
        clusters['local'] = LocalCluster(dashboard_address=f"{host}:{dashboard_port}",
                                         host=host,
                                         local_directory=f"{local_directory}/local",
                                         memory_limit=0,
                                         n_workers=cores,
                                         silence_logs=logging.DEBUG if self.debug else logging.CRITICAL)
        self.logger.debug(format_log("local", f"Cluster initialized ({clusters['local'].dashboard_link})"))
        # Create distributed clusters
        for index, filter_group_index in enumerate(filter_group_indices):
            # Create LSF cluster
            # TODO do not set -o -e then the workers will have log on their own jobid.out additionally the job id is
            #  stored on a env variable which can be written to our log.
            clusters[filter_group_index] = LSFCluster(cores=cores * processes,
                                                      death_timeout=worker_time * 60,
                                                      header_skip=['-n', '-R', '-M', '-P', '-W 00:30'],
                                                      job_extra=['-nnodes 1',
                                                                 '-G asccasc',
                                                                 '-q {}'.format(worker_queue),
                                                                 '-W {}'.format(worker_time),
                                                                 # '-o {}'.format(log_file),
                                                                 # '-e {}'.format(log_file)
                                                                 ],
                                                      local_directory=f"{local_directory}/{filter_group_index}",
                                                      memory=f"{memory}GB",
                                                      processes=cores,
                                                      scheduler_options=dict(
                                                          dashboard_address=f"{host}:{dashboard_port + index + 1}",
                                                          host=host,
                                                      ),
                                                      use_stdin=use_stdin)
            dashboard_link = clusters[filter_group_index].dashboard_link
            self.logger.debug(format_log(filter_group_index, f"Cluster initialized ({dashboard_link})"))

            # Print cluster debug info
        for cluster_key in clusters.keys():
            cluster = clusters[cluster_key]
            if self.debug:
                print("Dashboard link:", cluster.dashboard_link)
                if isinstance(cluster, LSFCluster):
                    print(cluster.job_script())

        # Return clusters
        return clusters

    async def __keep_workers_alive(self, filter_group_index: str, cluster: LSFCluster, client: Client):
        # While the job is still executing
        while True:
            # Wait a second
            await asyncio.sleep(WORKER_CHECK_INTERVAL)
            # Check workers
            self.__wait_until_workers_alive(filter_group_index=filter_group_index,
                                            cluster=cluster,
                                            client=client)

    def _read_parquet(self, log_dir: str, engine='auto'):
        # Read logs into a dataframe
        t_start = perf_counter()
        df = dd.read_parquet(f"{log_dir}/*.parquet", engine=engine)
        t_end = perf_counter()
        # Print performance
        if self.debug:
            print(f"Logs read ({t_end - t_start})")
        # Return job time
        return df

    def __scale_clusters(self, clusters: Dict[str, Union[LocalCluster, LSFCluster]], n_workers: int):
        for cluster_key in clusters.keys():
            cluster = clusters[cluster_key]
            if not isinstance(cluster, LocalCluster):
                cluster.scale(n_workers * 2)
                self.logger.debug(format_log(cluster_key, f"Scaling cluster to {n_workers * 2} nodes"))

    def __wait_until_workers_alive(self, filter_group_index: str, cluster: LSFCluster, client: Client,
                                   n_workers: int = None):
        # Get current number of workers
        current_n_workers = len(client.scheduler_info()['workers'])
        expected_n_workers = int(self.n_workers_per_node if n_workers is None else n_workers)
        # Wait until enough number of workers alive
        while client.status == 'running' and current_n_workers < expected_n_workers:
            # Log status
            self.logger.debug(
                format_log(filter_group_index, f"{current_n_workers}/{expected_n_workers} workers running"))
            # Ensure loop
            self.__ensure_asyncio_loop()
            # Try correcting state
            # noinspection PyProtectedMember
            cluster._correct_state()
            # Sleep a little
            sleep(WORKER_CHECK_INTERVAL)
            # Get current number of workers
            current_n_workers = len(client.scheduler_info()['workers'])
        # Print result
        self.logger.debug(format_log(filter_group_index, "All workers alive"))

    @staticmethod
    def __ensure_asyncio_loop():
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            if str(e).startswith('There is no current event loop in thread'):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:
                raise
