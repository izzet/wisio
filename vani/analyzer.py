import asyncio
import math
import dask
import dask.array as da
import dask.dataframe as dd
import logging
import os
import socket
from anytree import PostOrderIter
from dask.dataframe import DataFrame
from dask.distributed import Client, LocalCluster, wait
from dask_jobqueue import LSFCluster
from enum import Enum
from time import perf_counter, sleep
from tqdm import tqdm
from vani.common.filter_groups import *
from vani.common.interfaces import _BinInfo, _FilterGroup
from vani.common.nodes import AnalysisNode, BinNode, FilterGroupNode
from vani.utils.data_filtering import filter_non_io_traces, split_io_mpi_trace, split_read_write_metadata
from vani.utils.yaml_utils import load_persisted

_WORKER_CHECK_INTERVAL = 5.0


class ClusterType(Enum):
    Local = 'Local'
    LSF = 'LSF'


class ClusterOptions(object):

    def __init__(self, cluster_type: ClusterType, **cluster_settings):
        self.cluster_type = cluster_type
        self.cluster_settings = cluster_settings


class Analyzer(object):

    def __init__(self, n_workers: int, cluster_options: ClusterOptions, debug=False):
        # Keep values
        self.cluster_options = cluster_options
        self.debug = debug
        self.n_workers = n_workers
        # Initialize cluster
        self.cluster = self.__initialize_cluster(cluster_options=cluster_options)
        # Initialize client
        self.client = Client(self.cluster)
        # Scale cluster
        if cluster_options.cluster_type != ClusterType.Local:
            self.cluster.scale(n_workers)
            self.__wait_until_workers_alive()

    def analyze_parquet_logs(self, log_dir: str, depth=3, persist_stats=True, stats_file_prefix=""):
        # Keep workers alive
        # keep_alive_task = asyncio.create_task(self.__keep_workers_alive())
        # Read logs into a dataframe
        self.ddf = self._read_parquet(log_dir=log_dir)
        # Filter non-I/O traces (except for MPI)
        # Split dataframe into I/O, MPI and trace
        # Split io_df into read & write and metadata dataframes
        self.ddf = filter_non_io_traces(ddf=self.ddf)
        io_ddf, mpi_ddf, trace_ddf = split_io_mpi_trace(ddf=self.ddf)
        io_ddf_read, io_ddf_write, io_ddf_metadata = split_read_write_metadata(io_ddf=io_ddf)
        # Compute global stats
        self.global_stats = load_persisted(path=f"{stats_file_prefix}global_stats.yaml", fallback=self._compute_global_stats)
        job_time = self.global_stats['job_time']

        # ddf = io_ddf_write
        # ddf = ddf.persist()

        # wait(ddf)

        # def filter(start, end):
        #     return ddf[(ddf['tstart']>= start) & (ddf['tend']< end)]['rank'].unique()

        # def merge(x, y):
        #     return np.union1d(x, y)

        # MAX_DEPTH=5
        # depth = list(range(0, MAX_DEPTH+1))
        # depth.reverse()
        # tstart=0
        # tend=job_time
        # pieces = 2**MAX_DEPTH
        # interval = math.floor((tend-tstart)*1.0/pieces)
        # print(depth, pieces)
        # output = [0]*(MAX_DEPTH+1)
        # print(output)
        # for x in depth:
        #     depth_ret = []
        #     if (x == MAX_DEPTH):
        #         for i in range(tstart, math.floor(tend - interval), interval):
        #             depth_ret.append(dask.delayed(filter)(tstart, tstart + interval))
        #     else:
        #         pieces_in_next_level = 2**(x+1)
        #         for i in range(0, pieces_in_next_level , 2):
        #             depth_ret.append(dask.delayed(merge)(output[x+1][i], output[x+1][i+1]))
        #     output[x] = depth_ret


        # values = dask.compute(output)



        # Define filter groups
        n_bins = 10
        filter_groups = [
            ("Time-based > Read", TimelineReadFilterGroup(n_bins=n_bins, stats_file_prefix=stats_file_prefix), io_ddf_read),
            ("Time-based > Write", TimelineWriteFilterGroup(n_bins=n_bins, stats_file_prefix=stats_file_prefix), io_ddf_write),
            ("Time-based > Metadata", TimelineMetadataFilterGroup(n_bins=n_bins, stats_file_prefix=stats_file_prefix), io_ddf_metadata)
        ]
        # Keep filter group nodes
        filter_group_nodes = []
        tasks = {}
        # Persist filter group bins
        # bin_ddfs_per_filter_group = self.__persist_filter_group_bins(filter_groups=filter_groups, depth=depth, should_wait=False)
        # Prepare filter groups
        # for _, filter_group, target_ddf in filter_groups:
        #     start_persist_target_ddf = perf_counter()
        #     target_ddf = target_ddf.persist()
        #     wait(target_ddf)
        #     elapsed_persist_target_ddf = perf_counter() - start_persist_target_ddf
        #     filter_group.prepare(ddf=target_ddf, global_stats=self.global_stats, persist_stats=persist_stats, debug=self.debug)
        # Loop through filter groups
        for index in range(len(filter_groups)):
            # Get filter group and main filter
            _, filter_group, target_ddf = filter_groups[index]
            # Calculate bins
            bins, _ = filter_group.calculate_bins(global_stats=self.global_stats, n_bins=(2**depth))

            start_set_bins = perf_counter()
            filter_group.set_bins(target_ddf, bins)
            elapsed_set_bins = perf_counter() - start_set_bins
            # Persist
            start_persist_target_ddf = perf_counter()
            target_ddf = target_ddf.persist()
            wait(target_ddf)
            elapsed_persist_target_ddf = perf_counter() - start_persist_target_ddf
            # Prepare
            filter_group.prepare(ddf=target_ddf, global_stats=self.global_stats, persist_stats=persist_stats, debug=self.debug)
            # Get main filter
            main_filter = filter_group.main_filter()
            # Get filter group bin ddf
            # bin_ddfs = bin_ddfs_per_filter_group[index]
            
            # Loop through bins
            for bin_index in range(len(bins) - 1):
                # Find bin ddf
                # bin_ddf = bin_ddfs[bin_index]
                # Get tasks for bin
                node = filter_group.create_node(ddf=target_ddf, bin=(bins[bin_index], bins[bin_index + 1]), filter=main_filter)
                node_tasks = node.get_tasks()
                # Append tasks to task list
                tasks[node] = node_tasks
        # Compute all tasks
        counter_start = perf_counter()
        results = dask.compute(*tasks.values())
        counter_end = perf_counter()
        elapsed_time = counter_end - counter_start

        # TODO

        for index, node in enumerate(tasks.keys()):
            node_results = results[index]
            node.forward(node_results)
            print(index)

        analysis = AnalysisNode(filter_group_nodes=filter_group_nodes)
        # Cancel task
        # keep_alive_task.cancel()
        # Return analysis tree
        return analysis

    def _compute_global_stats(self):
        if self.debug:
            print("Computing global stats...")
        # Init stat tasks
        stats_tasks = [
            self.ddf['tend'].max(),
        ]
        # Compute stats
        job_time, = dask.compute(*stats_tasks)
        # Return stats
        return dict(
            job_time=float(job_time)
        )

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
            lines.append(f"{';'.join(reversed(columns))} {stop-start}")
        # Write lines into output file
        with open(output_path, 'w') as file:
            file.write('\n'.join(lines))

    def shutdown(self) -> None:
        self.client.close()
        self.cluster.close()

    def _current_n_workers(self):
        # Get current number of workers
        return len(self.client.scheduler_info()['workers'])

    def __initialize_cluster(self, cluster_options: ClusterOptions):
        # Prepare cluster configuration
        cores = cluster_options.cluster_settings.get('cores', 4)
        processes = cluster_options.cluster_settings.get('processes', 4)
        memory = cluster_options.cluster_settings.get('memory', '{}GB'.format(128))
        use_stdin = cluster_options.cluster_settings.get('use_stdin', True)
        worker_time = cluster_options.cluster_settings.get('worker_time', '02:00')
        worker_queue = cluster_options.cluster_settings.get('worker_queue', 'pdebug')
        log_file = cluster_options.cluster_settings.get('log_file', 'vani.log')
        host = cluster_options.cluster_settings.get('host', socket.gethostname())
        dashboard_address = cluster_options.cluster_settings.get('dashboard_address', '{}:8264'.format(host))
        # Create empty cluster
        cluster = None
        # Check specificed cluster type
        if (cluster_options.cluster_type is ClusterType.Local):
            user = os.environ.get('USER')
            local_directory = os.environ.get('BBPATH', f"/tmp/{user}/vani-analysis-tool")
            cluster = LocalCluster(n_workers=16,
                                   local_directory=local_directory,
                                   silence_logs=logging.DEBUG if self.debug else logging.CRITICAL)
        elif (cluster_options.cluster_type is ClusterType.LSF):
            # Initialize cluster
            cluster = LSFCluster(cores=cores,
                                 processes=processes,
                                 memory=memory,
                                 scheduler_options={'dashboard_address': dashboard_address, 'host': host},
                                 death_timeout=300,
                                 header_skip=['-N', '-R', '-M', '-P', '-W 00:30'],
                                 job_extra=['-nnodes 1',
                                            '-G asccasc',
                                            '-q {}'.format(worker_queue),
                                            '-W {}'.format(worker_time),
                                            '-o {}'.format(log_file),
                                            '-e {}'.format(log_file)],
                                 use_stdin=use_stdin)
        # Print cluster job script
        if self.debug:
            print("Dashboard link:", cluster.dashboard_link)
            if (cluster_options.cluster_type == ClusterType.LSF):
                print(cluster.job_script())
        # Return initialized cluster
        return cluster

    async def __keep_workers_alive(self):
        # While the job is still executing
        while True:
            # Wait a second
            await asyncio.sleep(_WORKER_CHECK_INTERVAL)
            # Check workers
            self.__wait_until_workers_alive()

    def __persist_filter_group_bins(self, filter_groups: Tuple[str, _FilterGroup, DataFrame], depth: int, should_wait=True):
        bin_ddfs_per_filter_group = []
        for _, filter_group, ddf in filter_groups:
            # Calculate bins
            bins, _ = filter_group.calculate_bins(global_stats=self.global_stats, n_bins=(2**depth))
            # Set bins
            start_set_bins = perf_counter()
            filter_group.set_bins(ddf=ddf, bins=bins)
            elapsed_set_bins = perf_counter() - start_set_bins
            # Get bin column
            binned_by = filter_group.binned_by()
            # Persist bin ddf tasks
            bin_ddfs_per_filter_group.append([ddf[ddf[binned_by] == bins[bin_index]].persist() for bin_index in range(len(bins) - 1)])
        if should_wait:
            # Persist bin ddfs
            wait_start = perf_counter()
            wait(bin_ddfs_per_filter_group)
            wait_end = perf_counter()
            if self.debug:
                print("Bins persisted:", wait_end - wait_start)
        return bin_ddfs_per_filter_group

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

    def __wait_until_workers_alive(self):
        # Get current number of workers
        current_n_workers = self._current_n_workers()
        # Wait until enough number of workers alive
        while (self.client.status == 'running' and current_n_workers < self.n_workers):
            # Print current status
            if self.debug:
                print(f"{current_n_workers}/{self.n_workers} workers running", end="\r")
            # Try correcting state
            self.cluster._correct_state()
            # Sleep a little
            sleep(_WORKER_CHECK_INTERVAL)
            # Get current number of workers
            current_n_workers = self._current_n_workers()
        # Print result
        if self.debug:
            print(f"All {self.n_workers} workers alive", end="")
