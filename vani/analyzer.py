import asyncio
import dask.dataframe as dd
import os
import socket
from anytree import PostOrderIter
from dask.dataframe import DataFrame
from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
from enum import Enum
from time import perf_counter, sleep
from vani.common.filter_groups import TimelineMetadataFilterGroup, TimelineReadWriteFilterGroup
from vani.common.interfaces import _FilterGroup
from vani.common.nodes import AnalysisNode, BinNode, FilterGroupNode
from vani.utils.data_filtering import filter_non_io_traces, split_io_mpi_trace, split_read_write_metadata

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

    def analyze_parquet_logs(self, log_dir: str, max_depth=3, persist_stats=True, stats_file_prefix=""):
        # Keep workers alive
        # keep_alive_task = asyncio.create_task(self.__keep_workers_alive())

        # Read logs into a dataframe
        ddf = self._read_parquet(log_dir=log_dir)

        # Filter non-I/O traces (except for MPI)
        # Split dataframe into I/O, MPI and trace
        # Split io_df into read & write and metadata dataframes
        ddf = filter_non_io_traces(ddf)
        io_ddf, mpi_ddf, trace_ddf = split_io_mpi_trace(ddf)
        io_ddf_read, io_ddf_write, io_ddf_metadata = split_read_write_metadata(io_ddf)

        self.io_ddf = io_ddf

        # Compute stats
        # TODO(hari): we can add this part of convertor to calculate the max of tend.
        job_time = ddf['tend'].max().compute()
        n_bins = 10

        # Define filter groups
        filter_groups = [
            ("Timeline - Read", TimelineReadWriteFilterGroup(job_time=job_time, n_bins=n_bins, stats_file_prefix=stats_file_prefix), io_ddf_read),
            ("Timeline - Write", TimelineReadWriteFilterGroup(job_time=job_time, n_bins=n_bins, stats_file_prefix=stats_file_prefix), io_ddf_write),
            ("Timeline - Metadata", TimelineMetadataFilterGroup(job_time=job_time, n_bins=n_bins, stats_file_prefix=stats_file_prefix), io_ddf_metadata)
        ]

        filter_group_nodes = []

        # Loop through filter groups
        for title, filter_group, target_ddf in filter_groups:
            # Analyze filter group
            filter_group_node = self._analyze_filter_group(title=title,
                                                           filter_group=filter_group,
                                                           ddf=target_ddf,
                                                           max_depth=max_depth,
                                                           persist_stats=persist_stats)
            filter_group_nodes.append(filter_group_node)

        analysis = AnalysisNode(filter_group_nodes=filter_group_nodes)

        # Cancel task
        # keep_alive_task.cancel()

        return analysis

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

    def _analyze_filter_group(self, title: str, filter_group: _FilterGroup, ddf: DataFrame, max_depth: int, persist_stats=True) -> FilterGroupNode:
        # Create filter group node
        filter_group_node = FilterGroupNode(title=title, filter_group=filter_group)
        # Prepare filter group
        filter_group.prepare(ddf=self.io_ddf, persist_stats=persist_stats, debug=self.debug)
        # Get filters
        filters = filter_group.filters()
        # Loop through filters
        for filter in filters:
            # Init tasks
            nodes = []
            # Create root node
            root = filter_group.create_root(ddf=ddf, filter=filter, parent=filter_group_node)
            nodes.append(root)
            # Run tasks
            while nodes:
                # Read nodes
                node = nodes.pop()
                # Analyze node
                bottlenecks, score = node.analyze()
                # If node is too way deep, stop further analysis
                if node.depth > max_depth:
                    continue
                # Loop through bottlenecks
                for bottleneck_bin in bottlenecks.index.array:
                    # Read bin info
                    start, stop = node.bin
                    # Set next bins
                    bins, _ = filter_group.next_bins(start=start, stop=stop)
                    binned_by = filter_group.binned_by()
                    bottleneck_ddf = node.ddf[node.ddf[binned_by] == bottleneck_bin]
                    filter_group.set_bins(ddf=bottleneck_ddf, bins=bins)
                    # Then loop through next bins
                    for bin_index in range(len(bins) - 1):
                        # Read bin dataframe
                        bin_ddf = bottleneck_ddf[bottleneck_ddf[binned_by] == bins[bin_index]]
                        # Add bin node to analysis queue
                        nodes.append(filter_group.create_node(ddf=bin_ddf,
                                                              bin=(bins[bin_index], bins[bin_index + 1]),
                                                              filter=filter,
                                                              parent=node))
            # Finish analysis
            break
        # Return analyzed node
        return filter_group_node

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
            cluster = LocalCluster(n_workers=8,
                                   local_directory=local_directory)
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
