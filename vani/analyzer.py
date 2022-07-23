import asyncio
import dask
import dask.dataframe as dd
import os
import socket
from dask.dataframe import DataFrame
from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
from enum import Enum
from time import perf_counter, sleep
from vani.common.constants import SECONDS_FORMAT
from vani.common.filter_groups import TimelineFilterGroup
from vani.common.filters import BandwidthFilter, DurationFilter, IOSizeFilter, ParallelismFilter
from vani.common.nodes import AnalysisNode, FilterGroupNode
from vani.utils.data_filtering import filter_non_io_traces, split_io_mpi_trace, split_read_write_metadata


# Define constants
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

    def analyze_parquet_logs(self, log_dir: str, granularity=0.2):
        # Keep workers alive
        # keep_alive_task = asyncio.create_task(self.__keep_workers_alive())

        # Read logs into a dataframe
        ddf = self.__read_parquet(log_dir=log_dir)

        # Filter non-I/O traces (except for MPI)
        # Split dataframe into I/O, MPI and trace
        # Split io_df into read & write and metadata dataframes
        ddf = filter_non_io_traces(ddf)
        io_ddf, mpi_ddf, trace_ddf = split_io_mpi_trace(ddf)
        io_ddf_read_write, io_ddf_metadata = split_read_write_metadata(io_ddf)

        # Compute stats
        job_time, io_time, max_duration, mean_bw, total_ranks, total_size = self.__compute_stats(ddf=ddf, io_ddf_read_write=io_ddf_read_write)

        print("---------------")

        # Define filter groups
        filter_groups = [
            TimelineFilterGroup(job_time=job_time,
                                total_size=total_size,
                                mean_bw=mean_bw,
                                max_duration=max_duration,
                                total_ranks=total_ranks,
                                n_bins=2)
        ]

        filter_group_nodes = []

        # Loop through filter groups
        for filter_group in filter_groups:

            filter_group_node = FilterGroupNode(filter_group=filter_group)
            filter_group_nodes.append(filter_group_node)
            # Get filters
            filters = filter_group.filters()
            # Loop through filters
            for filter in filters:
                print("Filter: ", filter.name())
                print("-----")
                # Init tasks
                nodes = []
                # Create root node
                filter_group.set_bins(ddf=io_ddf_read_write, bins=[0, job_time])
                root = filter_group.create_node(ddf=io_ddf_read_write, bin=(0, job_time), filter=filter, parent=filter_group_node)
                nodes.append(root)
                # Run tasks
                while nodes:
                    # Read nodes
                    node = nodes.pop()
                    # Analyze node
                    bottlenecks = node.analyze()
                    for index, bottleneck_bin in enumerate(bottlenecks.index.array):
                        start, stop = node.bin
                        bins, bin_step = filter_group.next_bins(start=start, stop=stop)
                        bottleneck_ddf = node.ddf[node.ddf['tbin'] == bottleneck_bin]
                        filter_group.set_bins(ddf=bottleneck_ddf, bins=bins)
                        for bin_index in range(len(bins) - 1):
                            bin_ddf = bottleneck_ddf[bottleneck_ddf['tbin'] == bins[bin_index]]
                            # Create a node
                            nodes.append(filter_group.create_node(ddf=bin_ddf,
                                                                  bin=(bins[bin_index], bins[bin_index + 1]),
                                                                  filter=filter,
                                                                  parent=node))

            # filter_group_node.render_tree()

        analysis = AnalysisNode(filter_group_nodes=filter_group_nodes)
        analysis.render_tree()

        # Cancel task
        # keep_alive_task.cancel()

    def _current_n_workers(self):
        # Get current number of workers
        return len(self.client.scheduler_info()["workers"])

    def __initialize_cluster(self, cluster_options: ClusterOptions):
        # Prepare cluster configuration
        cores = cluster_options.cluster_settings.get("cores", 4)
        processes = cluster_options.cluster_settings.get("processes", 4)
        memory = cluster_options.cluster_settings.get("memory", '{}GB'.format(128))
        use_stdin = cluster_options.cluster_settings.get("use_stdin", True)
        worker_time = cluster_options.cluster_settings.get("worker_time", "02:00")
        worker_queue = cluster_options.cluster_settings.get("worker_queue", "pdebug")
        log_file = cluster_options.cluster_settings.get("log_file", "vani.log")
        host = cluster_options.cluster_settings.get("host", socket.gethostname())
        dashboard_address = cluster_options.cluster_settings.get("dashboard_address", '{}:8264'.format(host))
        # Create empty cluster
        cluster = None
        # Check specificed cluster type
        if (cluster_options.cluster_type is ClusterType.Local):
            os.environ["BBPATH"] = os.environ.get('BBPATH', "/tmp")
            cluster = LocalCluster(n_workers=8,
                                   local_directory=os.environ["BBPATH"])
        elif (cluster_options.cluster_type is ClusterType.LSF):
            # Initialize cluster
            cluster = LSFCluster(cores=cores,
                                 processes=processes,
                                 memory=memory,
                                 scheduler_options={"dashboard_address": dashboard_address, "host": host},
                                 death_timeout=300,
                                 header_skip=['-n', '-R', '-M', '-P', '-W 00:30'],
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

    def __compute_stats(self, ddf: DataFrame, io_ddf_read_write: DataFrame):
        # Create I/O time metrics
        io_tend = io_ddf_read_write['tend'].max()
        io_tstart = io_ddf_read_write['tstart'].min()
        # Init stat tasks
        stats_tasks = {
            "Job time": ddf['tend'].max(),
            "I/O time": (io_tend - io_tstart),
            "Max duration": DurationFilter.on(io_ddf_read_write),
            "Mean BW": BandwidthFilter.on(ddf=io_ddf_read_write),
            "Total ranks": ParallelismFilter.on(ddf=io_ddf_read_write),
            "Total size": IOSizeFilter.on(ddf=io_ddf_read_write)
        }
        # Compute stats
        job_time, io_time, max_duration, mean_bw, total_ranks, total_size = dask.compute(*stats_tasks.values())
        stats = [job_time, io_time, max_duration, mean_bw, total_ranks, total_size]
        # Print stats
        if self.debug:
            for index, stat in enumerate(stats_tasks):
                print(f"{stat}: {SECONDS_FORMAT.format(stats[index])}")
        # Return stats
        return job_time, io_time, max_duration, mean_bw, total_ranks, total_size

    async def __keep_workers_alive(self):
        # While the job is still executing
        while True:
            # Wait a second
            await asyncio.sleep(_WORKER_CHECK_INTERVAL)
            # Check workers
            self.__wait_until_workers_alive()

    def __read_parquet(self, log_dir: str, engine="pyarrow-dataset"):
        # Read logs into a dataframe
        t_start = perf_counter()
        df = dd.read_parquet("{}/*.parquet".format(log_dir), engine=engine)
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
        while (self.client.status == "running" and current_n_workers < self.n_workers):
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
