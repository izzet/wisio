# Import system lib
import asyncio
import os
import socket
import dask
from enum import Enum
from time import perf_counter, sleep
from typing import Any

# Import third parties
import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.dataframe import DataFrame, Series
from dask.distributed import Client, LocalCluster, wait
from dask_jobqueue import LSFCluster
from tqdm.auto import tqdm


# Import locals
from vani.common.nodes import Node
from vani.common.filter_groups import TimelineFilterGroup
from vani.common.filters import BandwidthFilter, DurationFilter, IOSizeFilter, ParallelismFilter
from vani.utils.data_aug import set_bins
from vani.utils.data_filtering import filter_non_io_traces, split_io_mpi_trace, split_read_write_metadata
from vani.utils.print_utils import print_header, print_tabbed


# Define constants
WORKER_CHECK_INTERVAL = 5.0


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
        self.n_bins = 10
        self.n_workers = n_workers

        # Declare vars
        n_steps = 3

        # Start progress
        pbar = tqdm(total=n_steps)

        # Initialize cluster
        pbar.set_description(f"Initializing {cluster_options.cluster_type.name} cluster")
        self.cluster = self.__initialize_cluster(cluster_options=cluster_options)
        pbar.update()

        # Initialize client
        pbar.set_description("Initializing Dask client")
        self.client = Client(self.cluster)
        pbar.update()

        # Scale cluster
        pbar.set_description(f"Scaling up the cluster to {n_workers} nodes")
        if (cluster_options.cluster_type != ClusterType.Local):
            self.cluster.scale(n_workers)
            self.__wait_until_workers_alive()
        pbar.update()

        # Close progress
        pbar.set_description("Analyzer initialized 4")
        pbar.close()

    def analyze_parquet_logs(self, log_dir: str, granularity=0.2):
        # Keep workers alive
        # keep_alive_task = asyncio.create_task(self.__keep_workers_alive())

        # Declare vars
        # Start progress
        n_steps = 7
        pbar = tqdm(total=n_steps)

        # Read logs into a dataframe
        pbar.set_description("Reading logs into dataframe")
        ddf = self.__read_parquet(log_dir=log_dir)
        pbar.update()

        # Compute job time
        pbar.set_description("Computing job time")
        #! Could be delayed, we will see
        job_time = self._compute_timed("Job time", ddf['tend'].max())
        pbar.update()

        # Filter non-I/O traces (except for MPI)
        # Split dataframe into I/O, MPI and trace
        # Split io_df into read & write and metadata dataframes
        ddf = filter_non_io_traces(ddf)
        io_ddf, mpi_ddf, trace_ddf = split_io_mpi_trace(ddf)
        io_ddf_read_write, io_ddf_metadata = split_read_write_metadata(io_ddf)

        # Compute stats
        max_duration = self._compute_timed("Max duration", DurationFilter.on(io_ddf_read_write))
        mean_bw = self._compute_timed("Mean BW", BandwidthFilter.on(ddf=io_ddf_read_write))
        n_ranks = self._compute_timed("# Ranks", ParallelismFilter.on(ddf=io_ddf_read_write))
        total_size = self._compute_timed("Total size", IOSizeFilter.on(ddf=io_ddf_read_write))

        print("---------------")

        # Define filter groups
        filter_groups = [
            TimelineFilterGroup(job_time=job_time,
                                total_size=total_size,
                                mean_bw=mean_bw,
                                max_duration=max_duration,
                                n_ranks=n_ranks,
                                n_bins=2)
        ]
        # Loop through filter groups
        for filter_group in filter_groups:
            # Get filters
            filters = filter_group.filters()
            # Loop through filters
            for filter in filters:
                # Init tasks
                nodes = []
                # Create root node
                root = filter_group.create_node(ddf=io_ddf_read_write, bin=(0, job_time), filter=filter, label='root')
                nodes.append(root)
                # Run tasks
                while nodes:
                    # Read nodes
                    node = nodes.pop()
                    # Analyze node
                    potential_bottlenecks, bins, bin_step = node.analyze()
                    # Analyze potential bottlenecks
                    for index, potential_bottleneck in enumerate(potential_bottlenecks.index.array):
                        # Create a node
                        nodes.append(filter_group.create_node(ddf=node.ddf[node.ddf['tbin'] == potential_bottleneck],
                                                              bin=(potential_bottleneck, potential_bottleneck + bin_step),
                                                              filter=filter,
                                                              label=potential_bottlenecks[index]))

                    print("HEY")

        # Find problematic sizes by bin first
        problematic_size_bin_tasks = []
        problematic_size_bin_tasks.append((io_ddf_read_write, 0, job_time, "root", total_size, 0))

        while len(problematic_size_bin_tasks) > 0:
            parent_df, size_bin, bin_step, path, size, depth = problematic_size_bin_tasks.pop()
            # print_header(f"Finding problematic sizes by bin - depth: {depth}", tab_indent=depth)
            print_tabbed(f"[{size_bin}-{size_bin + bin_step}] {path} ({size} {size/total_size})", tab_indent=depth)
            problematic_size_bin_df = parent_df[parent_df["tbin"] == size_bin] if size_bin > 0 else parent_df
            problematic_size_bin_task, problematic_size_bin_step = self._prepare_compute_sizes_by_bin(
                df=problematic_size_bin_df,
                start=size_bin,
                stop=size_bin + bin_step,
                granularity=granularity
            )
            problematic_bws_bin_task, problematic_bw_bin_step = self._prepare_compute_bws_by_bin(
                df=problematic_size_bin_df,
                start=size_bin,
                stop=size_bin + bin_step,
                granularity=granularity
            )
            if problematic_size_bin_task is None:
                continue
                print_tabbed(f"No need to dig further because {problematic_size_bin_step} < {max_duration}", tab_indent=depth + 1)

                # problematic_bw_bin_task = self._prepare_compute_bw_by_bin(problematic_size_bin_df)
                # print(problematic_bw_bin_task.compute())

            else:
                problematic_bws_bin_task_result = problematic_bws_bin_task.compute()
                problematic_bws_by_bin, bws_by_bin = self._extract_problematic_sizes_by_bin(problematic_bws_bin_task_result, depth, True)
                # print(problematic_bws_by_bin)

                problematic_size_bin_task_result = problematic_size_bin_task.compute()
                problematic_sizes_by_bin, sizes_by_bin = self._extract_problematic_sizes_by_bin(problematic_size_bin_task_result, depth)
                for index, label in enumerate(problematic_sizes_by_bin):
                    problematic_size_bin = problematic_sizes_by_bin.index.array[index]
                    problematic_size_bin_path = f"{path}..{label}"
                    problematic_size_bin_size = sizes_by_bin.array[index]
                    problematic_size_bin_tasks.append((problematic_size_bin_df,
                                                       problematic_size_bin,
                                                       problematic_size_bin_step,
                                                       problematic_size_bin_path,
                                                       problematic_size_bin_size,
                                                       depth + 1))

        # Close progress
        pbar.set_description("Analysis completed")
        pbar.close()

        # Cancel task
        # keep_alive_task.cancel()

        return io_df_read_write, job_time

    def _extract_problematic_sizes_by_bin(self, sizes_by_bin: Series, depth=0, debug=False):
        # Prepare labels
        labels = [label for label in range(1, self.n_bins + 1)]
        # Arrange size bins
        fixed_sizes_by_bin = sizes_by_bin.sort_values(ascending=False)
        if depth == 0:
            # Add zeroed bin to fix labeling, this has no effect if there's already 0-indexed bins
            dummy_bin = pd.Series([0], index=[0])
            fixed_sizes_by_bin = sizes_by_bin.add(dummy_bin, fill_value=0).sort_values(ascending=False)
        # Label size bins
        labeled_sizes_by_bin = pd.cut(fixed_sizes_by_bin, bins=self.n_bins, labels=labels)  # , precision=20)
        # Flag >50% problematic by default
        problematic_sizes_by_bin = labeled_sizes_by_bin[labeled_sizes_by_bin > int(self.n_bins / 2)]
        # if debug:
        #     print_tabbed("{:<25} {:<25} {:<10} {:<15}".format('bin', 'size', 'label', 'problematic'), tab_indent=depth)
        #     for index, label in enumerate(labeled_sizes_by_bin):
        #         bin = labeled_sizes_by_bin.index.array[index]
        #         size = fixed_sizes_by_bin.array[index]
        #         problematic = 'x' if label in problematic_sizes_by_bin.array else ''
        #         print_tabbed("{:<25} {:<25} {:<10} {:<15}".format(bin, size, label, problematic), tab_indent=depth)
        # Return problematic sizes by bin
        return problematic_sizes_by_bin, fixed_sizes_by_bin

    def _calculate_bins_for_time_range(self, start: float, stop: float):
        # Return linear space between start and stop
        return np.linspace(start, stop, num=self.n_bins, retstep=True)

    def _prepare_compute_bws_by_bin(self, df: DataFrame, start: float, stop: float, granularity: float):
        # Calculate bins
        bins, step = self._calculate_bins_for_time_range(start, stop)
        # Check whether step is less than granularity
        if step < granularity:
            # Then do not analysis
            return None, step
        # Set bins
        set_bins(df, bins, step)
        # Compute bin sizes
        return df.groupby("tbin")["bandwidth"].mean()/1024.0, step

    def _prepare_compute_sizes_by_bin(self, df: DataFrame, start: float, stop: float, granularity: float):
        # Calculate bins
        bins, step = self._calculate_bins_for_time_range(start, stop)
        # Check whether step is less than granularity
        if step < granularity:
            # Then do not analysis
            return None, step
        # Set bins
        set_bins(df, bins, step)
        # Compute bin sizes
        return df.groupby("tbin")["size"].sum()/1024.0/1024.0/1024.0, step

    def _compute_bin_stats(self, df: DataFrame):
        tbin_grpd = df.groupby("tbin")

        tbin_grp_size_task = tbin_grpd["size"].sum()/1024.0/1024.0/1024.0
        tbin_grp_bw_task = tbin_grpd["bandwidth"].mean()/1024.0
        tbin_grp_ct_task = tbin_grpd["index"].count()

        return dask.compute(tbin_grp_size_task, tbin_grp_bw_task, tbin_grp_ct_task)

    def _compute_timed(self, title: str, task: Any):
        # Compute job time
        t_start = perf_counter()
        value = task.compute()
        t_end = perf_counter()
        # Print performance
        if self.debug:
            print(f"{title}: {value} ({t_end - t_start})")
        # Return job time
        return value

    def _current_n_workers(self):
        # Get current number of workers
        return len(self.client.scheduler_info()["workers"])

    def _persist(self, df: DataFrame):
        # Persist data frame
        t_start = perf_counter()
        df = df.persist()
        wait(df)
        t_end = perf_counter()
        t_elapsed = t_end - t_start
        # Print performance
        if self.debug:
            print(f"Persisting dataframe took {t_elapsed} seconds")
        # Return job time
        return df, t_elapsed

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

    async def __keep_workers_alive(self):
        # While the job is still executing
        while True:
            # Wait a second
            await asyncio.sleep(WORKER_CHECK_INTERVAL)
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
            sleep(WORKER_CHECK_INTERVAL)
            # Get current number of workers
            current_n_workers = self._current_n_workers()
        # Print result
        if self.debug:
            print(f"All {self.n_workers} workers alive", end="")
