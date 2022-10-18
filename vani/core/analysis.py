import asyncio
import glob
import json
import dask
import dask.dataframe as dd
import math
import numpy as np
import os
import time
from dask import delayed
from dask.distributed import wait
from hashlib import md5
from logging import Logger
from typing import Dict, List, Tuple
from vani.utils.file_utils import dir_hash, ensure_dir
from vani.utils.logger import ElapsedTimeLogger, format_log

CACHE_DIR = "cached"
METRIC_DIR = "metric"
PARTITION_DIR = "partitioned"


class Analysis(object):

    def __init__(self, working_dir: str, fg_indices: List[str], n_workers_per_node: int, logger: Logger, debug=False) -> None:
        self.id = math.floor(time.time())
        self.debug = debug
        self.fg_ddfs = {}
        self.fg_indices = fg_indices
        self.fg_metrics = {}
        self.fg_min_max_values = {}
        self.logger = logger
        self.n_workers_per_node = n_workers_per_node
        self.working_dir = working_dir

    @staticmethod
    def compute_min_max(log_dir: str, fg_index: str, depth: int):
        with open(f"{log_dir}/global.json") as file:
            global_metrics = json.load(file)
            min_val, max_val = global_metrics[fg_index][0], global_metrics[fg_index][1]
            next_tasks = 2 ** depth
            interval = math.ceil((max_val - min_val) * 1.0 / next_tasks)
            time_range = range(min_val, max_val, interval)
            return time_range, interval

    





    def __compute_metrics(self, fg_index: str):

        fg_ddf = self.fg_ddfs[fg_index]
        min_val, max_val = self.fg_min_max_values[fg_index]

        # noinspection PyShadowingNames,PyShadowingBuiltins
        def filter(start: int, stop: int):
            # Select dataframes
            target_ddf = fg_ddf.loc[start:stop]
            read_ddf = target_ddf[(target_ddf['io_cat'] == 1)]
            write_ddf = target_ddf[(target_ddf['io_cat'] == 2)]
            metadata_ddf = target_ddf[(target_ddf['io_cat'] == 3)]
            # Create tasks
            read_tasks = [
                read_ddf.index.unique() if fg_index == 'proc_id' else read_ddf['proc_id'].unique(),
                read_ddf['duration'].sum(),
                read_ddf['size'].sum(),
                read_ddf.index.unique() if fg_index == 'file_id' else read_ddf['file_id'].unique(),
                read_ddf['bandwidth'].sum(),
                read_ddf['index'].count(),
            ]
            write_tasks = [
                write_ddf.index.unique() if fg_index == 'proc_id' else write_ddf['proc_id'].unique(),
                write_ddf['duration'].sum(),
                write_ddf['size'].sum(),
                write_ddf.index.unique() if fg_index == 'file_id' else write_ddf['file_id'].unique(),
                write_ddf['bandwidth'].sum(),
                write_ddf['index'].count(),
            ]
            metadata_tasks = [
                metadata_ddf.index.unique() if fg_index == 'proc_id' else metadata_ddf['proc_id'].unique(),
                metadata_ddf['duration'].sum(),
                metadata_ddf.index.unique() if fg_index == 'file_id' else metadata_ddf['file_id'].unique(),
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
                'start': start,
                'stop': stop,
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
                'start': min(x['start'], y['start']),
                'stop': max(x['stop'], y['stop']),
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
                    tasks.append(dask.delayed(filter)(start, stop))
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
                    all_tasks[i + 1][t] = dask.delayed(self.__digest)(next_tasks)
            all_tasks[i] = tasks
        # noinspection PyTypeChecker
        for t, next_tasks in enumerate(all_tasks[0]):
            # noinspection PyUnresolvedReferences
            all_tasks[0][t] = dask.delayed(self.__digest)(next_tasks)

        # with ElapsedTimeLogger(logger=self.logger, message="Metrics computed", caller=fg_index):
        #     with get_task_stream(client=self.clients[fg_index],
        #                          filename=self.__path_with_id(f"{fg_index}-metrics.html"),
        #                          plot='save'):
        #         metrics_futures = self.clients[fg_index].compute(all_tasks)
        #         result = wait(metrics_futures)
        #         metrics = self.clients[fg_index].gather(metrics_futures)

        metrics = []
        return metrics

    def __compute_min_max(self, fg_index: str, log_dir: str) -> Tuple[int, int]:
        # Find json key
        # json_key = JSON_KEYS[fg_index]

        # Define functions
        def min_max_of_array(arr: List):
            return arr
            # return functools.reduce(lambda x, y: (min(x[0], y), max(x[1], y)), arr, (arr[0], arr[0],))

        def min_max_of_global_metric():
            with open(f"{log_dir}/global.json") as global_metrics_file:
                global_metrics = json.load(global_metrics_file)
                return 0, math.ceil(global_metrics[fg_index])

        def min_max_of_json_values(json_file_path: str):
            with open(json_file_path) as json_file:
                json_data = json.load(json_file)
                return min_max_of_array(json_data[fg_index])

        # If JSON key is tmid, then compute min & max through global metrics
        if fg_index == 'tmid':
            min_val, max_val = min_max_of_global_metric()
            self.logger.debug(format_log(fg_index, f"Min-max of {fg_index}: {min_val}-{max_val}"))
            return min_val, max_val

        # Read rank files
        rank_files = glob.glob(f"{log_dir}/rank_*.json", recursive=True)
        n_rank_files = len(rank_files)

        # Do not run distributed tasks if there is only a single rank
        if n_rank_files == 1:
            min_val, max_val = min_max_of_json_values(json_file_path=rank_files[0])
            self.logger.debug(format_log(fg_index, f"Min-max of {fg_index}: {min_val}-{max_val}"))
            return min_val, max_val

        # Otherwise run it distributed
        with self.clients[fg_index].as_current():
            depth = math.ceil(math.sqrt(n_rank_files))
            iterations = list(range(0, depth + 1))
            iterations.reverse()
            all_tasks = [0] * (depth + 1)
            with ElapsedTimeLogger(logger=self.logger, message="Min-max tasks arranged", caller=fg_index):
                for i in iterations:
                    tasks = []
                    if i == depth:
                        for rank_file in rank_files:
                            tasks.append(dask.delayed(min_max_of_json_values)(rank_file))
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
            # with get_task_stream(client=self.clients[fg_index],
            #                      filename=self.__path_with_id(f"{json_key}-min-max.html"),
            #                      plot='save'):
            #     to_be_vis = dask.delayed(all_tasks)()
            #     to_be_vis.dask.visualize(filename=self.__path_with_id(f"{json_key}-min-max.svg"))
            #     # self.clients[filter_group_index].cluster.scheduler.plugins['progress'].initialize(to_be_vis)
            #     with ElapsedTimeLogger(logger=self.logger,
            #                            message="Min-max tasks completed",
            #                            caller=fg_index):
            #         values = dask.compute(all_tasks)
                # self.clients[filter_group_index].cluster.scheduler.plugins['progress'].finalize()

        # Find min max values
        values = []
        min_val, max_val = values[0][0][0][0], values[0][0][0][1]
        self.logger.debug(format_log(fg_index, f"Min-max of {fg_index}: {min_val}-{max_val}"))

        # Return min max values
        return min_val, max_val

    @staticmethod
    def __digest(x: Dict):
        return {
            'start': x['start'],
            'stop': x['stop'],
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

    def __ensure_logs_partitioned(self, log_dir: str):
        # Ensure logs partitioned
        is_partitioned = os.path.exists(f"{log_dir}/{PARTITION_DIR}/_common_metadata")
        self.logger.debug(format_log("main", f"Logs partitioned before: {is_partitioned}"))
        files = glob.glob(f"{log_dir}/{PARTITION_DIR}/*.parquet", recursive=True)
        return files if is_partitioned else self.__partition_logs(log_dir=log_dir)

    def __indexed_ddf(self, fg_index: str, log_dir: str):
        cache_dir = f"{log_dir}/{CACHE_DIR}/{fg_index}"
        partition_dir = f"{log_dir}/{PARTITION_DIR}"
        with ElapsedTimeLogger(logger=self.logger, message="Logs hash calculated", caller=fg_index):
            partition_dir_hash = dir_hash(partition_dir)
        partition_dir_hash_file = f"{self.working_dir}/_{md5(partition_dir.encode()).hexdigest()}"
        if os.path.exists(partition_dir_hash_file):
            with open(partition_dir_hash_file, 'r') as file:
                partition_dir_hash_ex = file.read().rstrip()
            if partition_dir_hash == partition_dir_hash_ex and os.path.exists(cache_dir):
                with self.clients[fg_index].as_current():
                    with ElapsedTimeLogger(logger=self.logger, message="Logs read from cache",
                                           caller=fg_index):
                        indexed_ddf = dd.read_parquet(cache_dir, index=fg_index)
                with ElapsedTimeLogger(logger=self.logger, message="Logs persisted", caller=fg_index):
                    # with get_task_stream(client=self.clients[fg_index],
                    #                      filename=self.__path_with_id(f"{fg_index}-index-ddf.html"),
                    #                      plot='save'):
                    indexed_ddf = self.clients[fg_index].persist(indexed_ddf)
                    wait(indexed_ddf)
                return indexed_ddf
        with self.clients[fg_index].as_current():
            with ElapsedTimeLogger(logger=self.logger, message="Logs read", caller=fg_index):
                ddf = dd.read_parquet(f"{log_dir}/{PARTITION_DIR}/*.parquet", index=False)
            with ElapsedTimeLogger(logger=self.logger, message="Logs indexed", caller=fg_index):
                indexed_ddf = ddf.set_index([fg_index])
        with ElapsedTimeLogger(logger=self.logger, message="Logs persisted", caller=fg_index):
            # with get_task_stream(client=self.clients[fg_index],
            #                      filename=self.__path_with_id(f"{fg_index}-index-ddf.html"),
            #                      plot='save'):
            indexed_ddf = self.clients[fg_index].persist(indexed_ddf)
            wait(indexed_ddf)
        # TODO without client.as_current this gives error and says the indexed_ddf isn't available on the local cluster
        # TODO so, how should we do it async even though setting priority might not be of help
        with ElapsedTimeLogger(logger=self.logger, message="Logs cached into disk", caller=fg_index):
            # Persist indexed ddf into disk
            with self.clients[fg_index].as_current():
                # dd.to_parquet(indexed_ddf, path=cache_dir)
                indexed_ddf = indexed_ddf.repartition(partition_size='4GB')
                indexed_ddf.to_parquet(cache_dir)
        self.clients[fg_index].cancel(indexed_ddf)
        with self.clients[fg_index].as_current():
            with ElapsedTimeLogger(logger=self.logger, message="Logs read from cache",
                                   caller=fg_index):
                read_indexed_ddf = dd.read_parquet(cache_dir, index=fg_index)
        with ElapsedTimeLogger(logger=self.logger, message="Logs persisted again", caller=fg_index):
            # with get_task_stream(client=self.clients[fg_index],
            #                      filename=self.__path_with_id(f"{fg_index}-index-ddf.html"),
            #                      plot='save'):
            read_indexed_ddf = self.clients[fg_index].persist(read_indexed_ddf)
            wait(read_indexed_ddf)

        # Persist hash
        with open(partition_dir_hash_file, 'w') as file:
            file.write(partition_dir_hash)
        # TODO: cache the index dataset for future use. Utilize md5 hash on logs folder to see if anything is
        #  changed if not use cache if present
        return read_indexed_ddf

    def __partition_logs(self, log_dir: str, partition_size="4GB"):
        # Remove existing files
        existing_files = glob.glob(f"{log_dir}/{PARTITION_DIR}/*.parquet", recursive=True)
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
        partition_folder = f"{log_dir}/{PARTITION_DIR}/"
        with ElapsedTimeLogger(logger=self.logger, message=f"Partitions written into: {partition_folder}"):
            dd.to_parquet(ddf, partition_folder)
        # Return partitioned files
        partitioned_files = glob.glob(f"{log_dir}/{PARTITION_DIR}/*.parquet", recursive=True)
        return partitioned_files

    def __path_with_id(self, path: str):
        ensure_dir(f"{self.working_dir}/{self.id}")
        return f"{self.working_dir}/{self.id}/{path}"
