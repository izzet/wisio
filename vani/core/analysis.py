import asyncio
import dask
import dask.bag as db
import dask.dataframe as dd
import glob
import json
import math
import os
import time
from dask import delayed
from dask.dataframe import DataFrame
from dask.distributed import wait
from hashlib import md5
from logging import Logger
from typing import Dict
from vani.core.metrics import filter_delayed, flatten_delayed, merge_delayed
from vani.core.scores import min_max_delayed, score
from vani.utils.file_utils import dir_hash, ensure_dir
from vani.utils.json_encoders import NpEncoder
from vani.utils.logger import ElapsedTimeLogger, format_log

CACHE_DIR = "cached"
METRIC_DIR = "metric"
PARTITION_DIR = "partitioned"


class Analysis(object):

    def __init__(self, log_dir: str, working_dir: str, n_workers_per_node: int, logger: Logger,
                 depth=10, debug=False) -> None:
        self.id = math.floor(time.time())
        self.debug = debug
        self.depth = depth
        self.fg_index = None
        self.global_min_max = None
        self.log_dir = log_dir
        self.logger = logger
        self.n_workers_per_node = n_workers_per_node
        self.working_dir = working_dir

    def compute_metrics_file_id(self, ddf: DataFrame):
        unique_filenames_d = self.unique_filenames_delayed(ddf=ddf, log_dir=self.log_dir,
                                                           dask_key_name=f"unique-filenames-{self.fg_index}")
        save_filenames_d = self.save_filenames_delayed(filenames=unique_filenames_d, log_dir=self.log_dir,
                                                       dask_key_name=f"save-filenames-{self.fg_index}")
        metrics_d = self.metrics_filenames_delayed(ddf=ddf, filenames=unique_filenames_d,
                                                   dask_key_name=f"metrics-{self.fg_index}")
        return [unique_filenames_d, save_filenames_d, metrics_d]

    def compute_metrics_proc_id(self, ddf: DataFrame):
        unique_processes_d = self.unique_processes_delayed(ddf=ddf, dask_key_name=f"unique-processes-{self.fg_index}")
        # hostname_ids_d = self.hostname_ids_delayed(ddf=unique_processes_d,
        #                                            dask_key_name=f"hostname-ids-{self.fg_index}")
        metrics_d = self.metrics_proc_id_delayed(ddf=ddf, processes=unique_processes_d,
                                                 dask_key_name=f"metrics-{self.fg_index}")
        return [unique_processes_d, metrics_d]

    def compute_metrics_tmid(self, ddf: DataFrame):
        min_val, max_val = tuple(self.global_min_max[self.fg_index])
        next_tasks = 2 ** self.depth
        interval = math.ceil((max_val - min_val) * 1.0 / next_tasks)
        time_range = range(min_val, max_val, interval)
        iterations = list(range(0, self.depth + 1))
        iterations.reverse()
        all_tasks = [0] * (self.depth + 1)
        for i in iterations:
            tasks = []
            if i == self.depth:
                for start in time_range:
                    stop = start + interval - 1
                    target_ddf_d = self.target_ddf_delayed(ddf=ddf, start=start, stop=stop,
                                                           dask_key_name=f"target-ddf-{self.fg_index}-{start}-{stop}")
                    filter_d = filter_delayed(ddf=target_ddf_d, fg_index=self.fg_index, start=start, stop=stop,
                                              dask_key_name=f"filter-{self.fg_index}-{start}-{stop}")
                    tasks.append(filter_d)
            else:
                Analysis.__arrange_merge_tasks(self.fg_index, all_tasks, tasks, i)
            all_tasks[i] = tasks
        return Analysis.__arrange_high_level_tasks(self.fg_index, all_tasks)

    def compute_observations(self):
        pass

    def compute_scores(self, metrics: list):
        # print("---compute_scores---")
        # self.logger.debug(metrics)
        # print(metrics)
        root_metric = metrics[-1][0]
        # print(root_metric)
        min_max_d = min_max_delayed(metric=root_metric, global_min_max=self.global_min_max,
                                    dask_key_name=f"min-max-{self.fg_index}")
        if self.fg_index == 'tmid':
            metrics.reverse()
        metrics_bag = db.from_delayed(metrics)
        score.__name__ = f"scores-{self.fg_index}"
        # scores_map = metrics_bag.map(score, min_max_d)
        if self.fg_index == 'tmid':
            scores = metrics_bag.map(score, min_max_d)
        else:
            scores = metrics_bag.flatten().map(score, min_max_d)
        return scores.to_delayed()

    def load_global_min_max(self):
        with open(f"{self.log_dir}/global.json") as file:
            global_min_max = json.load(file)
        print("global_min_max", global_min_max)
        self.global_min_max = global_min_max
        return global_min_max

    def read_and_index_logs(self, use_cache=True):
        fg_partition_dir = f"{self.log_dir}/{PARTITION_DIR}/{self.fg_index}"
        if use_cache and os.path.exists(f"{fg_partition_dir}/_metadata"):
            ddf_d = self.read_parquet_delayed(log_dir=fg_partition_dir, index=[self.fg_index],
                                              dask_key_name=f"read-parquet-{self.fg_index}")
            persisted_ddf_d = self.persist_ddf_delayed(ddf=ddf_d, dask_key_name=f"persist-ddf-{self.fg_index}")
            return [ddf_d, persisted_ddf_d]
        ddf_d = self.read_parquet_delayed(log_dir=self.log_dir, dask_key_name=f"read-parquet-{self.fg_index}")
        indexed_ddf_d = self.set_ddf_index_delayed(ddf=ddf_d, fg_index=self.fg_index,
                                                   dask_key_name=f"set-index-{self.fg_index}")
        save_ddf_d = self.save_ddf_delayed(ddf=indexed_ddf_d, log_dir=self.log_dir, fg_index=self.fg_index,
                                           dask_key_name=f"save-ddf-{self.fg_index}")
        persisted_ddf_d = self.persist_ddf_delayed(ddf=indexed_ddf_d, dask_key_name=f"persist-ddf-{self.fg_index}")
        # wait_ddf_d = self.wait_ddf_delayed(ddf=persisted_ddf_d, dask_key_name=f"wait-ddf-{self.fg_index}")
        return [ddf_d, indexed_ddf_d, save_ddf_d, persisted_ddf_d]

    def set_filter_group(self, fg_index: str):
        self.fg_index = fg_index

    @staticmethod
    @delayed
    def hostname_ids_delayed(ddf: DataFrame):
        proc_id_list = ddf.index.unique()
        print("proc_id_list", proc_id_list)
        mask = (2 ** 15 - 1) << 48
        # print("{0:b}".format(mask))
        hostname_id_set = set()
        for proc in proc_id_list:
            hostname_id_set.add(proc & mask)
        hostname_id_list = list(hostname_id_set)
        hostname_id_list.sort()
        print("hostname_id_list", hostname_id_list)
        return hostname_id_list

    @staticmethod
    @delayed
    def metrics_filenames_delayed(ddf: DataFrame, filenames: list, fg_index='file_id'):
        print("///metrics_file_id_delayed///")
        print("filenames", filenames)
        depth = math.ceil(math.sqrt(len(filenames)))
        print("depth", depth)
        iterations = list(range(0, depth + 1))
        print("iterations", iterations)
        iterations.reverse()
        all_tasks = [0] * (depth + 1)
        for i in iterations:
            tasks = []
            if i == depth:
                for index, start in enumerate(filenames):
                    if index < len(filenames) - 1:
                        stop = filenames[index + 1] - 1
                    else:
                        stop = filenames[-1]
                    target_ddf_d = Analysis.target_ddf_delayed(ddf=ddf, start=start, stop=stop,
                                                               dask_key_name=f"target-ddf-{fg_index}-{start}-{stop}")
                    filter_d = filter_delayed(ddf=target_ddf_d, fg_index=fg_index, start=start, stop=stop,
                                              dask_key_name=f"filter-{fg_index}-{start}-{stop}")
                    tasks.append(filter_d)
            else:
                Analysis.__arrange_merge_tasks(fg_index, all_tasks, tasks, i)
            all_tasks[i] = tasks
        metric_tasks = Analysis.__arrange_high_level_tasks(fg_index, all_tasks)
        # Analysis.__arrange_root_tasks(fg_index, all_tasks)
        # print('all_tasks len', len(all_tasks))
        # print('all_tasks', all_tasks)
        metrics = dask.compute(*metric_tasks)
        # print(type(metrics))
        # print(metrics)
        # print(list(metrics))
        # print(metrics[0])
        # print(type(metrics[0]))
        return list(metrics)

    @staticmethod
    @delayed
    def metrics_proc_id_delayed(ddf: DataFrame, processes: list, fg_index='proc_id'):
        print("///compute_metrics_proc_id///")
        # print("processes", processes)
        depth = math.ceil(math.sqrt(len(processes)))
        print("depth", depth)
        iterations = list(range(0, depth + 1))
        print("iterations", iterations)
        iterations.reverse()
        all_tasks = [0] * (depth + 1)
        for i in iterations:
            tasks = []
            if i == depth:
                for index, start in enumerate(processes):
                    if index < len(processes) - 1:
                        stop = processes[index + 1] - 1
                    else:
                        stop = processes[-1]
                    target_ddf_d = Analysis.target_ddf_delayed(ddf=ddf, start=start, stop=stop,
                                                               dask_key_name=f"target-ddf-{fg_index}-{start}-{stop}")
                    filter_d = filter_delayed(ddf=target_ddf_d, fg_index=fg_index, start=start, stop=stop,
                                              dask_key_name=f"filter-{fg_index}-{start}-{stop}")
                    tasks.append(filter_d)
            else:
                Analysis.__arrange_merge_tasks(fg_index, all_tasks, tasks, i)
            all_tasks[i] = tasks
        metric_tasks = Analysis.__arrange_high_level_tasks(fg_index, all_tasks)
        # Analysis.__arrange_root_tasks(fg_index, all_tasks)
        # print('all_tasks len', len(all_tasks))
        # print('all_tasks', all_tasks)
        metrics = dask.compute(*metric_tasks)
        # print(type(metrics))
        # print(metrics)
        # print(list(metrics))
        # print(metrics[0])
        # print(type(metrics[0]))
        return list(metrics)

    @staticmethod
    @delayed
    def persist_ddf_delayed(ddf: DataFrame):
        ddf = ddf.persist()
        wait(ddf)
        return ddf

    @staticmethod
    @delayed
    def placeholder_delayed(x):
        return x

    @staticmethod
    @delayed
    def read_parquet_delayed(log_dir: str, index: list = None):
        if index:
            print("Index specified", index)
            return dd.read_parquet(f"{log_dir}/*.parquet", calculate_divisions=True, index=index)
        print("Index not specified")
        return dd.read_parquet(f"{log_dir}/*.parquet", index=False)

    # @staticmethod
    # @delayed
    # def repartition_ddf_delayed(ddf: DataFrame):
    #     return ddf.repartition(par)

    @staticmethod
    @delayed
    def save_ddf_delayed(ddf: DataFrame, log_dir: str, fg_index: str):
        ddf.to_parquet(f"{log_dir}/{PARTITION_DIR}/{fg_index}")

    @staticmethod
    @delayed
    def save_filenames_delayed(filenames: list, log_dir: str):
        filenames = list(filenames)
        filenames.sort()
        with open(f"{log_dir}/filenames.json", "w") as file:
            json.dump(filenames, file, cls=NpEncoder)

    @staticmethod
    @delayed
    def set_ddf_index_delayed(ddf: DataFrame, fg_index: str):
        return ddf.set_index([fg_index])

    @staticmethod
    @delayed
    def target_ddf_delayed(ddf: DataFrame, start: int, stop: int):
        if hasattr(ddf, 'compute'):
            if stop == -1:
                return ddf.loc[start:].reset_index().compute()
            return ddf.loc[start:stop].reset_index().compute()
        if stop == -1:
            return ddf.loc[start:].reset_index()
        return ddf.loc[start:stop].reset_index()

    @staticmethod
    @delayed
    def unique_filenames_delayed(ddf: DataFrame, log_dir: str):
        if os.path.exists(f"{log_dir}/filenames.json"):
            with open(f"{log_dir}/filenames.json", "r") as file:
                unique_filenames = json.load(file)
        else:
            unique_filenames = ddf.index.unique().compute()
        print("unique_filenames", unique_filenames)
        return unique_filenames

    @staticmethod
    @delayed
    def unique_processes_delayed(ddf: DataFrame):
        unique_processes_ddf = ddf.groupby(ddf.index)['hostname', 'rank', 'thread_id'].min().compute()
        unique_processes = list(unique_processes_ddf.index.unique())
        print("unique_processes", unique_processes)
        return unique_processes

    @staticmethod
    @delayed
    def wait_ddf_delayed(ddf: DataFrame):
        wait(ddf)

    @staticmethod
    def __arrange_high_level_tasks(fg_index: str, all_tasks: list):
        Analysis.__arrange_root_tasks(fg_index, all_tasks)
        # for t, next_tasks in enumerate(all_tasks[0]):
        #     all_tasks[0][t] = summary(next_tasks, dask_key_name=f"summary_{fg_index}_0_{t}")
        high_level_tasks = []
        all_tasks.reverse()
        for i, tasks in enumerate(all_tasks):
            high_level_tasks.append(
                Analysis.placeholder_delayed(tasks, dask_key_name=f"metrics-{fg_index}-{len(all_tasks) - i - 1}"))
        return high_level_tasks

    @staticmethod
    def __arrange_merge_tasks(fg_index: str, all_tasks: list, tasks: list, i: int):
        next_tasks = len(all_tasks[i + 1])
        if next_tasks % 2 == 1:
            next_tasks = next_tasks - 1
        for t in range(0, next_tasks, 2):
            merge_d = merge_delayed(all_tasks[i + 1][t], all_tasks[i + 1][t + 1],
                                    dask_key_name=f"merge-{fg_index}-{i}-{t}-{t + 1}")
            tasks.append(merge_d)
        next_tasks = len(all_tasks[i + 1])
        if next_tasks % 2 == 1:
            tasks.append(all_tasks[i + 1][next_tasks - 1])
        for t, next_tasks in enumerate(all_tasks[i + 1]):
            all_tasks[i + 1][t] = flatten_delayed(next_tasks, dask_key_name=f"flatten-{fg_index}-{i + 1}-{t}")

    @staticmethod
    def __arrange_root_tasks(fg_index: str, all_tasks: list):
        for t, next_tasks in enumerate(all_tasks[0]):
            all_tasks[0][t] = flatten_delayed(next_tasks, dask_key_name=f"flatten-{fg_index}-0-{t}")

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
