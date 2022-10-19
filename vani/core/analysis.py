import asyncio
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
from vani.core.metrics import filter, merge, summary
from vani.core.scores import min_max, score
from vani.utils.file_utils import dir_hash, ensure_dir
from vani.utils.logger import ElapsedTimeLogger, format_log

CACHE_DIR = "cached"
METRIC_DIR = "metric"
PARTITION_DIR = "partitioned"


def compute_min_max(log_dir: str, fg_index: str, depth: int):
    with open(f"{log_dir}/global.json") as file:
        global_metrics = json.load(file)
        min_val, max_val = global_metrics[fg_index][0], global_metrics[fg_index][1]
        next_tasks = 2 ** depth
        interval = math.ceil((max_val - min_val) * 1.0 / next_tasks)
        time_range = range(min_val, max_val, interval)
        return time_range, interval


@delayed
def persist_ddf(ddf: DataFrame):
    return ddf.persist()


@delayed
def placeholder(x):
    return x


@delayed
def read_parquet_to_ddf(log_dir: str):
    return dd.read_parquet(f"{log_dir}/*.parquet", index=False)


@delayed
def set_ddf_index(ddf: DataFrame, fg_index: str):
    return ddf.set_index([fg_index])


@delayed
def target_ddf(ddf: DataFrame, start: int, stop: int):
    return ddf.loc[start:stop].reset_index().compute()


@delayed
def wait_ddf(ddf: DataFrame):
    wait(ddf)
    return ddf


class Analysis(object):

    def __init__(self, log_dir: str, working_dir: str, n_workers_per_node: int, logger: Logger,
                 debug=False) -> None:
        self.id = math.floor(time.time())
        self.debug = debug
        self.depth = 10
        self.log_dir = log_dir
        self.logger = logger
        self.n_workers_per_node = n_workers_per_node
        self.working_dir = working_dir

    def compute_metrics(self, ddf: DataFrame, fg_index: str, fg_range: range, interval: int):
        iterations = list(range(0, self.depth + 1))
        iterations.reverse()
        all_tasks = [0] * (self.depth + 1)
        for i in iterations:
            tasks = []
            if i == self.depth:
                for start in fg_range:
                    stop = start + interval - 1
                    target_ddf_delayed = target_ddf(ddf=ddf, start=start, stop=stop,
                                                    dask_key_name=f"target_ddf_{fg_index}_{start}_{stop}")
                    filter_delayed = filter(ddf=target_ddf_delayed, fg_index=fg_index, start=start, stop=stop,
                                            dask_key_name=f"filter_{fg_index}_{start}_{stop}")
                    tasks.append(filter_delayed)
            else:
                next_tasks = len(all_tasks[i + 1])
                if next_tasks % 2 == 1:
                    next_tasks = next_tasks - 1
                for t in range(0, next_tasks, 2):
                    merge_delayed = merge(all_tasks[i + 1][t], all_tasks[i + 1][t + 1],
                                          dask_key_name=f"merge_{fg_index}_{i}_{t}_{t + 1}")
                    tasks.append(merge_delayed)
                next_tasks = len(all_tasks[i + 1])
                if next_tasks % 2 == 1:
                    tasks.append(all_tasks[i + 1][next_tasks - 1])
                for t, next_tasks in enumerate(all_tasks[i + 1]):
                    all_tasks[i + 1][t] = summary(next_tasks, dask_key_name=f"summary_{fg_index}_{i + 1}_{t}")
            all_tasks[i] = tasks
        # noinspection PyTypeChecker
        for t, next_tasks in enumerate(all_tasks[0]):
            all_tasks[0][t] = summary(next_tasks, dask_key_name=f"summary_{fg_index}_0_{t}")
        high_level_tasks = []
        all_tasks.reverse()
        for i, tasks in enumerate(all_tasks):
            high_level_tasks.append(placeholder(tasks, dask_key_name=f"metrics_{fg_index}_{len(all_tasks) - i - 1}"))
        return high_level_tasks

    def compute_observations(self):
        pass

    def compute_scores(self, metrics, fg_index: str):
        metrics.reverse()
        root_metric = metrics[0][0]
        min_max_delayed = min_max(metric=root_metric, dask_key_name=f"min_max_{fg_index}")
        iterations = list(range(0, self.depth + 1))
        iterations.reverse()
        all_tasks = [0] * (self.depth + 1)
        for i in iterations:
            tasks = []
            for t in range(2 ** i):
                tasks.append(
                    score(metric=metrics[i][t], min_max_values=min_max_delayed,
                          dask_key_name=f"score_{fg_index}_{i}_{t}"))
            all_tasks[i] = tasks
        high_level_tasks = []
        all_tasks.reverse()
        for i, tasks in enumerate(all_tasks):
            high_level_tasks.append(placeholder(tasks, dask_key_name=f"scores_{fg_index}_{len(all_tasks) - i - 1}"))
        return high_level_tasks

    def read_index_logs(self, fg_index: str):
        ddf_delayed = read_parquet_to_ddf(log_dir=self.log_dir, dask_key_name=f"read_parquet_{fg_index}")
        indexed_ddf_delayed = set_ddf_index(ddf=ddf_delayed, fg_index=fg_index, dask_key_name=f"set_index_{fg_index}")
        persisted_ddf_delayed = persist_ddf(ddf=indexed_ddf_delayed, dask_key_name=f"persist_ddf_{fg_index}")
        wait_ddf_delayed = wait_ddf(ddf=persisted_ddf_delayed, dask_key_name=f"wait_ddf_{fg_index}")
        return [ddf_delayed, indexed_ddf_delayed, persisted_ddf_delayed, wait_ddf_delayed]

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
