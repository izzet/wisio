import asyncio
import dask
import dask.bag as db
import dask.dataframe as dd
import glob
import json
import math
import numpy as np
import os
import time
from dask import delayed
from dask.dataframe import DataFrame
from dask.distributed import wait, worker_client
from hashlib import md5
from logging import Logger
from vani.core.characteristics import low_level_char_delayed
from vani.core.metrics import filter_asymptote_delayed, filter_delayed, flatten_delayed, merge_delayed, sort_delayed
from vani.core.scores import min_max_delayed, score
from vani.utils.file_utils import dir_hash, ensure_dir
from vani.utils.json_encoders import NpEncoder
from vani.utils.logger import ElapsedTimeLogger, format_log

CACHE_DIR = "cached"
METRIC_DIR = "metric"
INDEX_DIR = "indexed"


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

    def compute_low_level_char(self, ddf: DataFrame, leveled_metrics: list):
        all_tasks_d = []
        for by_metric in ['agg_dur', 'agg_bw']:
            tasks_d = []
            for index, metrics in enumerate(leveled_metrics):
                dask_key_suffix = f"{self.fg_index}-{by_metric}-{index}"
                sorted_metrics_d = sort_delayed(metrics=metrics, by_metric=by_metric, reverse=by_metric is 'agg_dur',
                                                dask_key_name=f"llc-sort-{dask_key_suffix}")
                filtered_metrics_d = filter_asymptote_delayed(sorted_metrics=sorted_metrics_d, by_metric=by_metric,
                                                              dask_key_name=f"llc-filter-{dask_key_suffix}")
                low_level_char_d = self.compute_low_level_char_delayed(ddf=ddf, fg_index=self.fg_index,
                                                                       filtered_metrics=filtered_metrics_d,
                                                                       dask_key_name=f"llc-char-{dask_key_suffix}")
                tasks_d.append(low_level_char_d)
            all_tasks_d.append(delayed(lambda x: x)(tasks_d, dask_key_name=f"llc-all-{self.fg_index}-{by_metric}"))
        return all_tasks_d

    @staticmethod
    @delayed
    def compute_low_level_char_delayed(ddf: DataFrame, fg_index: str, filtered_metrics: list):
        tasks_d = []
        for metric in filtered_metrics:
            start, stop = metric['start'], metric['stop']
            dask_key_suffix = f"{fg_index}-{start}-{stop}"
            target_ddf_d = Analysis.target_ddf_delayed(ddf=ddf, start=start, stop=stop,
                                                       dask_key_name=f"llc-target-ddf-{dask_key_suffix}")
            agg = {
                'filename': 'unique',
                'func_id': 'unique',
                'index': 'count',
                'rank': 'unique',
            }
            low_level_char_d = low_level_char_delayed(ddf=target_ddf_d, metric=metric, agg=agg,
                                                      dask_key_name=f"llc-char-{dask_key_suffix}")
            tasks_d.append(low_level_char_d)
        # client = dask.distributed.get_client()
        # characteristics_future = client.compute(delayed_tasks)
        # characteristics = client.gather(characteristics_future)
        with worker_client() as client:
            char_f = client.compute(tasks_d)
            char = client.gather(char_f)
        # characteristics = dask.compute(delayed_tasks)
        print(char)
        return char

    def compute_metrics(self, ddf: DataFrame):
        if self.fg_index == 'tmid':
            return self.compute_metrics_tmid(ddf=ddf)
        elif self.fg_index == 'proc_id':
            return self.compute_metrics_proc_id(ddf=ddf)
        elif self.fg_index == 'file_id':
            return self.compute_metrics_file_id(ddf=ddf)
        else:
            raise

    def compute_metrics_file_id(self, ddf: DataFrame):
        unique_filenames_d = self.unique_filenames_delayed(ddf=ddf, log_dir=self.log_dir,
                                                           dask_key_name=f"unique-filenames-{self.fg_index}")  # .compute()
        save_filenames_d = self.save_filenames_delayed(filenames=unique_filenames_d, log_dir=self.log_dir,
                                                       dask_key_name=f"save-filenames-{self.fg_index}")
        metrics_d = self.metrics_filenames_delayed(ddf=ddf, filenames=unique_filenames_d,
                                                   dask_key_name=f"metrics-{self.fg_index}")
        return [unique_filenames_d, save_filenames_d, [metrics_d]]

    def compute_metrics_proc_id(self, ddf: DataFrame):
        unique_processes_d = self.unique_processes_delayed(ddf=ddf, log_dir=self.log_dir,
                                                           dask_key_name=f"unique-processes-{self.fg_index}")  # .compute()
        save_processes_d = self.save_processes_delayed(processes=unique_processes_d, log_dir=self.log_dir,
                                                       dask_key_name=f"save-processes-{self.fg_index}")
        # hostname_ids_d = self.hostname_ids_delayed(ddf=unique_processes_d,
        #                                            dask_key_name=f"hostname-ids-{self.fg_index}")
        metrics_d = self.metrics_proc_id_delayed(ddf=ddf, processes=unique_processes_d,
                                                 dask_key_name=f"metrics-{self.fg_index}")
        return [unique_processes_d, save_processes_d, [metrics_d]]

    def compute_metrics_tmid(self, ddf: DataFrame):
        min_val, max_val = tuple(self.global_min_max[self.fg_index])
        next_tasks = 2 ** self.depth
        interval = math.ceil((max_val - min_val) * 1.0 / next_tasks)
        time_range = range(min_val, max_val, interval)
        iterations = list(range(0, self.depth + 1))
        iterations.reverse()
        all_tasks_d = [0] * (self.depth + 1)
        for i in iterations:
            tasks_d = []
            if i == self.depth:
                for start in time_range:
                    stop = start + interval - 1
                    target_ddf_d = self.target_ddf_delayed(ddf=ddf, start=start, stop=stop,
                                                           dask_key_name=f"target-ddf-{self.fg_index}-{start}-{stop}")
                    filter_d = filter_delayed(ddf=target_ddf_d, fg_index=self.fg_index, start=start, stop=stop,
                                              dask_key_name=f"filter-{self.fg_index}-{start}-{stop}")
                    tasks_d.append(filter_d)
            else:
                Analysis.__arrange_merge_tasks(self.fg_index, all_tasks_d, tasks_d, i)
            all_tasks_d[i] = tasks_d
        # return Analysis.__arrange_high_level_tasks(self.fg_index, all_tasks)
        return [all_tasks_d]

    def compute_scores(self, metrics: list):
        # Get root metric
        root_metric = metrics[-1][0]
        # Calculate min max values
        min_max_d = min_max_delayed(metric=root_metric, global_min_max=self.global_min_max,
                                    dask_key_name=f"min-max-{self.fg_index}")
        # Create metrics bag
        if self.fg_index == 'tmid':
            metrics.reverse()
        metrics_b = db.from_delayed(metrics)
        # Calculate scores
        if self.fg_index == 'tmid':
            score.__name__ = f"scores-{self.fg_index}"
            scores = metrics_b.map(score, min_max_d)
        else:
            score.__name__ = f"scores-{self.fg_index}"
            scores = metrics_b.flatten().map(score, min_max_d)
        # Return delayed scores
        return scores.to_delayed()

    def load_global_min_max(self):
        with open(f"{self.log_dir}/global.json") as file:
            global_min_max = json.load(file)
        print("global_min_max", global_min_max)
        self.global_min_max = global_min_max
        return global_min_max

    def read_and_index_logs(self, use_cache=True):
        fg_index_dir = f"{self.log_dir}/{INDEX_DIR}/{self.fg_index}"
        if use_cache and os.path.exists(f"{fg_index_dir}/_metadata"):
            ddf_d = self.read_parquet_delayed(log_dir=fg_index_dir, index=[self.fg_index],
                                              dask_key_name=f"read-parquet-{self.fg_index}")
            persisted_ddf_d = self.persist_ddf_delayed(ddf=ddf_d, dask_key_name=f"persist-ddf-{self.fg_index}")
            return [ddf_d, persisted_ddf_d]
        ddf_d = self.read_parquet_delayed(log_dir=self.log_dir, dask_key_name=f"read-parquet-{self.fg_index}")
        indexed_ddf_d = self.set_ddf_index_delayed(ddf=ddf_d, fg_index=self.fg_index,
                                                   dask_key_name=f"set-index-{self.fg_index}")
        persisted_ddf_d = self.persist_ddf_delayed(ddf=indexed_ddf_d, dask_key_name=f"persist-ddf-{self.fg_index}")
        partitioned_ddf_d = self.repartition_delayed(ddf=persisted_ddf_d,
                                                     dask_key_name=f"repartition-ddf-{self.fg_index}")
        save_ddf_d = self.save_ddf_delayed(ddf=partitioned_ddf_d, log_dir=self.log_dir, fg_index=self.fg_index,
                                           dask_key_name=f"save-ddf-{self.fg_index}")
        return [ddf_d, indexed_ddf_d, persisted_ddf_d, save_ddf_d, partitioned_ddf_d]

    @staticmethod
    @delayed
    def repartition_delayed(ddf: DataFrame, partition_size='128MB'):
        return ddf.repartition(partition_size=partition_size)

    def set_filter_group(self, fg_index: str):
        self.fg_index = fg_index

    @staticmethod
    @delayed
    def hostname_ids_delayed(ddf: DataFrame):
        proc_id_list = ddf.index.unique()
        # print("proc_id_list", proc_id_list)
        mask = (2 ** 15 - 1) << 48
        # print("{0:b}".format(mask))
        hostname_id_set = set()
        for proc in proc_id_list:
            hostname_id_set.add(proc & mask)
        hostname_id_list = list(hostname_id_set)
        hostname_id_list.sort()
        # print("hostname_id_list", hostname_id_list)
        return hostname_id_list

    @staticmethod
    @delayed
    def metrics_filenames_delayed(ddf: DataFrame, filenames: list, fg_index='file_id'):
        print('calculating metrics for filenames', len(filenames))
        tasks_d = []
        for filename in filenames:
            target_ddf_d = Analysis.target_ddf_delayed(ddf=ddf, start=filename, stop=filename,
                                                       dask_key_name=f"target-ddf-{fg_index}-{filename}")
            filter_d = filter_delayed(ddf=target_ddf_d, fg_index=fg_index, start=filename, stop=filename,
                                      dask_key_name=f"filter-{fg_index}-{filename}")
            flatten_d = flatten_delayed(filter_d, dask_key_name=f"flatten-{fg_index}-{filename}")
            tasks_d.append(flatten_d)
        print('num of tasks created', len(tasks_d))
        with worker_client() as client:
            print('submitting on', client)
            metrics_f = client.compute(tasks_d)
            print('gathering tasks', len(metrics_f))
            metrics = client.gather(metrics_f)
        # metrics = dask.compute(tasks_d)
        print('computed metrics', type(metrics))
        print('computed metrics', len(metrics))
        # print('computed metrics', metrics)
        return metrics

    @staticmethod
    @delayed
    def metrics_proc_id_delayed(ddf: DataFrame, processes: list, fg_index='proc_id'):
        print('calculating metrics for processes', len(processes))
        tasks_d = []
        for process in processes:
            target_ddf_d = Analysis.target_ddf_delayed(ddf=ddf, start=process, stop=process,
                                                       dask_key_name=f"target-ddf-{fg_index}-{process}")
            filter_d = filter_delayed(ddf=target_ddf_d, fg_index=fg_index, start=process, stop=process,
                                      dask_key_name=f"filter-{fg_index}-{process}")
            flatten_d = flatten_delayed(filter_d, dask_key_name=f"flatten-{fg_index}-{process}")
            tasks_d.append(flatten_d)
        print('num of tasks created', len(tasks_d))
        metrics = []
        with worker_client() as client:
            print('submitting on', client)
            for tasks in np.array_split(tasks_d, np.sqrt(len(tasks_d))):
                print('tasks', tasks)
                # metrics_f = client.compute(*tasks, sync=False)
                sub_metrics = dask.compute(*tasks)
                print('sub_metrics', sub_metrics)
                # print('gathering tasks', len(metrics_f))
                # metrics_0 = client.gather(metrics_f)
                # print('metrics_0', metrics_0)
                # metrics.extend(metrics_0)
                # all_metrics_f.extend(metrics_f)
                metrics.extend(sub_metrics)
        # metrics = dask.compute(tasks_d)

        print('computed metrics', type(metrics))
        print('computed metrics', len(metrics))
        # print('computed metrics', metrics)
        return metrics

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

    @staticmethod
    @delayed
    def save_ddf_delayed(ddf: DataFrame, log_dir: str, fg_index: str):
        client = dask.distributed.get_client()
        print('save_ddf_delayed client', client)
        with worker_client() as client2:
            print('save_ddf_delayed client2', client2)
        ddf.to_parquet(f"{log_dir}/{INDEX_DIR}/{fg_index}")

    @staticmethod
    @delayed
    def save_filenames_delayed(filenames: list, log_dir: str):
        print('saving', filenames)
        filenames = list(filenames)
        filenames.sort()
        with open(f"{log_dir}/filenames.json", "w") as file:
            json.dump(filenames, file, cls=NpEncoder)

    @staticmethod
    @delayed
    def save_processes_delayed(processes: list, log_dir: str):
        print('saving', processes)
        processes = list(processes)
        processes.sort()
        with open(f"{log_dir}/processes.json", "w") as file:
            json.dump(processes, file, cls=NpEncoder)

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
        # print("unique_filenames", unique_filenames)
        return unique_filenames

    @staticmethod
    @delayed
    def unique_processes_delayed(ddf: DataFrame, log_dir: str):
        if os.path.exists(f"{log_dir}/processes.json"):
            with open(f"{log_dir}/processes.json", "r") as file:
                unique_processes = json.load(file)
        else:
            unique_processes_ddf = ddf.groupby(ddf.index)['hostname', 'rank', 'thread_id'].min().compute()
            unique_processes = list(unique_processes_ddf.index.unique())
        # print("unique_processes", unique_processes)
        return unique_processes

    @staticmethod
    def __arrange_high_level_tasks(fg_index: str, all_tasks: list):
        Analysis.__arrange_root_tasks(fg_index, all_tasks)
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
        is_partitioned = os.path.exists(f"{log_dir}/{INDEX_DIR}/_common_metadata")
        self.logger.debug(format_log("main", f"Logs partitioned before: {is_partitioned}"))
        files = glob.glob(f"{log_dir}/{INDEX_DIR}/*.parquet", recursive=True)
        return files if is_partitioned else self.__partition_logs(log_dir=log_dir)

    def __indexed_ddf(self, fg_index: str, log_dir: str):
        cache_dir = f"{log_dir}/{CACHE_DIR}/{fg_index}"
        partition_dir = f"{log_dir}/{INDEX_DIR}"
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
                ddf = dd.read_parquet(f"{log_dir}/{INDEX_DIR}/*.parquet", index=False)
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
        existing_files = glob.glob(f"{log_dir}/{INDEX_DIR}/*.parquet", recursive=True)
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
        partition_folder = f"{log_dir}/{INDEX_DIR}/"
        with ElapsedTimeLogger(logger=self.logger, message=f"Partitions written into: {partition_folder}"):
            dd.to_parquet(ddf, partition_folder)
        # Return partitioned files
        partitioned_files = glob.glob(f"{log_dir}/{INDEX_DIR}/*.parquet", recursive=True)
        return partitioned_files

    def __path_with_id(self, path: str):
        ensure_dir(f"{self.working_dir}/{self.id}")
        return f"{self.working_dir}/{self.id}/{path}"
