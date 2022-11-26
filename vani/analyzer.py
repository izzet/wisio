from itertools import chain

import dask
import dask.bag as db
import dask.dataframe as dd
import glob
import json
import numpy as np
import os
from anytree import PostOrderIter
from dask.distributed import Client, as_completed, worker_client
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


@dask.delayed
def low_level_characteristics_tmid(target_ddf: DataFrame, item):
    if target_ddf.empty:
        return item

    def f(x):

        d = {}
        # FIXME change rank vs proc based on app or workflow
        d['procs'] = x['rank'].unique()
        d['files'] = x['filename'].unique()
        d['funcs'] = x['func_id'].unique()
        # TODO

        # High-level filter without uniques to understand focus areas
        # Low-level filter to explore details
        # apply to aggregate (10x faster)

        return pd.Series(d, index=['procs', 'files', 'funcs'])

    aggregated_values = target_ddf.groupby('io_cat').apply(f)
    # aggregated_values = ddf.groupby(['io_cat', 'proc_id']).apply(f)

    del target_ddf

    index_values = aggregated_values.index.unique()

    if 1 in index_values:
        item['read']['files'] = aggregated_values.loc[1]['files']
        item['read']['funcs'] = aggregated_values.loc[1]['funcs']
        item['read']['procs'] = aggregated_values.loc[1]['procs']
    else:
        item['read']['files'] = []
        item['read']['funcs'] = []
        item['read']['procs'] = []
    if 2 in index_values:
        item['write']['files'] = aggregated_values.loc[2]['files']
        item['write']['funcs'] = aggregated_values.loc[2]['funcs']
        item['write']['procs'] = aggregated_values.loc[2]['procs']
    else:
        item['write']['files'] = []
        item['write']['funcs'] = []
        item['write']['procs'] = []
    if 3 in index_values:
        item['metadata']['files'] = aggregated_values.loc[3]['files']
        item['metadata']['funcs'] = aggregated_values.loc[3]['funcs']
        item['metadata']['procs'] = aggregated_values.loc[3]['procs']
    else:
        item['metadata']['files'] = []
        item['metadata']['funcs'] = []
        item['metadata']['procs'] = []

    all_files = np.union1d(np.union1d(item['read']['files'], item['write']['files']), item['metadata']['files'])
    all_funcs = np.union1d(np.union1d(item['read']['funcs'], item['write']['funcs']), item['metadata']['funcs'])
    all_procs = np.union1d(np.union1d(item['read']['procs'], item['write']['procs']), item['metadata']['procs'])

    item['all']['files'] = all_files
    item['all']['funcs'] = all_funcs
    item['all']['procs'] = all_procs.astype(int)

    item['all']['num_files'] = len(all_files)
    item['all']['num_funcs'] = len(all_funcs)
    item['all']['num_procs'] = len(all_procs)

    item['file_per_process'] = len(all_files) / len(all_procs)
    item['md_io_ratio'] = 0 if item['all']['agg_dur'] == 0 else item['metadata']['agg_dur'] / item['all']['agg_dur']

    return item


@dask.delayed
def low_level_characteristics(ddf: DataFrame, ranges: list, fg_index):
    delayed_tasks = []
    for r in ranges:
        start, stop = r['start'], r['stop']
        args = [ddf, start, stop]
        target_ddf_d = dask.delayed(lambda ddf, start, stop: ddf.loc[start:stop].reset_index())(*args,
                                                                                                dask_key_name=f"ll-target-ddf-{fg_index}-{start}-{stop}")
        low_level_characteristics_d = low_level_characteristics_tmid(target_ddf=target_ddf_d, item=r,
                                                                     dask_key_name=f"ll-characteristics-{fg_index}-{start}-{stop}")
        delayed_tasks.append(low_level_characteristics_d)
    # client = dask.distributed.get_client()
    # characteristics_future = client.compute(delayed_tasks)
    # characteristics = client.gather(characteristics_future)
    with worker_client() as client:
        characteristics_future = client.compute(delayed_tasks)
        characteristics = client.gather(characteristics_future)
    # characteristics = dask.compute(delayed_tasks)
    # print(characteristics)
    return characteristics


def sort_metrics(metrics: list):
    # TODO make it parallel
    sorted_metrics = sorted(metrics, key=lambda x: x['all']['agg_dur'], reverse=True)
    return sorted_metrics


def filter(sorted_metrics: list, threshold=90):
    total_value = 0
    for m in sorted_metrics:
        total_value = total_value + m['all']['agg_dur']

    selected_metrics = []
    cur_per_sum = 0
    for m in sorted_metrics:
        per = m['all']['agg_dur'] * 100 / total_value
        m['all']['per'] = per
        cur_per_sum = cur_per_sum + per
        print('cur_per_sum', cur_per_sum)
        selected_metrics.append(m)
        if cur_per_sum >= threshold:
            break
    return selected_metrics


class Analyzer(object):

    # noinspection PyTypeChecker
    def __init__(self, debug=False, cluster_settings: Dict[str, Any] = None, working_dir=".digio"):
        # Create logger
        self.logger = create_logger(__name__, f"{working_dir}/analyzer.log")
        self.logger.info(format_log("main", "Initializing analyzer"))
        # Keep values
        self.cluster_settings = cluster_settings
        self.debug = debug
        self.fg_indices = ['tmid', 'file_id', 'proc_id']  # 'tmid', 'proc_id', 'file_id']
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
            # Compute metrics
            metrics_d = analysis.compute_metrics(ddf=logs_d[-1])
            delayed_tasks.extend(metrics_d)
            # Compute low level characteristics
            low_level_char_d = analysis.compute_low_level_char(ddf=logs_d[-1], leveled_metrics=metrics_d)
            delayed_tasks.append(low_level_char_d)

            # Loop through metrics
            all_filtered_metrics_d = []
            # for metrics_depth in metrics_d:
            #     sorted_metrics_d = dask.delayed(sort_metrics)(metrics_depth)
                # delayed_tasks.append(sorted_metrics_d)

                # filtered_metrics_d = dask.delayed(filter)(sorted_metrics_d)
                #
                # ll_characteristics = low_level_characteristics(ddf=logs_d[-1], ranges=filter_metrics_d,
                #                                                fg_index=fg_index)
                # delayed_tasks.append(filter_metrics_d)
                # filter_metrics_list_d.append(ll_characteristics)

            # delayed_tasks.append(
            #     dask.delayed(lambda x: x)(filter_metrics_list_d, dask_key_name=f"filtered-metrics-{fg_index}"))

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
                if key_name.startswith("metrics-") or key_name.startswith("filtered-") or key_name.startswith("ll-all"):
                    # key_name.startswith("scores-") or
                    # key_name.startswith("sort-") or
                    # key_name.startswith("list-") or \
                    # key_name.startswith("filtered-"):
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
