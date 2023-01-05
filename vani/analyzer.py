import json
from dask.distributed import as_completed
from time import perf_counter
from vani.common.filter_groups import *
from vani.common.interfaces import *
from vani.core.analysis import Analysis
from vani.core.dask_mgmt import DEFAULT_N_WORKERS_PER_NODE, DaskManager
from vani.utils.file_utils import ensure_dir
from vani.utils.json_encoders import NpEncoder
from vani.utils.logger import create_logger, format_log
from vani.utils.visualization import save_graph


class Analyzer(object):

    # noinspection PyTypeChecker
    def __init__(self, debug=False, cluster_settings: Dict[str, Any] = None, working_dir=".digio"):
        # Create logger
        ensure_dir(working_dir)
        self.logger = create_logger(__name__, f"{working_dir}/analyzer.log")
        self.logger.info(format_log("main", "Initializing analyzer"))
        # Keep values
        self.cluster_settings = cluster_settings
        self.debug = debug
        self.fg_indices = ['tmid', 'file_id', 'proc_id']  # 'tmid', 'proc_id', 'file_id']
        self.n_workers_per_node = cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        self.working_dir = working_dir
        # Boot Dask clusters & clients
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
            low_level_char_d = analysis.compute_low_level_char(ddf=logs_d[-1], leveled_metrics=metrics_d[-1])
            delayed_tasks.extend(low_level_char_d)

            # Keep delayed tasks
            fg_delayed_tasks[fg_index] = delayed_tasks
            # break

        for fg_index in self.fg_indices:
            save_graph(dask.delayed(fg_delayed_tasks[fg_index])(dask_key_name=f"save-graph-{fg_index}"),
                       filename=f"{self.working_dir}/{analysis.id}/task-graph-{fg_index}")

        fg_futures = {}
        for fg_index in self.fg_indices:
            client = self.dask_mgr.get_client(cluster_key=fg_index)
            fg_futures[fg_index] = client.compute(fg_delayed_tasks[fg_index], sync=False)

        ensure_dir(f"{self.working_dir}/{analysis.id}/")

        for fg_index in self.fg_indices:
            start_time = perf_counter()
            for future in as_completed(fg_futures[fg_index]):
                end_time = perf_counter() - start_time
                print(f"{future.key} {future.status} {end_time / 60}")
                key_name = f"{future.key[0]}-{future.key[1]}" if isinstance(future.key, tuple) else future.key
                if key_name.startswith("metrics-") or key_name.startswith("llc-all"):
                    json_result = future.result()
                    with open(f"{self.working_dir}/{analysis.id}/{key_name}.json", "w+") as file:
                        json.dump(json_result, file, cls=NpEncoder, sort_keys=True)

        return

    def shutdown(self):
        self.dask_mgr.shutdown()
