import asyncio
import logging
import socket
from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
from logging import Logger
from time import sleep
from typing import Dict, List, Union
from vani.common.filter_groups import *
from vani.common.interfaces import *
from vani.utils.logger import format_log

DEFAULT_NODE_MEMORY = 1600
DEFAULT_N_THREADS_PER_WORKER = 1
DEFAULT_N_WORKERS_PER_NODE = 16
DEFAULT_WORKER_QUEUE = 'pdebug'
DEFAULT_WORKER_TIME = 120
WORKER_CHECK_INTERVAL = 5.0


class DaskManager(object):

    def __init__(self, working_dir: str, fg_indices: List[str], logger: Logger, debug=False) -> None:
        self.debug = debug
        self.fg_indices = fg_indices
        self.logger = logger
        self.working_dir = working_dir

    def boot(self, cluster_settings: Dict[str, Any], n_workers_per_node: int):
        self.clusters = self.initialize_clusters(cluster_settings=cluster_settings)
        self.clients = self.initialize_clients(self.clusters)
        self.scale_clusters(clusters=self.clusters, n_workers=n_workers_per_node)

    def shutdown(self):
        self.logger.debug(format_log('dask_mgr', "Shutting down..."))
        for client in self.clients.values():
            client.close()
        self.logger.debug(format_log('dask_mgr', "Clients closed"))
        for cluster in self.clusters.values():
            cluster.close()
        self.logger.debug(format_log('dask_mgr', "Clusters closed"))

    def initialize_clients(self, clusters: Dict[str, Union[LocalCluster, LSFCluster]]):
        # Initialize clients
        clients = {}
        # Loop through clusters
        for cluster_key in clusters.keys():
            # Get cluster instance
            cluster = clusters[cluster_key]
            # Create a client & set it as default if it is a local cluster
            clients[cluster_key] = Client(cluster, set_as_default=isinstance(cluster, LocalCluster))
            self.logger.debug(format_log(cluster_key, "Client initialized"))
            # Print client information
            if self.debug:
                print(clients[cluster_key])
        # Return clients
        return clients

    def initialize_clusters(self, cluster_settings: Dict[str, Any]) -> Dict[str, Union[LocalCluster, LSFCluster]]:
        # Read required config
        dashboard_port = cluster_settings.get('dashboard_port')
        local_directory = cluster_settings.get('local_directory')
        # Read optional config
        cores = cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        host = cluster_settings.get('host', socket.gethostname())
        log_file = cluster_settings.get('log_file', "%J.log")
        log_file = log_file if log_file.startswith('/') else f"{self.working_dir}/worker_logs/{log_file}"
        memory = cluster_settings.get('memory', DEFAULT_NODE_MEMORY)
        processes = cluster_settings.get('processes', DEFAULT_N_THREADS_PER_WORKER)
        use_stdin = cluster_settings.get('use_stdin', True)
        worker_queue = cluster_settings.get('worker_queue', DEFAULT_WORKER_QUEUE)
        worker_time = cluster_settings.get('worker_time', DEFAULT_WORKER_TIME)
        # Create clusters
        clusters = {}
        # Create a local cluster
        clusters['local'] = LocalCluster(dashboard_address=f"{host}:{dashboard_port}",
                                         host=host,
                                         local_directory=f"{local_directory}/local",
                                         memory_limit=0,
                                         n_workers=cores,
                                         name="cluster-local",
                                         silence_logs=logging.DEBUG if self.debug else logging.CRITICAL)
        self.logger.debug(format_log("local", f"Cluster initialized ({clusters['local'].dashboard_link})"))

        # Create distributed clusters
        for index, fg_index in enumerate(self.fg_indices):
            # Create LSF cluster
            clusters[fg_index] = LSFCluster(cores=cores * processes,
                                            death_timeout=worker_time * 60,
                                            header_skip=['-n', '-R', '-M', '-P', '-W 00:30'],
                                            job_extra=['-nnodes 1',
                                                       '-G asccasc',
                                                       '-q {}'.format(worker_queue),
                                                       '-W {}'.format(worker_time),
                                                       '-o {}'.format(log_file),
                                                       '-e {}'.format(log_file)],
                                            local_directory=f"{local_directory}/{fg_index}",
                                            memory=f"{memory}GB",
                                            name=f"cluster-{fg_index}",
                                            processes=cores,
                                            scheduler_options=dict(
                                                dashboard_address=f"{host}:{dashboard_port + index + 1}",
                                                host=host,
                                            ),
                                            use_stdin=use_stdin)
            dashboard_link = clusters[fg_index].dashboard_link
            self.logger.debug(format_log(fg_index, f"Cluster initialized ({dashboard_link})"))

        # Print cluster debug info
        for cluster_key in clusters.keys():
            cluster = clusters[cluster_key]
            if self.debug:
                print("Dashboard link:", cluster.dashboard_link)
                if isinstance(cluster, LSFCluster):
                    print(cluster.job_script())

        # Return clusters
        return clusters

    async def keep_workers_alive(self, fg_index: str):
        # While the job is still executing
        while True:
            # Wait a second
            await asyncio.sleep(WORKER_CHECK_INTERVAL)
            # Check workers
            self.wait_until_workers_alive(fg_index=fg_index)

    def scale_clusters(self, clusters: Dict[str, Union[LocalCluster, LSFCluster]], n_workers: int):
        for cluster_key in clusters.keys():
            cluster = clusters[cluster_key]
            if not isinstance(cluster, LocalCluster):
                cluster.scale(n_workers * 2)
                self.logger.debug(format_log(cluster_key, f"Scaling cluster to {n_workers * 2} nodes"))

    def wait_until_workers_alive(self, fg_index: str, n_workers: int):
        # Get cluster & client
        client = self.clients[fg_index]
        cluster = self.clusters[fg_index]
        # Get current number of workers
        current_n_workers = len(client.scheduler_info()['workers'])
        # Wait until enough number of workers alive
        while client.status == 'running' and current_n_workers < n_workers:
            # Log status
            self.logger.debug(
                format_log(cluster.name, f"{current_n_workers}/{n_workers} workers running"))
            # Ensure loop
            self.__ensure_asyncio_loop()
            # Try correcting state
            # noinspection PyProtectedMember
            cluster._correct_state()
            # Sleep a little
            sleep(WORKER_CHECK_INTERVAL)
            # Get current number of workers
            current_n_workers = len(client.scheduler_info()['workers'])
        # Print result
        self.logger.debug(format_log(cluster.name, "All workers alive"))

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
