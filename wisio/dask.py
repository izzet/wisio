import logging
from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
from logging import Logger
from socket import gethostname
from .utils.file_utils import ensure_dir
from .utils.logger import format_log


DEFAULT_NODE_MEMORY = 1600
DEFAULT_N_THREADS_PER_WORKER = 16
DEFAULT_N_WORKERS_PER_NODE = 32
DEFAULT_WORKER_TIME = 120
WORKER_CHECK_INTERVAL = 5.0


class ClusterManager(object):

    def __init__(
        self,
        working_dir: str,
        n_clusters: int,
        logger: Logger,
        cluster_settings: dict = None,
        force_local=False,
        verbose=False
    ):
        self.clients = []
        self.cluster_settings = cluster_settings or {}
        self.clusters = []
        self.force_local = force_local
        self.logger = logger
        self.n_clusters = n_clusters
        self.verbose = verbose
        self.working_dir = working_dir

    def boot(self):
        self.clusters = self._initialize_clusters()
        self.clients = self._initialize_clients()
        self.scale_clusters(n_workers=self.cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE))

    def scale_clusters(self, n_workers: int):
        for cluster in self.clusters:
            if not isinstance(cluster, LocalCluster):
                cluster.scale(n_workers)
                self.logger.debug(format_log(cluster.name, f"Scaling cluster to {n_workers} nodes"))

    def shutdown(self):
        for client in self.clients:
            client.close()
        for cluster in self.clusters:
            cluster.close()

    def _initialize_clients(self):
        # Return clients
        return [Client(cluster) for cluster in self.clusters]

    def _initialize_clusters(self):
        # Read config
        cores = self.cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        dashboard_port = self.cluster_settings.get('dashboard_port')
        host = self.cluster_settings.get('host', gethostname())
        local_directory = self.cluster_settings.get('local_directory')
        log_file = self.cluster_settings.get('log_file', "%J.log")
        if not log_file.startswith('/'):
            ensure_dir(f"{self.working_dir}/worker_logs")
            log_file = f"{self.working_dir}/worker_logs/{log_file}"
        memory = self.cluster_settings.get('memory', DEFAULT_NODE_MEMORY)
        processes = self.cluster_settings.get('processes', DEFAULT_N_THREADS_PER_WORKER)
        use_stdin = self.cluster_settings.get('use_stdin', True)
        worker_queue = self.cluster_settings.get('worker_queue')
        worker_time = self.cluster_settings.get('worker_time', DEFAULT_WORKER_TIME)

        # Create distributed clusters
        if self.force_local:
            return [
                LocalCluster(
                    dashboard_address=f"{host}:{dashboard_port + i}",
                    host=host,
                    local_directory=f"{local_directory}/cluster-{i}-local",
                    memory_limit=0,
                    n_workers=cores,
                    name=f"cluster-{i}",
                    silence_logs=logging.DEBUG if self.verbose else logging.CRITICAL
                )
                for i in range(self.n_clusters)
            ]

        return [
            LSFCluster(
                cores=cores * processes,
                death_timeout=worker_time * 60,
                header_skip=['-n', '-R', '-M', '-P', '-W 00:30'],
                job_extra=['-nnodes 1',
                           '-G asccasc',
                           '-q {}'.format(worker_queue),
                           '-W {}'.format(worker_time),
                           '-o {}'.format(log_file),
                           '-e {}'.format(log_file)],
                local_directory=f"{local_directory}/cluster-{i}",
                memory=f"{memory}GB",
                name=f"cluster-{i}",
                processes=cores,
                scheduler_options=dict(
                    dashboard_address=f"{host}:{dashboard_port + i}",
                    host=host,
                ),
                use_stdin=use_stdin
            )
            for i in range(self.n_clusters)
        ]
