import logging
import socket
from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster, PBSCluster
from time import sleep
from .config import ClusterConfig, ClusterType, get_working_dir
from .utils.file_utils import ensure_dir


class ClusterManager(object):
    def __init__(self, config: ClusterConfig):
        self.config = config
        self.working_dir = get_working_dir()
        ensure_dir(self.working_dir)
        ensure_dir(f"{self.working_dir}/worker_logs")

    def boot(self):
        self.cluster = self._initialize_cluster()
        self.client = Client(self.cluster)
        if self.config.type != ClusterType.LOCAL:
            self.cluster.scale(self.config.n_workers)
            logging.info(f"Scaling cluster to {self.config.n_workers} nodes")
            self._wait_until_workers_alive()

    def shutdown(self):
        self.client.close()
        self.cluster.close()

    def _initialize_cluster(self):
        dashboard_address = None
        host_name = None
        if self.config.host is not None:
            host_name = f"{self.config.host}"
        elif self.config.type != ClusterType.LOCAL:
            host_name = socket.gethostname()
        if self.config.dashboard_port is not None:
            assert host_name is not None, 'Host address must be specified'
            dashboard_address = f"{host_name}:{self.config.dashboard_port}"
        if self.config.type == ClusterType.LOCAL:
            return LocalCluster(
                host=host_name,
                local_directory=self.config.local_dir,
                memory_limit=self.config.memory,
                n_workers=self.config.n_workers,
                processes=self.config.processes,
                silence_logs=logging.DEBUG if self.config.debug else logging.CRITICAL,
            )
        elif self.config.type == ClusterType.LSF:
            return LSFCluster(
                cores=self.config.n_workers * self.config.n_threads_per_worker,
                death_timeout=self.config.death_timeout,
                job_directives_skip=self.config.job_directives_skip,
                job_extra_directives=self.config.job_extra_directives,
                local_directory=self.config.local_dir,
                memory=f"{self.config.memory}GB",
                processes=self.config.n_workers,
                scheduler_options=dict(
                    dashboard_address=dashboard_address,
                    host=host_name,
                ),
                use_stdin=self.config.use_stdin,
            )
        elif self.config.type == ClusterType.PBS:
            return PBSCluster(
                cores=self.config.n_workers * self.config.n_threads_per_worker,
                death_timeout=self.config.death_timeout,
                job_directives_skip=self.config.job_directives_skip,
                job_extra_directives=self.config.job_extra_directives,
                local_directory=self.config.local_dir,
                log_directory=f"{self.working_dir}/worker_logs",
                memory=f"{self.config.memory}GB",
                processes=self.config.n_workers,
                scheduler_options=dict(
                    dashboard_address=dashboard_address,
                    host=host_name,
                ),
            )

    def _wait_until_workers_alive(self, sleep_seconds=2):
        active_workers = len(self.client.scheduler_info()['workers'])
        while (
            self.client.status == 'running' and active_workers < self.config.n_workers
        ):
            active_workers = len(self.client.scheduler_info()['workers'])
            logging.debug(
                f"Waiting for workers ({active_workers}/{self.config.n_workers})"
            )
            # Try to force cluster to boot workers
            self.cluster._correct_state()
            # Wait
            sleep(sleep_seconds)
        logging.debug('All workers alive')
