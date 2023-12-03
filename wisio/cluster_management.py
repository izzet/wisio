import logging
import yaml
from dask.distributed import Client, LocalCluster
from dask_jobqueue import LSFCluster
from dataclasses import dataclass
from typing import List, Literal
from .utils.file_utils import ensure_dir


ClusterType = Literal['local', 'lsf']


@dataclass
class ClusterConfig:
    cluster_type: ClusterType = 'local'
    dashboard_port: int = None
    death_timeout: int = None
    debug: bool = False
    header_skip: List[str] = None
    host: str = None
    job_extra: List[str] = None
    local_dir: str = None
    memory: int = 1600
    n_threads_per_worker: int = 16
    n_workers: int = 8
    use_stdin: bool = True


class ClusterManager(object):

    def __init__(self, working_dir: str, config: ClusterConfig):
        self.config = config
        self.working_dir = working_dir
        ensure_dir(working_dir)
        ensure_dir(f"{working_dir}/worker_logs")

    def boot(self):
        self.cluster = self._initialize_cluster()
        self.client = Client(self.cluster)
        if self.config.cluster_type != 'local':
            self.cluster.scale(self.config.n_workers)
            logging.info(f"Scaling cluster to {self.config.n_workers} nodes")

    def shutdown(self):
        self.client.close()
        self.cluster.close()

    def _initialize_cluster(self):
        dashboard_address = None
        if self.config.dashboard_port is not None:
            assert self.config.host is not None, 'Host address must be specified'
            dashboard_address = f"{self.config.host}:{self.config.dashboard_port}"
        if self.config.cluster_type == 'local':
            return LocalCluster(
                # dashboard_address=dashboard_address,
                host=self.config.host,
                local_directory=self.config.local_dir,
                memory_limit=self.config.memory,
                n_workers=self.config.n_workers,
                silence_logs=logging.DEBUG if self.config.debug else logging.CRITICAL,
            )
        elif self.config.cluster_type == 'lsf':
            return LSFCluster(
                cores=self.config.n_workers * self.config.n_threads_per_worker,
                death_timeout=self.config.death_timeout,
                header_skip=self.config.header_skip,
                job_extra=self.config.job_extra,
                local_directory=self.config.local_dir,
                memory=f"{self.config.memory}GB",
                processes=self.config.n_workers,
                scheduler_options=dict(
                    dashboard_address=dashboard_address,
                    host=self.config.host,
                ),
                use_stdin=self.config.use_stdin,
            )


def load_cluster_config(config_path: str):
    with open(config_path) as config_file:
        config = yaml.safe_load(config_file)
        return ClusterConfig(**config)
