import logging
import socket
from dataclasses import asdict, dataclass, field
from hydra.core.config_store import ConfigStore
from hydra.conf import HelpConf, JobConf
from omegaconf import MISSING
from typing import Any, Dict, List, Optional

from .constants import VIEW_TYPES, Layer
from .rules import KNOWN_RULES, Rule
from .utils.env_utils import get_bool_env_var


CHECKPOINT_VIEWS = get_bool_env_var("WISIO_CHECKPOINT_VIEWS", False)
HASH_CHECKPOINT_NAMES = get_bool_env_var("WISIO_HASH_CHECKPOINT_NAMES", True)


@dataclass
class AnalyzerConfig:
    bottleneck_dir: Optional[str] = "${hydra:runtime.output_dir}/bottlenecks"
    checkpoint: Optional[bool] = True
    checkpoint_dir: Optional[str] = "${hydra:runtime.output_dir}/checkpoints"
    layer_defs: Dict[str, str] = MISSING
    layer_deps: Optional[Dict[str, Optional[str]]] = MISSING
    time_approximate: Optional[bool] = True
    time_granularity: Optional[float] = MISSING


@dataclass
class DarshanAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.darshan.DarshanAnalyzer"
    time_granularity: Optional[float] = 1e3


@dataclass
class DFTracerAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.dftracer.DFTracerAnalyzer"
    layer_defs: Dict[str, str] = field(
        default_factory=lambda: {
            'dlio': 'cat.isin(["dlio_benchmark"])',
            'compute': 'cat.isin(["ai_framework"])',
            'dataloader': 'cat.isin(["data_loader"])',
            'image': 'func_name.str.contains(".__getitem__")',
            'reader': 'cat.isin(["reader"])',
            'posix': 'cat.isin(["POSIX", "STDIO", "POSIX_lustre", "POSIX_ssd", "STDIO_lustre", "STDIO_ssd"])',
            'checkpoint': 'cat.isin(["checkpoint"]) & ~func_name.str.contains("checkpoint_start_|checkpoint_end_|.save_state")',
        }
    )
    layer_deps: Optional[Dict[str, Optional[str]]] = field(
        default_factory=lambda: {
            'dlio': None,
            'compute': 'dlio',
            'dataloader': None,
            'image': 'dataloader',
            'reader': 'image',
            'posix': 'reader',
            'checkpoint': None,
        }
    )
    derived_metrics: Dict[str, Dict[str, list]] = field(
        default_factory=lambda: {
            'dlio': {
                'compute_time': [
                    'func_name.isin(["<module>.yield"])',
                    'time',
                ],
                'stall_time': [
                    'func_name.isin(["<module>.iter"])',
                    'time',
                ],
            },
            'compute': {},
            'dataloader': {
                'sample_time': [
                    'func_name.str.contains(".__getitem__|._parse_image")',
                    'time',
                ],
            },
            'image': {},
            'reader': {
                'preprocess_time': [
                    'func_name.str.contains(".preprocess")',
                    'time',
                ],
                'sample_size': [
                    'func_name.str.contains(".get_sample")',
                    'size',
                ],
                'sample_size_max': [
                    'func_name.str.contains(".get_sample")',
                    'size_max',
                ],
                'sample_size_min': [
                    'func_name.str.contains(".get_sample")',
                    'size_min',
                ],
                'sample_time': [
                    'func_name.str.contains(".get_sample")',  # for unet3d check open
                    'time',
                ],
            },
            'posix': {
                'data_count': ['io_cat == 1 or io_cat == 2', 'count'],
                'data_size': ['io_cat == 1 or io_cat == 2', 'size'],
                'data_size_max': ['io_cat == 1 or io_cat == 2', 'size_max'],
                'data_size_min': ['io_cat == 1 or io_cat == 2', 'size_min'],
                'data_time': ['io_cat == 1 or io_cat == 2', 'time'],
                'ipc_count': ['io_cat == 5', 'count'],
                'ipc_time': ['io_cat == 5', 'time'],
                'metadata_count': ['io_cat == 3', 'count'],
                'metadata_time': ['io_cat == 3', 'time'],
                'metadata_lustre_count': [
                    'io_cat == 3 and cat.str.endswith("_lustre")',
                    'count',
                ],
                'metadata_ssd_count': [
                    'io_cat == 3 and cat.str.endswith("_ssd")',
                    'count',
                ],
                'metadata_lustre_time': [
                    'io_cat == 3 and cat.str.endswith("_lustre")',
                    'time',
                ],
                'metadata_ssd_time': [
                    'io_cat == 3 and cat.str.endswith("_ssd")',
                    'time',
                ],
                'other_count': ['io_cat == 6', 'count'],
                'other_time': ['io_cat == 6', 'time'],
                'pctl_count': ['io_cat == 4', 'count'],
                'pctl_time': ['io_cat == 4', 'time'],
                'read_count': ['io_cat == 1', 'count'],
                'read_lustre_count': [
                    'io_cat == 1 and cat.str.endswith("_lustre")',
                    'count',
                ],
                'read_ssd_count': ['io_cat == 1 and cat.str.endswith("_ssd")', 'count'],
                'read_size': ['io_cat == 1', 'size'],
                'read_lustre_size': [
                    'io_cat == 1 and cat.str.endswith("_lustre")',
                    'size',
                ],
                'read_ssd_size': ['io_cat == 1 and cat.str.endswith("_ssd")', 'size'],
                'read_size_max': ['io_cat == 1', 'size_max'],
                'read_size_min': ['io_cat == 1', 'size_min'],
                'read_time': ['io_cat == 1', 'time'],
                'read_lustre_time': [
                    'io_cat == 1 and cat.str.endswith("_lustre")',
                    'time',
                ],
                'read_ssd_time': ['io_cat == 1 and cat.str.endswith("_ssd")', 'time'],
                'write_count': ['io_cat == 2', 'count'],
                'write_lustre_count': [
                    'io_cat == 2 and cat.str.endswith("_lustre")',
                    'count',
                ],
                'write_ssd_count': [
                    'io_cat == 2 and cat.str.endswith("_ssd")',
                    'count',
                ],
                'write_size': ['io_cat == 2', 'size'],
                'write_lustre_size': [
                    'io_cat == 2 and cat.str.endswith("_lustre")',
                    'size',
                ],
                'write_ssd_size': ['io_cat == 2 and cat.str.endswith("_ssd")', 'size'],
                'write_size_max': ['io_cat == 2', 'size_max'],
                'write_size_min': ['io_cat == 2', 'size_min'],
                'write_time': ['io_cat == 2', 'time'],
                'write_lustre_time': [
                    'io_cat == 2 and cat.str.endswith("_lustre")',
                    'time',
                ],
                'write_ssd_time': ['io_cat == 2 and cat.str.endswith("_ssd")', 'time'],
                'close_count': [
                    'io_cat == 3 and func_name.str.contains("close") and ~func_name.str.contains("dir")',
                    'count',
                ],
                'close_time': [
                    'io_cat == 3 and func_name.str.contains("close") and ~func_name.str.contains("dir")',
                    'time',
                ],
                'open_count': [
                    'io_cat == 3 and func_name.str.contains("open") and ~func_name.str.contains("dir")',
                    'count',
                ],
                'open_time': [
                    'io_cat == 3 and func_name.str.contains("open") and ~func_name.str.contains("dir")',
                    'time',
                ],
                'seek_count': [
                    'io_cat == 3 and func_name.str.contains("seek")',
                    'count',
                ],
                'seek_time': [
                    'io_cat == 3 and func_name.str.contains("seek")',
                    'time',
                ],
                'stat_count': [
                    'io_cat == 3 and func_name.str.contains("stat")',
                    'count',
                ],
                'stat_time': [
                    'io_cat == 3 and func_name.str.contains("stat")',
                    'time',
                ],
            },
            'checkpoint': {},
        }
    )
    additional_metrics: Optional[Dict[str, Optional[str]]] = field(
        default_factory=lambda: {
            'dlio_compute_util': 'dlio_compute_time / (dlio_compute_time + dlio_stall_time + checkpoint_time.fillna(0))',
        }
    )
    logical_views: Dict[str, Dict[str, Optional[str]]] = field(
        default_factory=lambda: {
            'file_name': {
                'file_dir': None,
                'file_pattern': None,
            },
            'proc_name': {
                'host_name': 'proc_name.str.split("#").str[1]',
                'proc_id': 'proc_name.str.split("#").str[2]',
                'thread_id': 'proc_name.str.split("#").str[3]',
            },
        }
    )
    time_granularity: Optional[float] = 1e6


@dataclass
class RecorderAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.recorder.RecorderAnalyzer"
    time_granularity: Optional[float] = 1e7


@dataclass
class ClusterConfig:
    local_directory: Optional[str] = "/tmp/${hydra:job.name}/${hydra:job.id}"


@dataclass
class ExternalClusterConfig(ClusterConfig):
    _target_: str = "wisio.cluster.ExternalCluster"
    restart_on_connect: Optional[bool] = False
    scheduler_address: Optional[str] = MISSING


@dataclass
class JobQueueClusterSchedulerConfig:
    dashboard_address: Optional[str] = None
    host: Optional[str] = field(default_factory=socket.gethostname)


@dataclass
class JobQueueClusterConfig(ClusterConfig):
    cores: int = 16  # ncores
    death_timeout: Optional[int] = 60
    job_directives_skip: Optional[List[str]] = field(default_factory=list)
    job_extra_directives: Optional[List[str]] = field(default_factory=list)
    log_directory: Optional[str] = ""
    memory: Optional[str] = None
    processes: Optional[int] = 1  # nnodes
    scheduler_options: Optional[JobQueueClusterSchedulerConfig] = field(
        default_factory=JobQueueClusterSchedulerConfig
    )


@dataclass
class LocalClusterConfig(ClusterConfig):
    _target_: str = "dask.distributed.LocalCluster"
    host: Optional[str] = None
    memory_limit: Optional[int] = None
    n_workers: Optional[int] = None
    processes: Optional[bool] = True
    silence_logs: Optional[int] = logging.CRITICAL


@dataclass
class LSFClusterConfig(JobQueueClusterConfig):
    _target_: str = "dask_jobqueue.LSFCluster"
    use_stdin: Optional[bool] = True


@dataclass
class PBSClusterConfig(JobQueueClusterConfig):
    _target_: str = "dask_jobqueue.PBSCluster"


@dataclass
class SLURMClusterConfig(JobQueueClusterConfig):
    _target_: str = "dask_jobqueue.SLURMCluster"


@dataclass
class OutputConfig:
    compact: Optional[bool] = False
    group_behavior: Optional[bool] = False
    name: Optional[str] = ""
    root_only: Optional[bool] = False
    view_names: Optional[List[str]] = field(default_factory=list)


@dataclass
class ConsoleOutputConfig(OutputConfig):
    _target_: str = "wisio.output.ConsoleOutput"
    max_bottlenecks: Optional[int] = 3
    show_debug: Optional[bool] = False
    show_characteristics: Optional[bool] = True
    show_header: Optional[bool] = True


@dataclass
class CSVOutputConfig(OutputConfig):
    _target_: str = "wisio.output.CSVOutput"


@dataclass
class SQLiteOutputConfig(OutputConfig):
    _target_: str = "wisio.output.SQLiteOutput"
    run_db_path: Optional[str] = ""


@dataclass
class CustomJobConfig(JobConf):
    name: str = "wisio"


@dataclass
class CustomHelpConfig(HelpConf):
    app_name: str = "WisIO"
    header: str = "${hydra:help.app_name}: Workflow I/O Analysis Tool"
    footer: str = field(
        default_factory=lambda: """
Powered by Hydra (https://hydra.cc)

Use --hydra-help to view Hydra specific help
    """.strip()
    )
    template: str = field(
        default_factory=lambda: """
${hydra:help.header}

== Configuration groups ==

Compose your configuration from those groups (group=option)

$APP_CONFIG_GROUPS
== Config ==

Override anything in the config (foo.bar=value)

$CONFIG
${hydra:help.footer}
    """.strip()
    )


@dataclass
class CustomLoggingConfig:
    version: int = 1
    formatters: Dict[str, Any] = field(
        default_factory=lambda: {
            "simple": {
                "datefmt": "%H:%M:%S",
                "format": "[%(levelname)s] [%(asctime)s.%(msecs)03d] %(message)s [%(pathname)s:%(lineno)d]",
            }
        }
    )
    handlers: Dict[str, Any] = field(
        default_factory=lambda: {
            "file": {
                "class": "logging.FileHandler",
                "formatter": "simple",
                "filename": "${hydra:runtime.output_dir}/${hydra:job.name}.log",
            },
        }
    )
    root: Dict[str, Any] = field(
        default_factory=lambda: {
            "level": "INFO",
            "handlers": ["file"],
        }
    )
    disable_existing_loggers: bool = False


@dataclass
class Config:
    defaults: List[Any] = field(
        default_factory=lambda: [
            {"hydra/job": "custom"},
            {"cluster": "local"},
            {"output": "console"},
            "_self_",
            {"override hydra/help": "custom"},
            {"override hydra/job_logging": "custom"},
        ]
    )
    analyzer: AnalyzerConfig = MISSING
    bottleneck_rules: Optional[Dict[str, Rule]] = field(
        default_factory=lambda: KNOWN_RULES
    )
    cluster: ClusterConfig = MISSING
    debug: Optional[bool] = False
    exclude_bottlenecks: Optional[List[str]] = field(default_factory=list)
    exclude_characteristics: Optional[List[str]] = field(default_factory=list)
    logical_view_types: Optional[bool] = False
    metrics: Optional[Dict[Layer, List[str]]] = field(
        default_factory=lambda: {Layer.POSIX: ["iops"]}
    )
    output: OutputConfig = MISSING
    percentile: Optional[float] = None
    threshold: Optional[int] = None
    time_granularity: Optional[float] = 1e6
    time_view_type: Optional[str] = None
    trace_path: str = MISSING
    verbose: Optional[bool] = False
    view_types: Optional[Dict[Layer, List[str]]] = field(
        default_factory=lambda: {Layer.POSIX: VIEW_TYPES}
    )
    unoverlapped_posix_only: Optional[bool] = False


def init_hydra_config_store() -> ConfigStore:
    cs = ConfigStore.instance()
    cs.store(group="hydra/help", name="custom", node=asdict(CustomHelpConfig()))
    cs.store(group="hydra/job", name="custom", node=CustomJobConfig)
    cs.store(group="hydra/job_logging", name="custom", node=CustomLoggingConfig)
    cs.store(name="config", node=Config)
    cs.store(group="analyzer", name="darshan", node=DarshanAnalyzerConfig)
    cs.store(group="analyzer", name="dftracer", node=DFTracerAnalyzerConfig)
    cs.store(group="analyzer", name="recorder", node=RecorderAnalyzerConfig)
    cs.store(group="cluster", name="external", node=ExternalClusterConfig)
    cs.store(group="cluster", name="local", node=LocalClusterConfig)
    cs.store(group="cluster", name="lsf", node=LSFClusterConfig)
    cs.store(group="cluster", name="pbs", node=PBSClusterConfig)
    cs.store(group="cluster", name="slurm", node=SLURMClusterConfig)
    cs.store(group="output", name="console", node=ConsoleOutputConfig)
    cs.store(group="output", name="csv", node=CSVOutputConfig)
    cs.store(group="output", name="sqlite", node=SQLiteOutputConfig)
    return cs
