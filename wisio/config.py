import logging
import socket
from dataclasses import asdict, dataclass, field
from hydra.core.config_store import ConfigStore
from hydra.conf import HelpConf, JobConf
from omegaconf import MISSING
from typing import Any, Dict, List, Optional

from .constants import COL_TIME_RANGE, VIEW_TYPES, Layer
from .rules import KNOWN_RULES, Rule
from .utils.env_utils import get_bool_env_var


CHECKPOINT_VIEWS = get_bool_env_var("WISIO_CHECKPOINT_VIEWS", False)
HASH_CHECKPOINT_NAMES = get_bool_env_var("WISIO_HASH_CHECKPOINT_NAMES", False)


@dataclass
class AnalyzerConfig:
    additional_metrics: Optional[Dict[str, Optional[str]]] = MISSING
    bottleneck_dir: Optional[str] = "${hydra:runtime.output_dir}/bottlenecks"
    checkpoint: Optional[bool] = True
    checkpoint_dir: Optional[str] = "${hydra:runtime.output_dir}/checkpoints"
    derived_metrics: Optional[Dict[str, Dict[str, list]]] = MISSING
    layer_defs: Dict[str, Optional[str]] = MISSING
    layer_deps: Optional[Dict[str, Optional[str]]] = MISSING
    threaded_layers: Optional[List[str]] = MISSING
    time_approximate: Optional[bool] = True
    time_granularity: Optional[float] = MISSING
    unscored_metrics: Optional[List[str]] = MISSING


@dataclass
class DarshanAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.darshan.DarshanAnalyzer"
    time_granularity: Optional[float] = 1e3


DERIVED_POSIX_METRICS = {
    # 'data_count_sum': ['io_cat == 1 or io_cat == 2', 'count_sum'],
    # 'data_size_sum': ['io_cat == 1 or io_cat == 2', 'size_sum'],
    # 'data_size_max': ['io_cat == 1 or io_cat == 2', 'size_max'],
    # 'data_size_min': ['io_cat == 1 or io_cat == 2', 'size_min'],
    # 'data_time_sum': ['io_cat == 1 or io_cat == 2', 'time_sum'],
    # 'ipc_count_sum': ['io_cat == 5', 'count_sum'],
    # 'ipc_count_mean': ['io_cat == 5', 'count_mean'],
    # 'ipc_time_sum': ['io_cat == 5', 'time_sum'],
    # 'ipc_time_mean': ['io_cat == 5', 'time_mean'],
    # 'metadata_count_sum': ['io_cat == 3', 'count_sum'],
    # 'metadata_time_sum': ['io_cat == 3', 'time_sum'],
    'other_count_sum': ['io_cat == 6', 'count_sum'],
    # 'other_count_mean': ['io_cat == 6', 'count_mean'],
    'other_time_sum': ['io_cat == 6', 'time_sum'],
    # 'other_time_mean': ['io_cat == 6', 'time_mean'],
    # 'pctl_count_sum': ['io_cat == 4', 'count_sum'],
    # 'pctl_count_mean': ['io_cat == 4', 'count_mean'],
    # 'pctl_time_sum': ['io_cat == 4', 'time_sum'],
    # 'pctl_time_mean': ['io_cat == 4', 'time_mean'],
    'read_count_sum': ['io_cat == 1', 'count_sum'],
    # 'read_count_mean': ['io_cat == 1', 'count_mean'],
    'read_size_sum': ['io_cat == 1', 'size_sum'],
    # 'read_size_mean': ['io_cat == 1', 'size_mean'],
    'read_size_max': ['io_cat == 1', 'size_max'],
    'read_size_min': ['io_cat == 1', 'size_min'],
    'read_time_sum': ['io_cat == 1', 'time_sum'],
    # 'read_time_mean': ['io_cat == 1', 'time_mean'],
    'sync_count_sum': ['io_cat == 7', 'count_sum'],
    # 'sync_count_mean': ['io_cat == 7', 'count_mean'],
    'sync_time_sum': ['io_cat == 7', 'time_sum'],
    # 'sync_time_mean': ['io_cat == 7', 'time_mean'],
    'write_count_sum': ['io_cat == 2', 'count_sum'],
    # 'write_count_mean': ['io_cat == 2', 'count_mean'],
    'write_size_sum': ['io_cat == 2', 'size_sum'],
    # 'write_size_mean': ['io_cat == 2', 'size_mean'],
    'write_size_max': ['io_cat == 2', 'size_max'],
    'write_size_min': ['io_cat == 2', 'size_min'],
    'write_time_sum': ['io_cat == 2', 'time_sum'],
    # 'write_time_mean': ['io_cat == 2', 'time_mean'],
    'close_count_sum': [
        'io_cat == 3 and func_name.str.contains("close") and ~func_name.str.contains("dir")',
        'count_sum',
    ],
    # 'close_count_mean': [
    #     'io_cat == 3 and func_name.str.contains("close") and ~func_name.str.contains("dir")',
    #     'count_mean',
    # ],
    'close_time_sum': [
        'io_cat == 3 and func_name.str.contains("close") and ~func_name.str.contains("dir")',
        'time_sum',
    ],
    # 'close_time_mean': [
    #     'io_cat == 3 and func_name.str.contains("close") and ~func_name.str.contains("dir")',
    #     'time_mean',
    # ],
    'open_count_sum': [
        'io_cat == 3 and func_name.str.contains("open") and ~func_name.str.contains("dir")',
        'count_sum',
    ],
    # 'open_count_mean': [
    #     'io_cat == 3 and func_name.str.contains("open") and ~func_name.str.contains("dir")',
    #     'count_mean',
    # ],
    'open_time_sum': [
        'io_cat == 3 and func_name.str.contains("open") and ~func_name.str.contains("dir")',
        'time_sum',
    ],
    # 'open_time_mean': [
    #     'io_cat == 3 and func_name.str.contains("open") and ~func_name.str.contains("dir")',
    #     'time_mean',
    # ],
    'seek_count_sum': [
        'io_cat == 3 and func_name.str.contains("seek")',
        'count_sum',
    ],
    # 'seek_count_mean': [
    #     'io_cat == 3 and func_name.str.contains("seek")',
    #     'count_mean',
    # ],
    'seek_time_sum': [
        'io_cat == 3 and func_name.str.contains("seek")',
        'time_sum',
    ],
    # 'seek_time_mean': [
    #     'io_cat == 3 and func_name.str.contains("seek")',
    #     'time_mean',
    # ],
    'stat_count_sum': [
        'io_cat == 3 and func_name.str.contains("stat")',
        'count_sum',
    ],
    # 'stat_count_mean': [
    #     'io_cat == 3 and func_name.str.contains("stat")',
    #     'count_mean',
    # ],
    'stat_time_sum': [
        'io_cat == 3 and func_name.str.contains("stat")',
        'time_sum',
    ],
    # 'stat_time_mean': [
    #     'io_cat == 3 and func_name.str.contains("stat")',
    #     'time_mean',
    # ],
}


@dataclass
class DFTracerAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.dftracer.DFTracerAnalyzer"
    layer_defs: Dict[str, Optional[str]] = field(
        default_factory=lambda: {
            'app': 'func_name == "DLIOBenchmark.run"',
            'training': 'func_name == "DLIOBenchmark._train"',
            'compute': 'cat == "ai_framework"',
            'fetch_data': 'func_name.isin(["<module>.iter", "fetch-data.iter", "loop.iter"])',
            'data_loader': 'cat == "data_loader" & ~func_name.isin(["loop.iter", "loop.yield"])',
            'data_loader_fork': 'cat == "posix" & func_name == "fork"',
            'reader': 'cat == "reader"',
            'reader_posix': 'cat.str.contains("posix|stdio") & cat.str.contains("_reader")',
            'reader_posix_lustre': 'cat.str.contains("posix|stdio") & cat.str.contains("_reader_lustre")',
            # 'reader_posix_ssd': 'cat.str.contains("posix|stdio") & cat.str.contains("_reader_ssd")',
            'checkpoint': 'cat == "checkpoint"',
            'checkpoint_posix': 'cat.str.contains("posix|stdio") & cat.str.contains("_checkpoint")',
            'checkpoint_posix_lustre': 'cat.str.contains("posix|stdio") & cat.str.contains("_checkpoint_lustre")',
            'checkpoint_posix_ssd': 'cat.str.contains("posix|stdio") & cat.str.contains("_checkpoint_ssd")',
            'other_posix': 'cat.isin(["posix", "stdio"])',
            'other_posix_lustre': 'cat.isin(["posix_lustre", "stdio_lustre"])',
            # 'other_posix_ssd': 'cat.isin(["posix_ssd", "stdio_ssd"])',
        }
    )
    layer_deps: Optional[Dict[str, Optional[str]]] = field(
        default_factory=lambda: {
            'app': None,
            'training': 'app',
            'compute': 'training',
            'fetch_data': 'training',
            'data_loader': 'fetch_data',
            'data_loader_fork': 'fetch_data',
            'reader': 'data_loader',
            'reader_posix': 'reader',
            'reader_posix_lustre': 'reader_posix',
            # 'reader_posix_ssd': 'reader_posix',
            'checkpoint': 'training',
            'checkpoint_posix': 'checkpoint',
            'checkpoint_posix_lustre': 'checkpoint_posix',
            'checkpoint_posix_ssd': 'checkpoint_posix',
            'other_posix': None,
            'other_posix_lustre': 'other_posix',
            # 'other_posix_ssd': 'other_posix',
        }
    )
    threaded_layers: Optional[List[str]] = field(
        default_factory=lambda: [
            'data_loader',
            'data_loader_fork',
            'reader',
            'reader_posix',
            'reader_posix_lustre',
            # 'reader_posix_ssd',
        ]
    )
    derived_metrics: Optional[Dict[str, Dict[str, list]]] = field(
        default_factory=lambda: {
            'app': {},
            'training': {},
            'compute': {},
            'fetch_data': {},
            'data_loader': {
                'init_time_sum': [
                    'func_name.str.contains("init")',
                    'time_sum',
                ],
                'item_count_sum': [
                    'func_name.str.contains("__getitem__")',
                    'count_sum',
                ],
                'item_time_sum': [
                    'func_name.str.contains("__getitem__")',
                    'time_sum',
                ],
            },
            'data_loader_fork': {},
            'reader': {
                'close_count_sum': [
                    'func_name.str.contains(".close")',
                    'count_sum',
                ],
                'close_time_sum': [
                    'func_name.str.contains(".close")',
                    'time_sum',
                ],
                'open_count_sum': [
                    'func_name.str.contains(".open")',  # e.g. NPZReader.open
                    'count_sum',
                ],
                'open_time_sum': [
                    'func_name.str.contains(".open")',  # e.g. NPZReader.open
                    'time_sum',
                ],
                'preprocess_count_sum': [
                    'func_name.str.contains(".preprocess")',
                    'count_sum',
                ],
                'preprocess_time_sum': [
                    'func_name.str.contains(".preprocess")',
                    'time_sum',
                ],
                'sample_count_sum': [
                    'func_name.str.contains(".get_sample")',
                    'count_sum',
                ],
                'sample_time_sum': [
                    'func_name.str.contains(".get_sample")',
                    'time_sum',
                ],
            },
            'reader_posix': {},
            'reader_posix_lustre': DERIVED_POSIX_METRICS,
            # 'reader_posix_ssd': DERIVED_POSIX_METRICS,
            'checkpoint': {},
            'checkpoint_posix': {},
            'checkpoint_posix_lustre': DERIVED_POSIX_METRICS,
            'checkpoint_posix_ssd': DERIVED_POSIX_METRICS,
            'other_posix': {},
            'other_posix_lustre': DERIVED_POSIX_METRICS,
            # 'other_posix_ssd': DERIVED_POSIX_METRICS,
        }
    )
    additional_metrics: Optional[Dict[str, Optional[str]]] = field(
        default_factory=lambda: {
            'compute_util': 'compute_time_sum.fillna(0) / (compute_time_sum.fillna(0) + fetch_data_time_sum.fillna(0) + checkpoint_time_sum.fillna(0))',
            'consumer_rate': 'data_loader_item_count_sum / compute_time_sum',
            'producer_rate': 'data_loader_item_count_sum / data_loader_item_time_sum',
            'producer_consumer_rate': 'producer_rate / consumer_rate',
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
    unscored_metrics: Optional[List[str]] = field(
        default_factory=lambda: [
            'consumer_rate',
            'producer_rate',
        ]
    )


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
    output: OutputConfig = MISSING
    percentile: Optional[float] = None
    threshold: Optional[int] = None
    time_granularity: Optional[float] = 1e6
    time_view_type: Optional[str] = COL_TIME_RANGE
    trace_path: str = MISSING
    verbose: Optional[bool] = False
    view_types: Optional[List[str]] = field(default_factory=lambda: VIEW_TYPES)
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
