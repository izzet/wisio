import logging
from dataclasses import asdict, dataclass, field
from hydra.core.config_store import ConfigStore
from hydra.conf import HelpConf, JobConf
from omegaconf import MISSING
from typing import Any, Dict, List, Optional

from .constants import VIEW_TYPES


@dataclass
class AnalyzerConfig:
    bottleneck_dir: Optional[str] = "${hydra:runtime.output_dir}/bottlenecks"
    checkpoint: Optional[bool] = True
    checkpoint_dir: Optional[str] = "${hydra:runtime.output_dir}/checkpoints"
    debug: Optional[bool] = False
    time_approximate: Optional[bool] = True
    time_granularity: Optional[float] = MISSING
    verbose: Optional[bool] = False


@dataclass
class DarshanAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.darshan.DarshanAnalyzer"
    time_granularity: Optional[float] = 1e3


@dataclass
class DFTracerAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.dftracer.DFTracerAnalyzer"
    time_granularity: Optional[float] = 1e6


@dataclass
class RecorderAnalyzerConfig(AnalyzerConfig):
    _target_: str = "wisio.recorder.RecorderAnalyzer"
    time_granularity: Optional[float] = 1e7


@dataclass
class ClusterConfig:
    local_directory: Optional[str] = "/tmp/${hydra:job.name}/${hydra:job.id}"


@dataclass
class JobQueueClusterSchedulerConfig:
    dashboard_address: Optional[str] = None
    host: Optional[str] = None


@dataclass
class JobQueueClusterConfig:
    cores: int = 1
    death_timeout: Optional[int] = 60
    job_directives_skip: Optional[List[str]] = field(default_factory=list)
    job_extra_directives: Optional[List[str]] = field(default_factory=list)
    log_directory: Optional[str] = ""
    memory: Optional[str] = "8GB"
    processes: Optional[int] = 8
    scheduler_options: Optional[JobQueueClusterSchedulerConfig] = None


@dataclass
class LocalClusterConfig(ClusterConfig):
    _target_: str = "dask.distributed.LocalCluster"
    host: Optional[str] = ""
    memory_limit: Optional[int] = None
    n_workers: Optional[int] = None
    processes: Optional[bool] = False
    silence_logs: Optional[int] = logging.CRITICAL


@dataclass
class LSFClusterConfig(JobQueueClusterConfig):
    _target_: str = "dask_jobqueue.LSFCluster"
    use_stdin: Optional[bool] = True


@dataclass
class PBSClusterConfig(JobQueueClusterConfig):
    _target_: str = "dask_jobqueue.PBSCluster"


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
    cluster: ClusterConfig = MISSING
    debug: Optional[bool] = False
    exclude_bottlenecks: Optional[List[str]] = field(default_factory=list)
    exclude_characteristics: Optional[List[str]] = field(default_factory=list)
    metrics: Optional[List[str]] = field(default_factory=lambda: ["iops"])
    logical_view_types: Optional[bool] = False
    output: OutputConfig = MISSING
    threshold: Optional[int] = 45
    time_granularity: Optional[float] = 1e6
    trace_path: str = MISSING
    verbose: Optional[bool] = False
    view_types: Optional[List[str]] = field(default_factory=lambda: VIEW_TYPES)


def init_hydra_config_store() -> None:
    cs = ConfigStore.instance()
    cs.store(group="hydra/help", name="custom", node=asdict(CustomHelpConfig()))
    cs.store(group="hydra/job", name="custom", node=CustomJobConfig)
    cs.store(group="hydra/job_logging", name="custom", node=CustomLoggingConfig)
    cs.store(name="config", node=Config)
    cs.store(group="analyzer", name="darshan", node=DarshanAnalyzerConfig)
    cs.store(group="analyzer", name="dftracer", node=DFTracerAnalyzerConfig)
    cs.store(group="analyzer", name="recorder", node=RecorderAnalyzerConfig)
    cs.store(group="cluster", name="local", node=LocalClusterConfig)
    cs.store(group="cluster", name="lsf", node=LSFClusterConfig)
    cs.store(group="cluster", name="pbs", node=PBSClusterConfig)
    cs.store(group="output", name="console", node=ConsoleOutputConfig)
    cs.store(group="output", name="csv", node=CSVOutputConfig)
    cs.store(group="output", name="sqlite", node=SQLiteOutputConfig)
