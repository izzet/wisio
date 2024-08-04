import os
from dataclasses import dataclass, field
from enum import Enum
from hydra.core.hydra_config import HydraConfig
from omegaconf import MISSING
from typing import List, Optional
from .constants import VIEW_TYPES


class AnalysisType(Enum):
    RECORDER = "recorder"
    DARSHAN = "darshan"
    DFTRACER = "dftracer"


class ClusterType(Enum):
    LOCAL = "local"
    LSF = "lsf"
    PBS = "pbs"


class OutputType(Enum):
    CONSOLE = "console"
    CSV = "csv"
    SQLITE = "sqlite"


@dataclass
class AnalysisConfig:
    bottleneck_dir: str
    trace_path: str
    type: AnalysisType = AnalysisType.DARSHAN
    exclude_bottlenecks: Optional[List[str]] = field(default_factory=list)
    exclude_characteristics: Optional[List[str]] = field(default_factory=list)
    metrics: Optional[List[str]] = field(default_factory=lambda: ["iops"])
    logical_view_types: Optional[bool] = False
    threshold: Optional[int] = 45
    time_granularity: Optional[float] = 1e6
    view_types: Optional[List[str]] = field(default_factory=lambda: VIEW_TYPES)


@dataclass
class CheckpointConfig:
    dir: str
    enabled: Optional[bool] = True


@dataclass
class ClusterConfig:
    type: ClusterType = ClusterType.LOCAL
    dashboard_port: Optional[int] = 0
    debug: Optional[bool] = False
    host: Optional[str] = ""
    local_dir: Optional[str] = MISSING
    memory: Optional[int] = 0
    n_threads_per_worker: Optional[int] = 16
    n_workers: Optional[int] = 1
    processes: Optional[bool] = False


@dataclass
class OutputConfig:
    type: OutputType = OutputType.CONSOLE
    compact: Optional[bool] = False
    group_behavior: Optional[bool] = False
    max_bottlenecks: Optional[int] = 3
    name: Optional[str] = ""
    root_only: Optional[bool] = False
    show_debug: Optional[bool] = False
    show_characteristics: Optional[bool] = True
    show_header: Optional[bool] = True
    view_names: Optional[List[str]] = field(default_factory=list)


@dataclass
class MainConfig:
    analysis: AnalysisConfig
    checkpoint: CheckpointConfig
    cluster: ClusterConfig
    output: OutputConfig
    debug: Optional[bool] = False
    verbose: Optional[bool] = False


def get_working_dir() -> str:
    if HydraConfig.initialized():
        return HydraConfig.get().run.dir
    if os.environ.get("WISIO_DIR"):
        return os.environ["WISIO_DIR"]
    return ".wisio"
