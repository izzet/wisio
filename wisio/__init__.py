from dask_jobqueue.lsf import LSFCluster
from dask_jobqueue.pbs import PBSCluster
from dask_jobqueue.slurm import SLURMCluster
from dataclasses import dataclass
from distributed import Client, LocalCluster
from hydra import compose, initialize
from hydra.core.hydra_config import HydraConfig
from hydra.utils import instantiate
from omegaconf import DictConfig
from typing import List, Union, Optional

from .analyzer import Analyzer
from .config import init_hydra_config_store
from .dftracer import DFTracerAnalyzer
from .output import ConsoleOutput, CSVOutput, SQLiteOutput
from .recorder import RecorderAnalyzer
from .types import ViewType

try:
    from .darshan import DarshanAnalyzer
except ModuleNotFoundError:
    DarshanAnalyzer = Analyzer

AnalyzerType = Union[DarshanAnalyzer, DFTracerAnalyzer, RecorderAnalyzer]
ClusterType = Union[LocalCluster, LSFCluster, PBSCluster, SLURMCluster]
OutputType = Union[ConsoleOutput, CSVOutput, SQLiteOutput]


@dataclass
class WisIOInstance:
    analyzer: Analyzer
    client: Client
    cluster: ClusterType
    hydra_config: DictConfig
    output: OutputType

    def analyze_trace(
        self,
        percentile: Optional[float] = None,
        threshold: Optional[int] = None,
        view_types: Optional[List[ViewType]] = None,
    ):
        return self.analyzer.analyze_trace(
            exclude_bottlenecks=self.hydra_config.exclude_bottlenecks,
            exclude_characteristics=self.hydra_config.exclude_characteristics,
            logical_view_types=self.hydra_config.logical_view_types,
            metrics=self.hydra_config.metrics,
            percentile=self.hydra_config.percentile if not percentile else percentile,
            threshold=self.hydra_config.threshold if not threshold else threshold,
            trace_path=self.hydra_config.trace_path,
            view_types=self.hydra_config.view_types if not view_types else view_types,
        )


def init_with_hydra(hydra_overrides: List[str]):
    with initialize(version_base=None, config_path=None):
        init_hydra_config_store()
        hydra_config = compose(
            config_name="config",
            overrides=hydra_overrides,
            return_hydra_config=True,
        )
    HydraConfig.instance().set_config(hydra_config)
    cluster = instantiate(hydra_config.cluster)
    client = Client(cluster)
    analyzer = instantiate(
        hydra_config.analyzer,
        debug=hydra_config.debug,
        verbose=hydra_config.verbose,
    )
    output = instantiate(hydra_config.output)
    return WisIOInstance(
        analyzer=analyzer,
        client=client,
        cluster=cluster,
        hydra_config=hydra_config,
        output=output,
    )
