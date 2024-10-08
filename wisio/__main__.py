import hydra
from dask_jobqueue import LSFCluster, PBSCluster, SLURMCluster
from distributed import Client, LocalCluster
from hydra.utils import instantiate
from typing import Union

from .analyzer import Analyzer
from .config import Config, init_hydra_config_store
from .dftracer import DFTracerAnalyzer
from .output import ConsoleOutput, CSVOutput, SQLiteOutput
from .recorder import RecorderAnalyzer


try:
    from .darshan import DarshanAnalyzer
except ModuleNotFoundError:
    DarshanAnalyzer = Analyzer


AnalyzerType = Union[DarshanAnalyzer, DFTracerAnalyzer, RecorderAnalyzer]
ClusterType = Union[LocalCluster, LSFCluster, PBSCluster, SLURMCluster]
OutputType = Union[ConsoleOutput, CSVOutput, SQLiteOutput]


init_hydra_config_store()


@hydra.main(version_base=None, config_name="config")
def main(cfg: Config) -> None:
    cluster: ClusterType = instantiate(cfg.cluster)
    client = Client(cluster)
    analyzer: AnalyzerType = instantiate(
        cfg.analyzer,
        debug=cfg.debug,
        verbose=cfg.verbose,
    )
    result = analyzer.analyze_trace(
        trace_path=cfg.trace_path,
        # accuracy=cfg.accuracy,
        exclude_bottlenecks=cfg.exclude_bottlenecks,
        exclude_characteristics=cfg.exclude_characteristics,
        logical_view_types=cfg.logical_view_types,
        metrics=cfg.metrics,
        threshold=cfg.threshold,
        view_types=cfg.view_types,
    )
    output: OutputType = instantiate(cfg.output)
    output.handle_result(metrics=cfg.metrics, result=result)


if __name__ == "__main__":
    main()
