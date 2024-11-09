import hydra
from dask_jobqueue import LSFCluster, PBSCluster, SLURMCluster
from distributed import Client, LocalCluster
from hydra.utils import instantiate
from typing import Union

from .analyzer import Analyzer
from .config import Config, init_hydra_config_store
from .cluster import ExternalCluster
from .dftracer import DFTracerAnalyzer
from .output import ConsoleOutput, CSVOutput, SQLiteOutput
from .recorder import RecorderAnalyzer
from .types import Rule


try:
    from .darshan import DarshanAnalyzer
except ModuleNotFoundError:
    DarshanAnalyzer = Analyzer


AnalyzerType = Union[DarshanAnalyzer, DFTracerAnalyzer, RecorderAnalyzer]
ClusterType = Union[ExternalCluster, LocalCluster, LSFCluster, PBSCluster, SLURMCluster]
OutputType = Union[ConsoleOutput, CSVOutput, SQLiteOutput]


init_hydra_config_store()


@hydra.main(version_base=None, config_name="config")
def main(cfg: Config) -> None:
    cluster: ClusterType = instantiate(cfg.cluster)
    if isinstance(cluster, ExternalCluster):
        client = Client(cluster.scheduler_address)
        if cluster.restart_on_connect:
            client.restart()
    else:
        client = Client(cluster)
    analyzer: AnalyzerType = instantiate(
        cfg.analyzer,
        debug=cfg.debug,
        verbose=cfg.verbose,
    )
    result = analyzer.analyze_trace(
        bottleneck_rules={
            rule: Rule(**rule_def) for rule, rule_def in cfg.bottleneck_rules.items()
        },
        exclude_bottlenecks=cfg.exclude_bottlenecks,
        exclude_characteristics=cfg.exclude_characteristics,
        logical_view_types=cfg.logical_view_types,
        metrics=cfg.metrics,
        percentile=cfg.percentile,
        threshold=cfg.threshold,
        trace_path=cfg.trace_path,
        unoverlapped_posix_only=cfg.unoverlapped_posix_only,
        view_types=cfg.view_types,
    )
    analyzer.write_bottlenecks(result.flat_bottlenecks)
    output: OutputType = instantiate(cfg.output)
    output.handle_result(metrics=cfg.posix_metrics, result=result)


if __name__ == "__main__":
    main()
