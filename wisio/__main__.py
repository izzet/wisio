import hydra
from distributed import Client
from hydra.utils import instantiate

from . import AnalyzerType, ClusterType, OutputType
from .config import Config, init_hydra_config_store


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
        percentile=cfg.percentile,
        threshold=cfg.threshold,
        view_types=cfg.view_types,
    )
    output: OutputType = instantiate(cfg.output)
    output.handle_result(metrics=cfg.metrics, result=result)


if __name__ == "__main__":
    main()
