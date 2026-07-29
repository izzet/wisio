import hydra
from distributed import Client
from hydra.utils import instantiate

from . import AnalyzerType, ClusterType, OutputType
from .config import Config, init_hydra_config_store


init_hydra_config_store()


@hydra.main(version_base=None, config_name="config")
def main(cfg: Config) -> None:
    # The cluster and client are closed on the way out, in reverse order of
    # creation. Relying on interpreter exit is enough for a one-shot CLI run
    # but not for anything calling main() more than once in a process: the
    # cluster's workers outlive the call, and the next one starts its own on
    # top of them. The test suite does exactly that -- 16 e2e cases in one
    # session -- and every cluster stayed up, so the same test ran in 38s
    # early on and 11 minutes near the end, all of them contending for two
    # runner cores. `finally`, so a failing analysis does not strand workers
    # and turn one broken test into a slow suite.
    cluster: ClusterType = instantiate(cfg.cluster)
    client = Client(cluster)
    try:
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
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
