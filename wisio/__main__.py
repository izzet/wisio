import hydra
from hydra.core.config_store import ConfigStore

from .analyzer import Analyzer
from .analyzer_result import AnalysisResult
from .config import AnalysisType, MainConfig, OutputConfig, OutputType
from .dftracer import DFTracerAnalyzer
from .recorder import RecorderAnalyzer


try:
    from .darshan import DarshanAnalyzer
except ModuleNotFoundError:
    from unittest.mock import Mock

    def raise_error(*args, **kwargs):
        raise RuntimeError("'darshan' module is not found")

    DarshanAnalyzer = Mock(spec=Analyzer, side_effect=raise_error)


def _handle_output(output_config: OutputConfig, result: AnalysisResult):
    if output_config.type == OutputType.CONSOLE:
        result.output.console(
            compact=output_config.compact,
            group_behavior=output_config.group_behavior,
            max_bottlenecks=output_config.max_bottlenecks,
            name=output_config.name,
            root_only=output_config.root_only,
            show_debug=output_config.show_debug,
            show_characteristics=output_config.show_characteristics,
            show_header=output_config.show_header,
            view_names=output_config.view_names,
        )
    elif output_config.type == OutputType.CSV:
        result.output.csv(
            max_bottlenecks=output_config.max_bottlenecks,
            name=output_config.name,
            show_debug=output_config.show_debug,
        )
    elif output_config.type == OutputType.SQLITE:
        result.output.sqlite(
            name=output_config.name,
            run_db_path=output_config.run_db_path,
        )


def handle_darshan(config: MainConfig):
    analyzer = DarshanAnalyzer(
        analysis_config=config.analysis,
        checkpoint_config=config.checkpoint,
        cluster_config=config.cluster,
        output_config=config.output,
        debug=config.debug,
        verbose=config.verbose,
    )
    result = analyzer.analyze_dxt(
        exclude_bottlenecks=config.analysis.exclude_bottlenecks,
        exclude_characteristics=config.analysis.exclude_characteristics,
        logical_view_types=config.analysis.logical_view_types,
        metrics=config.analysis.metrics,
        threshold=config.analysis.threshold,
        time_granularity=config.analysis.time_granularity,
        trace_path=config.analysis.trace_path,
        view_types=config.analysis.view_types,
    )
    _handle_output(output_config=config.output, result=result)


def handle_dftracer(config: MainConfig):
    analyzer = DFTracerAnalyzer(
        analysis_config=config.analysis,
        checkpoint_config=config.checkpoint,
        cluster_config=config.cluster,
        output_config=config.output,
        debug=config.debug,
        verbose=config.verbose,
    )
    result = analyzer.analyze_pfw(
        exclude_bottlenecks=config.analysis.exclude_bottlenecks,
        exclude_characteristics=config.analysis.exclude_characteristics,
        logical_view_types=config.analysis.logical_view_types,
        metrics=config.analysis.metrics,
        threshold=config.analysis.threshold,
        time_granularity=config.analysis.time_granularity,
        trace_path=config.analysis.trace_path,
        view_types=config.analysis.view_types,
    )
    _handle_output(output_config=config.output, result=result)


def handle_recorder(config: MainConfig):
    analyzer = RecorderAnalyzer(
        analysis_config=config.analysis,
        checkpoint_config=config.checkpoint,
        cluster_config=config.cluster,
        output_config=config.output,
        debug=config.debug,
        verbose=config.verbose,
    )
    result = analyzer.analyze_parquet(
        exclude_bottlenecks=config.analysis.exclude_bottlenecks,
        exclude_characteristics=config.analysis.exclude_characteristics,
        logical_view_types=config.analysis.logical_view_types,
        metrics=config.analysis.metrics,
        threshold=config.analysis.threshold,
        time_granularity=config.analysis.time_granularity,
        trace_path=config.analysis.trace_path,
        view_types=config.analysis.view_types,
    )
    _handle_output(output_config=config.output, result=result)


@hydra.main(version_base=None, config_path="configs", config_name="config")
def main(config: MainConfig) -> None:
    if config.analysis.type == AnalysisType.DARSHAN:
        handle_darshan(config=config)
    elif config.analysis.type == AnalysisType.DFTRACER:
        handle_dftracer(config=config)
    elif config.analysis.type == AnalysisType.RECORDER:
        handle_recorder(config=config)
    else:
        raise ValueError(f"Unknown analysis type: {config.analysis.type}")


cs = ConfigStore.instance()
cs.store(name="base_config", node=MainConfig)

if __name__ == '__main__':
    main()
