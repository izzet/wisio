import argparse
import yaml
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, field
from typing import List, Literal

from .analyzer import Analyzer
from .analyzer_result import AnalysisResult
from .cluster_management import ClusterConfig
from .dftracer import DFTracerAnalyzer
from .recorder import RecorderAnalyzer
from .types import Metric, OutputType, ViewType

try:
    from .darshan import DarshanAnalyzer
except ModuleNotFoundError:
    from unittest.mock import Mock

    def raise_error(*args, **kwargs):
        raise RuntimeError("'darshan' module is not found")

    DarshanAnalyzer = Mock(spec=Analyzer, side_effect=raise_error)


AnalyzerType = Literal['darshan', 'dftracer', 'recorder']


@dataclass
class OutputConfig:
    compact: bool = False
    group_behavior: bool = True
    max_bottlenecks: int = 3
    name: str = ''
    root_only: bool = False
    run_db_path: str = None
    show_debug: bool = False
    show_characteristics: bool = True
    show_header: bool = True
    type: OutputType = 'console'
    view_names: List[str] = field(default_factory=list)


@dataclass
class Config:
    type: AnalyzerType
    trace_path: str
    bottleneck_dir: str = None
    checkpoint: bool = True
    checkpoint_dir: str = None
    cluster: ClusterConfig = None
    debug: bool = False
    exclude_bottlenecks: List[str] = field(default_factory=list)
    exclude_characteristics: List[str] = field(default_factory=list)
    logical_view_types: bool = False
    metrics: List[Metric] = field(default_factory=list)
    output: OutputConfig = None
    slope_threshold: int = 45
    time_granularity: int = 1e7  # Recorder's time granularity
    verbose: bool = False
    view_types: List[ViewType] = field(default_factory=list)
    working_dir: str = '.wisio'

    def __post_init__(self):
        if isinstance(self.cluster, dict):
            self.cluster = ClusterConfig(**self.cluster)
        if isinstance(self.output, dict):
            self.output = OutputConfig(**self.output)


def _handle_output(result: AnalysisResult, config: Config):
    output_config = config.output
    if output_config.type == 'console':
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
    elif output_config.type == 'csv':
        result.output.csv(
            max_bottlenecks=output_config.max_bottlenecks,
            name=output_config.name,
            show_debug=output_config.show_debug,
        )
    elif output_config.type == 'sqlite':
        result.output.sqlite(
            name=output_config.name,
            run_db_path=output_config.run_db_path,
        )


def _load_config(config_path: str):
    with open(config_path) as config_file:
        config = yaml.safe_load(config_file)
        return Config(**config)


def handle_bottleneck(bot_parser: ArgumentParser, args: Namespace):
    if not args.bot_command:
        bot_parser.print_help()
    elif args.bot_command == 'inspect':
        pass


def handle_config(config_parser: ArgumentParser, args: Namespace):
    if not args.config_command:
        config_parser.print_help()
    elif args.config_command == 'create':
        pass


def handle_darshan(analyze_parser: ArgumentParser, config: Config):
    analyzer = DarshanAnalyzer(
        bottleneck_dir=config.bottleneck_dir,
        checkpoint=config.checkpoint,
        checkpoint_dir=config.checkpoint_dir,
        cluster_config=config.cluster,
        debug=config.debug,
        verbose=config.verbose,
        working_dir=config.working_dir,
    )
    result = analyzer.analyze_dxt(
        exclude_bottlenecks=config.exclude_bottlenecks,
        exclude_characteristics=config.exclude_characteristics,
        logical_view_types=config.logical_view_types,
        metrics=config.metrics,
        slope_threshold=config.slope_threshold,
        time_granularity=config.time_granularity,
        trace_path_pattern=config.trace_path,
        view_types=config.view_types,
    )
    _handle_output(config=config, result=result)


def handle_dftracer(analyze_parser: ArgumentParser, config: Config):
    analyzer = DFTracerAnalyzer(
        bottleneck_dir=config.bottleneck_dir,
        checkpoint=config.checkpoint,
        checkpoint_dir=config.checkpoint_dir,
        cluster_config=config.cluster,
        debug=config.debug,
        verbose=config.verbose,
        working_dir=config.working_dir,
    )
    result = analyzer.analyze_pfw(
        exclude_bottlenecks=config.exclude_bottlenecks,
        exclude_characteristics=config.exclude_characteristics,
        logical_view_types=config.logical_view_types,
        metrics=config.metrics,
        slope_threshold=config.slope_threshold,
        time_granularity=config.time_granularity,
        trace_path_pattern=config.trace_path,
        view_types=config.view_types,
    )
    _handle_output(config=config, result=result)


def handle_recorder(analyze_parser: ArgumentParser, config: Config):
    analyzer = RecorderAnalyzer(
        bottleneck_dir=config.bottleneck_dir,
        checkpoint=config.checkpoint,
        checkpoint_dir=config.checkpoint_dir,
        cluster_config=config.cluster,
        debug=config.debug,
        verbose=config.verbose,
        working_dir=config.working_dir,
    )
    result = analyzer.analyze_parquet(
        exclude_bottlenecks=config.exclude_bottlenecks,
        exclude_characteristics=config.exclude_characteristics,
        logical_view_types=config.logical_view_types,
        metrics=config.metrics,
        slope_threshold=config.slope_threshold,
        time_granularity=config.time_granularity,
        trace_path=config.trace_path,
        view_types=config.view_types,
    )
    _handle_output(config=config, result=result)


def main():
    parser = argparse.ArgumentParser(description='WisIO')
    subparsers = parser.add_subparsers(title='commands', dest='command')

    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument('-c', '--config', required=True, help='Config path')

    analyze_parser = subparsers.add_parser(
        'analyze', help='Analyze trace data', parents=[base_parser]
    )

    bot_parser = subparsers.add_parser('bottleneck', help='Bottleneck inspection')
    bot_parsers = bot_parser.add_subparsers(title='commands', dest='bot_command')
    bot_inspect_parser = bot_parsers.add_parser(
        'inspect', help='Inspect a bottleneck', parents=[base_parser]
    )
    bot_inspect_parser.add_argument('id', help='Bottleneck ID')

    config_parser = subparsers.add_parser('config', help='Config helper')
    config_parsers = config_parser.add_subparsers(
        title='commands', dest='config_command'
    )
    config_parsers.add_parser('create', help='Create a config file')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
    elif args.command == 'analyze':
        config = _load_config(args.config)
        if config.type == 'darshan':
            handle_darshan(analyze_parser, config)
        elif config.type == 'dftracer':
            handle_dftracer(analyze_parser, config)
        elif config.type == 'recorder':
            handle_recorder(analyze_parser, config)
        else:
            print(f"Invalid analyzer type: {config.type}")
    elif args.command == 'bottleneck':
        handle_bottleneck(bot_parser, args)
    elif args.command == 'config':
        handle_config(config_parser, args)
    else:
        print('Invalid command')


if __name__ == '__main__':
    main()
