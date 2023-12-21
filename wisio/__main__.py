import argparse
import yaml
from dataclasses import dataclass, field
from time import time
from typing import List

from .analyzer_result import AnalysisResult
from .cluster_management import ClusterConfig
from .darshan import DarshanAnalyzer
from .recorder import RecorderAnalyzer
from .types import Metric, OutputType, ViewType


@dataclass
class OutputConfig:
    file_path: str = None
    max_bottlenecks_per_view_type: int = 3
    name: str = None
    show_debug: bool = False
    type: OutputType = 'console'


@dataclass
class Config:
    trace_path: str
    checkpoint: bool = True
    checkpoint_dir: str = None
    cluster: ClusterConfig = None
    debug: bool = False
    metric_threshold: float = 0.5
    metrics: List[Metric] = field(default_factory=list)
    output: OutputConfig = None
    slope_threshold: int = 45
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
            max_bottlenecks_per_view_type=output_config.max_bottlenecks_per_view_type,
            show_debug=output_config.show_debug,
        )
    elif output_config.type == 'csv':
        file_path = output_config.file_path
        if file_path is None:
            file_path = f"{config.working_dir}/{int(time())}.csv"
        result.output.csv(
            file_path=file_path,
            max_bottlenecks_per_view_type=output_config.max_bottlenecks_per_view_type,
            name=output_config.name,
            show_debug=output_config.show_debug,
        )


def _load_config(config_path: str):
    with open(config_path) as config_file:
        config = yaml.safe_load(config_file)
        return Config(**config)


def handle_config(config_parser, args):
    if not args.config_command:
        config_parser.print_help()
    elif args.config_command == 'create':
        pass


def handle_darshan(darshan_parser, args):

    if not args.darshan_command:
        darshan_parser.print_help()
    elif args.darshan_command == 'analyze':

        config = _load_config(args.config)

        analyzer = DarshanAnalyzer(
            checkpoint=config.checkpoint,
            checkpoint_dir=config.checkpoint_dir,
            cluster_config=config.cluster,
            debug=config.debug,
            working_dir=config.working_dir,
        )

        result = analyzer.analyze_dxt(
            metric_threshold=config.metric_threshold,
            metrics=config.metrics,
            slope_threshold=config.slope_threshold,
            trace_path_pattern=config.trace_path,
            view_types=config.view_types,
        )

        _handle_output(config=config, result=result)


def handle_recorder(recorder_parser, args):

    if not args.recorder_command:
        recorder_parser.print_help()
    elif args.recorder_command == 'analyze':

        config = _load_config(args.config)

        analyzer = RecorderAnalyzer(
            checkpoint=config.checkpoint,
            checkpoint_dir=config.checkpoint_dir,
            cluster_config=config.cluster,
            debug=config.debug,
            working_dir=config.working_dir,
        )

        result = analyzer.analyze_parquet(
            metric_threshold=config.metric_threshold,
            metrics=config.metrics,
            slope_threshold=config.slope_threshold,
            trace_path=config.trace_path,
            view_types=config.view_types,
        )

        _handle_output(config=config, result=result)


def main():
    parser = argparse.ArgumentParser(description='WisIO')
    subparsers = parser.add_subparsers(title='commands', dest='command')

    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument(
        '-c', '--config', required=True, help='Config path')

    config_parser = subparsers.add_parser('config', help='Config helper')
    config_subparsers = config_parser.add_subparsers(
        title='commands', dest='config_command')
    config_subparsers.add_parser('create', help='Create a config file')

    darshan_parser = subparsers.add_parser('darshan', help='Darshan analyzer')
    darshan_subparsers = darshan_parser.add_subparsers(
        title='commands', dest='darshan_command')
    darshan_subparsers.add_parser(
        'analyze', help='Analyze Darshan traces', parents=[base_parser])

    recorder_parser = subparsers.add_parser(
        'recorder', help='Recorder analyzer')
    recorder_subparsers = recorder_parser.add_subparsers(
        title='commands', dest='recorder_command')
    recorder_subparsers.add_parser(
        'analyze', help='Analyze Recorder traces', parents=[base_parser])

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
    elif args.command == 'config':
        handle_config(config_parser, args)
    elif args.command == 'darshan':
        handle_darshan(darshan_parser, args)
    elif args.command == 'recorder':
        handle_recorder(recorder_parser, args)
    else:
        print('Invalid command')


if __name__ == '__main__':
    main()
