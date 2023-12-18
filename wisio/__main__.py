import argparse
import yaml
from dataclasses import dataclass, field
from typing import List

from .analyzer_result import AnalysisResult
from .cluster_management import ClusterConfig
from .darshan import DarshanAnalyzer
from .recorder import RecorderAnalyzer
from .types import Metric, OutputType, ViewType


@dataclass
class Config:
    trace_path: str
    checkpoint: bool = True
    checkpoint_dir: str = None
    cluster_config: ClusterConfig = None
    debug: bool = False
    metrics: List[Metric] = field(default_factory=list)
    output_type: OutputType = 'console'
    view_types: List[ViewType] = field(default_factory=list)
    working_dir: str = '.wisio'

    def __post_init__(self):
        if isinstance(self.cluster_config, dict):
            self.cluster_config = ClusterConfig(**self.cluster_config)


def _handle_output(result: AnalysisResult, output_type: OutputType, show_debug: bool):
    if output_type == 'console':
        result.output.console(show_debug=show_debug)


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
            cluster_config=config.cluster_config,
            debug=config.debug,
            output_type=config.output_type,
            working_dir=config.working_dir,
        )

        result = analyzer.analyze_dxt(
            trace_path_pattern=config.trace_path,
            metrics=config.metrics,
            view_types=config.view_types,
        )

        _handle_output(
            output_type=config.output_type,
            result=result,
            show_debug=config.debug,
        )


def handle_recorder(recorder_parser, args):

    if not args.recorder_command:
        recorder_parser.print_help()
    elif args.recorder_command == 'analyze':

        config = _load_config(args.config)

        analyzer = RecorderAnalyzer(
            checkpoint=config.checkpoint,
            checkpoint_dir=config.checkpoint_dir,
            cluster_config=config.cluster_config,
            debug=config.debug,
            output_type=config.output_type,
            working_dir=config.working_dir,
        )

        result = analyzer.analyze_parquet(
            trace_path=config.trace_path,
            metrics=config.metrics,
            view_types=config.view_types,
        )

        _handle_output(
            output_type=config.output_type,
            result=result,
            show_debug=config.debug,
        )


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
