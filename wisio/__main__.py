import argparse
import yaml
from dataclasses import dataclass, field
from typing import List
from .cluster_management import ClusterConfig
from .recorder import RecorderAnalyzer
from .types import Metric


@dataclass
class Config:
    trace_path: str
    checkpoint: bool = True
    checkpoint_dir: str = None
    cluster_config: ClusterConfig = None
    debug: bool = False
    metrics: List[Metric] = field(default_factory=list)
    working_dir: str = '.wisio'

    def __post_init__(self):
        if isinstance(self.cluster_config, dict):
            self.cluster_config = ClusterConfig(**self.cluster_config)


def _load_config(config_path: str):
    with open(config_path) as config_file:
        config = yaml.safe_load(config_file)
        return Config(**config)


def handle_config(config_parser, args):
    if not args.config_command:
        config_parser.print_help()
    elif args.config_command == 'create':
        pass


def handle_recorder(recorder_parser, args):

    if not args.recorder_command:
        recorder_parser.print_help()
    elif args.recorder_command == 'analyze':

        config = _load_config(args.config)

        analyzer = RecorderAnalyzer(
            cluster_config=config.cluster_config,
            debug=config.debug,
            working_dir=config.working_dir,
        )

        result = analyzer.analyze_parquet(
            trace_path=config.trace_path,
            checkpoint=config.checkpoint,
            checkpoint_dir=config.checkpoint_dir,
            metrics=config.metrics,
        )

        print(len(result.main_view))


def ask_questions():
    question1 = input("Enter your answer to question 1: ")
    question2 = input("Enter your answer to question 2: ")

    # Process the answers (you can replace this with your logic)
    result = f"Your answers were: Question 1 - {question1}, Question 2 - {question2}"

    return result


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
    elif args.command == 'recorder':
        handle_recorder(recorder_parser, args)
    else:
        print('Invalid command')


if __name__ == '__main__':
    main()
