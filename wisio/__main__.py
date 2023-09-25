import argparse
from wisio.recorder import RecorderAnalyzer


def handle_recorder(recorder_parser, args):

    if not args.command:
        recorder_parser.print_help()
    elif args.command == 'analyze':

        analyzer = RecorderAnalyzer(
            cluster_manager_args=dict(
                force_local=args.use_local_cluster,
                cluster_settings=dict(
                    cores=args.cluster_n_workers,
                    dashboard_port=args.cluster_dashboard_port,
                    local_directory=args.cluster_local_dir,
                    log_file=args.cluster_log_filename,
                    memory=args.cluster_total_memory,
                    worker_queue=args.cluster_worker_queue,
                )
            ),
            debug=args.debug,
            working_dir=args.working_dir,
        )

        result = analyzer.analyze_parquet(
            log_dir=args.trace_dir,
            checkpoint=args.checkpoint,
            checkpoint_dir=args.checkpoint_dir,
            metrics=args.metrics,
        )

        print(len(result.main_view))


def main():
    parser = argparse.ArgumentParser(description='WisIO')
    subparsers = parser.add_subparsers(title='analyzers', dest='analyzer')

    recorder_parser = subparsers.add_parser(
        'recorder', help='Recorder analyzer')
    recorder_subparsers = recorder_parser.add_subparsers(
        title='commands', dest='command')
    recorder_analyze_parser = recorder_subparsers.add_parser(
        'analyze', help='Analyze Recorder traces')
    recorder_analyze_parser.add_argument(
        '-p', '--trace-dir', required=True, help='Trace directory path')
    recorder_analyze_parser.add_argument(
        '-l', '--use-local-cluster', action='store_true', default=False, help='Use a local Dask cluster')
    recorder_analyze_parser.add_argument(
        '-m', '--metrics',
        nargs='+',
        choices=['duration', 'bw', 'iops', 'intensity'],
        default=['duration'],
        help='Specify metrics'
    )
    recorder_analyze_parser.add_argument(
        '-n', '--cluster-n-workers', default=4, help='Number of workers')
    recorder_analyze_parser.add_argument(
        '--cluster-total-memory', help='Total memory')
    recorder_analyze_parser.add_argument(
        '--cluster-dashboard-port', help='Dashboard port')
    recorder_analyze_parser.add_argument(
        '--cluster-local-dir', help='Local directory')
    recorder_analyze_parser.add_argument(
        '--cluster-log-filename', default='%J.log', help='Log filename')
    recorder_analyze_parser.add_argument(
        '--cluster-worker-queue', help='Worker queue')
    recorder_analyze_parser.add_argument(
        '--checkpoint', action='store_true', default=True, help='Enable checkpointing')
    recorder_analyze_parser.add_argument(
        '--checkpoint-dir', help='Checkpointing directory path')
    recorder_analyze_parser.add_argument(
        '--working-dir', default='.recorder', help='Working directory path')
    recorder_analyze_parser.add_argument(
        '--debug', action='store_true', default=False, help='Enable debugging')

    args = parser.parse_args()

    if not args.analyzer:
        parser.print_help()
    elif args.analyzer == 'recorder':
        handle_recorder(recorder_parser, args)
    else:
        print('Invalid command')


if __name__ == '__main__':
    main()
