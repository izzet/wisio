from vani.recorder import RecorderAnalyzer


def run_recorder_analyzer():

    log_dir = "/p/vast1/iopp/recorder_app_logs/cm1/nodes-32/workflow-4/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/hacc/nodes-32/workflow-0/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/montage/nodes-32/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/genome_pegasus/nodes-32/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/lbann-jag/nodes-32/_parquet"

    recorder_analyzer = RecorderAnalyzer(
        cluster_manager_args=dict(
            force_local=True,
            cluster_settings=dict(
                cores=8,
                dashboard_port=3446,
                local_directory="/var/tmp/dask-recorder",
                log_file="%J.log",
                worker_queue="pdebug"
            )
        ),
        working_dir='.recorder',
        debug=True
    )

    try:
        res = recorder_analyzer.analyze_parquet(log_dir=log_dir)
        print(res)
    finally:

        recorder_analyzer.cluster_manager.shutdown()

    pass


if __name__ == '__main__':
    run_recorder_analyzer()
