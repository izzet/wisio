from vani.analyzer import Analyzer

if __name__ == '__main__':

    # Initialize analyzer
    cluster_settings = dict(
        cores=64,
        dashboard_port=3446,
        local_directory="/var/tmp/dask2",
        log_file="%J.log"
    )
    dig = Analyzer(debug=True, cluster_settings=cluster_settings)

    # Analysis configuration
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/cm1/nodes-32/workflow-4/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/hacc/nodes-32/workflow-0/_parquet"
    log_dir = "/p/gpfs1/iopp/recorder_app_logs/montage/nodes-32/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/genome_pegasus/nodes-32/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/lbann-jag/nodes-32/_parquet"

    try:
        # Do the analysis
        analysis = dig.analyze_parquet_logs(log_dir, depth=10, persist_stats=True, stats_file_prefix="cm1_")
        # analysis.render_tree()
        # analysis.generate_hypotheses()

    finally:
        dig.shutdown()
