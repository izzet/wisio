
from analyzer import Analyzer, ClusterOptions, ClusterType

if __name__ == '__main__':
    # Initialize analyzer
    co = ClusterOptions(cluster_type=ClusterType.LSF)
    vn = Analyzer(n_workers=4, cluster_options=co, debug=True)

    # Analysis configuration
    log_dir = "/p/gpfs1/iopp/parquet_app_logs/hacc/nodes-32/workflow-0"
    # log_dir = "/p/gpfs1/iopp/parquet_app_logs/cm1/nodes-32/workflow-4"
    # log_dir = "/p/gpfs1/iopp/parquet_app_logs/lbann-cosmoflow/nodes-32"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/montage_pegasus/nodes-32/_parquet"
    # log_dir = "/p/gpfs1/iopp/recorder_app_logs/genome_pegasus/nodes-32/_parquet"

    # Do the analysis
    io_df_read_write, job_time = vn.analyze_parquet_logs(log_dir)

    print(job_time)