#!/usr/bin/env bash

if [ ! -n "$1" ]; then
    echo "You must pass the config directory!"
    echo "For example: ./run_recorder_scale_tests.sh ./config_samples/"
    exit 1
fi

config_helper="$(dirname "$0")/../config_samples/config_helper.py"
config_dir="$1"
log_file="run_recorder_scale_lsf_log.txt"

# Use find to locate files that match the specified pattern
# scales=("1" "2" "4" "8")
scales=("2" "4" "8" "16" "32")
# scales=("1" "2" "4" "8" "16" "32" "64" "128" "256")
memory=("400" "800" "1600" "3200" "6400")
config_files=$(find "$config_dir" -type f -name "_recorder_genome_*_lsf.yml")

# Iterate over the found files and execute them
for config_file in $config_files; do
    echo "Running script for file: $config_file"

    echo "$config_file" >>"$log_file"

    for i in "${!scales[@]}"; do
        echo "Running scale ${scales[$i]} with config file: $config_file"

        python $config_helper set $config_file checkpoint true
        python $config_helper set $config_file debug false
        python $config_helper set $config_file cluster.cluster_type lsf
        # python $config_helper set $config_file cluster.cluster_type local
        # python $config_helper set $config_file cluster.memory 200
        python $config_helper set $config_file cluster.memory ${memory[$i]}
        # python $config_helper set $config_file cluster.memory 0
        python $config_helper set $config_file cluster.n_workers ${scales[$i]}
        python $config_helper set $config_file cluster.n_threads_per_worker 1
        # python $config_helper set $config_file cluster.n_workers 1
        # python $config_helper set $config_file cluster.n_threads_per_worker ${scales[$i]}
        python $config_helper set $config_file cluster.processes true
        python $config_helper set $config_file logical_view_types false
        python $config_helper set $config_file metric_threshold 0.1
        python $config_helper set $config_file output.run_db_path .wisio/scaling.db
        python $config_helper set $config_file output.type sqlite
        python $config_helper set $config_file slope_threshold 45
        python $config_helper set $config_file verbose false

        wisio recorder analyze -c "$config_file" >>"$log_file" 2>&1
    done

    echo "Completed script for file: $config_file"
done

echo "Script completed successfully!"
