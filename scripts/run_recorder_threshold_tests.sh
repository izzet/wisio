#!/usr/bin/env bash

if [ ! -n "$1" ]; then
    echo "You must pass the config directory!"
    echo "For example: ./run_recorder_threshold_tests.sh ./config_samples/"
    exit 1
fi

config_helper="$(dirname "$0")/../config_samples/config_helper.py"
config_dir="$1"
log_file="run_recorder_threshold_log.txt"

# Use find to locate files that match the specified pattern
thresholds=("0" "0.001" "0.01" "0.1" "0.25" "0.5" "0.75" "0.9")
config_files=$(find "$config_dir" -type f -name "recorder_*_lsf.yml")

# Iterate over the found files and execute them
for config_file in $config_files; do
    echo "Running script for file: $config_file"

    echo "$config_file" >>"$log_file"

    for threshold in "${thresholds[@]}"; do
        echo "Running threshold $threshold with config file: $config_file"

        python $config_helper set $config_file checkpoint true
        python $config_helper set $config_file debug false
        python $config_helper set $config_file cluster.cluster_type lsf
        python $config_helper set $config_file cluster.memory 1600
        python $config_helper set $config_file cluster.n_workers 8
        python $config_helper set $config_file cluster.n_threads_per_worker 16
        python $config_helper set $config_file metric_threshold $threshold
        python $config_helper set $config_file output.type csv
        # python $config_helper set $config_file slope_threshold 90
        python $config_helper set $config_file verbose false

        wisio recorder analyze -c "$config_file" >>"$log_file" 2>&1
    done

    echo "Completed script for file: $config_file"
done

echo "Script completed successfully!"
