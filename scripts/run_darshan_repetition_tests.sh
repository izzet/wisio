#!/usr/bin/env bash

if [ ! -n "$1" ] || [ ! -n "$2" ]; then
    echo "You must pass the config directory and the number of repetitions!"
    echo "For example: ./run_darshan_repetition_tests.sh ./config_samples/ 5"
    exit 1
fi

config_helper="$(dirname "$0")/../config_samples/config_helper.py"
config_dir="$1"
repetitions="$2"
log_file="run_darshan_log.txt"
slope_threshold=${3:-80}

# Use find to locate files that match the specified pattern
config_files=$(find "$config_dir" -type f -name "*.yml")

# Iterate over the found files and execute them
for config_file in $config_files; do
    echo "Running script for file: $config_file"

    echo "$config_file" >>"$log_file"
    for ((i = 1; i <= $repetitions; i++)); do
        echo "Running iteration $i with config file: $config_file"

        # python $config_helper set $config_file checkpoint true
        python $config_helper set $config_file type darshan
        python $config_helper set $config_file debug false
        python $config_helper set $config_file logical_view_types false
        # python $config_helper set $config_file cluster.cluster_type lsf
        # python $config_helper set $config_file cluster.memory 1600
        # python $config_helper set $config_file cluster.n_workers 8
        # python $config_helper set $config_file cluster.n_threads_per_worker 16
        # python $config_helper set $config_file metric_threshold 0.5
        python $config_helper set $config_file output.run_db_path .wisio/dlpv3.db
        python $config_helper set $config_file output.type sqlite
        # python $config_helper set $config_file slope_threshold $slope_threshold
        python $config_helper set $config_file verbose false

        wisio analyze -c "$config_file" >>"$log_file" 2>&1
    done

    echo "Completed script for file: $config_file"
done

echo "Script completed successfully!"
