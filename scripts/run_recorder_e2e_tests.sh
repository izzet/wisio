#!/usr/bin/env bash

if [ ! -n "$1" ]; then
    echo "You must pass the config directory!"
    echo "For example: ./run_recorder_e2e_tests.sh ./config_samples/"
    exit 1
fi

config_helper="$(dirname "$0")/../config_samples/config_helper.py"
config_dir="$1"

# Use find to locate files that match the specified pattern
config_files=$(find "$config_dir" -type f -name "recorder_genome_*.yml")

# Iterate over the found files and execute them
for config_file in $config_files; do
    log_file="${config_file%.yml}_log.txt"
    echo "Running script for file: $config_file"

    python $config_helper set $config_file checkpoint true
    python $config_helper set $config_file debug false
    python $config_helper set $config_file cluster.cluster_type lsf
    python $config_helper set $config_file cluster.memory 1600
    python $config_helper set $config_file cluster.n_workers 8
    python $config_helper set $config_file cluster.n_threads_per_worker 16
    python $config_helper set $config_file logical_view_types false
    python $config_helper set $config_file metric_threshold 0.1
    python $config_helper set $config_file output.max_bottlenecks_per_view_type 0
    python $config_helper set $config_file output.show_debug true
    python $config_helper set $config_file output.type console
    python $config_helper set $config_file slope_threshold 45
    python $config_helper set $config_file verbose false

    wisio recorder analyze -c "$config_file" >"$log_file" 2>&1

    echo "Completed script for file: $config_file"
done

echo "Script completed successfully!"
