#!/bin/sh

# check if all paths exist
raw_data_dir="$1"
raw_data_name="$2"
extracted_dir="$3"

# check if compressed file exists
if [ -f "$compressed_file" ]; then
    if [ ! -d "$extracted_dir/$test_data_name" ]; then
        mkdir -p "$extracted_dir/$test_data_name" &&
            tar -xzf "$test_data_dir/$test_data_name.tar.gz" -C "$extracted_dir/$test_data_name"
    fi
fi
