#!/usr/bin/env bash

if [ ! -n "$1" ] || [ ! -n "$2" ]; then
    echo "You must pass a number of processes and a log directory!"
    echo "For example: ./recorder-to-parquet.sh 8 /p/gpfs1/logs"
    exit 1
fi

CONVERTER_BIN=$iopp/software/recorder-hari-pilgrim/build/bin

rm -r $2/_parquet

mpirun -n $1 $CONVERTER_BIN/recorder2parquet-workflow-vani $2

# /p/gpfs1/iopp/recorder_app_logs/cm1/nodes-32/workflow-4
# /p/gpfs1/iopp/recorder_app_logs/hacc/nodes-32/workflow-0