import argparse
import asyncio
import dask
import json
import os
import sys
sys.path.append('..')
from time import sleep
from vani.core.dask_mgmt import DaskManager, DEFAULT_N_WORKERS_PER_NODE
from vani.utils.logger import create_logger, format_log

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("action", help="[start, stop, healthcheck]")
    parser.add_argument("--dir", help="working directory", default=".digio")
    args = parser.parse_args()
    # Create logger
    dir_path = os.path.dirname(os.path.realpath(__file__))
    logger = create_logger(__name__, f"{dir_path}/{args.dir}/daemon.log")
    logger.info(format_log("main", "Initializing analyzer"))
    print(args.action)
    if args.action == "start":
        # Boot Dask clusters & clients
        cluster_settings = dict(
            dashboard_port=3486,
            local_directory="/var/tmp/dask3",
            log_file="%J.log"
        )
        fg_indices = ['tmid', 'proc_id']
        n_workers_per_node = cluster_settings.get('cores', DEFAULT_N_WORKERS_PER_NODE)
        dask_mgr = DaskManager(working_dir=args.dir, fg_indices=fg_indices,
                               logger=logger, debug=True)
        dask_mgr.boot(cluster_settings=cluster_settings, n_workers_per_node=n_workers_per_node)
        client_urls = {}
        for fg_index in dask_mgr.clients:
            client_urls[fg_index] = dask_mgr.clients[fg_index].scheduler_info()['address']
        with open(f"{dir_path}/{args.dir}/clients.json", "w+") as file:
            json.dump(client_urls, file)
        while True:
            for fg_index in fg_indices:
                asyncio.run(dask_mgr.keep_workers_alive(fg_index=fg_index, n_workers=n_workers_per_node))
            sleep(15)
