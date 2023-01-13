import itertools as it
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from pandas import DataFrame
from typing import Dict, Tuple

from ._recorder.analysis import compute_hlm, compute_hlm_perm, compute_llc, compute_stats, compute_unique_filenames, compute_unique_processes
from .base import Analyzer
from .dask import ClusterManager

N_PERMUTATIONS = 2


class RecorderAnalyzer(Analyzer):

    def __init__(
        self,
        working_dir: str,
        cluster_manager_args: dict = None,
        debug=False
    ):
        super().__init__('recorder', working_dir, debug)

        self.filter_groups = ['trange', 'file_id', 'proc_id']

        # Create cluster manager
        self.cluster_manager = ClusterManager(
            working_dir=working_dir,
            n_clusters=len(self.filter_groups) * N_PERMUTATIONS + 1,
            logger=self.logger,
            verbose=self.debug,
            **cluster_manager_args
        )

        self.cluster_manager.boot()

    def analyze_parquet(self, log_dir: str, delta=0.0001):
        # Load global min max
        global_min_max = self.load_global_min_max(log_dir=log_dir)

        # Compute hlm
        with ThreadPoolExecutor(max_workers=self.cluster_manager.n_clusters) as executor:

            # Compute stats
            stats_df_future = executor.submit(
                compute_stats,
                client=self.cluster_manager.clients[0],
                log_dir=log_dir
            )

            # Compute hlm
            hlm_dfs = self._compute_hlm_dfs(
                executor=executor,
                log_dir=log_dir,
                global_min_max=global_min_max
            )

            # Compute permutations
            hlm_perm_dfs = self._compute_hlm_perm_dfs(
                executor=executor,
                log_dir=log_dir,
                global_min_max=global_min_max,
                hlm_dfs=hlm_dfs
            )

            # Wait stats
            stats_df = stats_df_future.result()

            # Compute llc
            llc_dfs = self._compute_llc_dfs(
                executor=executor,
                log_dir=log_dir,
                global_min_max=global_min_max,
                hlm_perm_dfs=hlm_perm_dfs,
                stats_df=stats_df
            )

            # Find all unique file and proc IDs
            unique_filenames, unique_processes = self._compute_unique_names(
                executor=executor,
                log_dir=log_dir,
                llc_dfs=llc_dfs
            )

        return stats_df, hlm_dfs, hlm_perm_dfs, llc_dfs, unique_filenames, unique_processes

    def load_global_min_max(self, log_dir: str) -> dict:
        with open(f"{log_dir}/global.json") as file:
            global_min_max = json.load(file)
        return global_min_max

    def _compute_hlm_dfs(self, executor: ThreadPoolExecutor, log_dir: str, global_min_max: dict):
        hlm_dfs = {}
        for filter_group, hlm_df in zip(
            self.filter_groups,
            executor.map(
                compute_hlm,
                self.cluster_manager.clients[1:],
                it.repeat(log_dir),
                it.repeat(global_min_max),
                self.filter_groups
            )
        ):
            print('hlm', filter_group, len(hlm_df))
            hlm_dfs[filter_group] = hlm_df
        return hlm_dfs

    def _compute_hlm_perm_dfs(self, executor: ThreadPoolExecutor, log_dir: str, global_min_max: dict, hlm_dfs: dict):
        perm_dfs = {}
        for filter_perm, perm_df in zip(
            it.permutations(self.filter_groups, N_PERMUTATIONS),
            executor.map(
                compute_hlm_perm,
                self.cluster_manager.clients[1:],
                it.repeat(log_dir),
                it.repeat(global_min_max),
                it.permutations(self.filter_groups, N_PERMUTATIONS),
                np.repeat(list(hlm_dfs.values()), N_PERMUTATIONS)
            )
        ):
            print('perm', filter_perm, len(perm_df))
            perm_dfs[filter_perm] = perm_df
        return perm_dfs

    def _compute_llc_dfs(self, executor: ThreadPoolExecutor, log_dir: str, global_min_max: dict, hlm_perm_dfs: Dict[Tuple[str, str], DataFrame], stats_df: DataFrame):
        llc_dfs = {}
        for filter_perm, hlm_perm_df, llc_df in zip(
            it.permutations(self.filter_groups, N_PERMUTATIONS + 1),
            list(hlm_perm_dfs.values()),
            executor.map(
                compute_llc,
                self.cluster_manager.clients[1:],
                it.repeat(log_dir),
                it.repeat(global_min_max),
                it.permutations(self.filter_groups, N_PERMUTATIONS + 1),
                list(hlm_perm_dfs.values()),
                it.repeat(stats_df),
            )
        ):
            print('llc', filter_perm, len(hlm_perm_df), len(llc_df))
            llc_dfs[filter_perm] = llc_df
        return llc_dfs

    def _compute_unique_names(self, executor: ThreadPoolExecutor, log_dir: str, llc_dfs: Dict[Tuple[str, str, str], DataFrame]):
        # Find all unique file and proc IDs
        unique_file_ids = list(set(it.chain.from_iterable([llc_ddf.index.unique(level='file_id') for llc_ddf in llc_dfs.values()])))
        unique_proc_ids = list(set(it.chain.from_iterable([llc_ddf.index.unique(level='proc_id') for llc_ddf in llc_dfs.values()])))
        # Compute unique names
        unique_filenames_f = executor.submit(
            compute_unique_filenames,
            client=self.cluster_manager.clients[0],
            log_dir=log_dir,
            unique_file_ids=unique_file_ids
        )
        unique_processes_f = executor.submit(
            compute_unique_processes,
            client=self.cluster_manager.clients[1],
            log_dir=log_dir,
            unique_proc_ids=unique_proc_ids
        )
        # Wait for results
        unique_filenames = unique_filenames_f.result()
        unique_processes = unique_processes_f.result()
        print('unique_filenames', len(unique_filenames))
        print('unique_processes', len(unique_processes))
        # Return results
        return unique_filenames, unique_processes
