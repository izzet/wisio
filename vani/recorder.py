import dask.dataframe as dd
import itertools
import json
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from vani._recorder.analysis import compute_hlm, compute_hlm_perm, compute_llc, compute_llc_alt
from vani.base import Analyzer
from vani.dask import ClusterManager

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
            n_clusters=len(self.filter_groups),
            logger=self.logger,
            cluster_keys=self.filter_groups,
            verbose=self.debug,
            **cluster_manager_args
        )

        self.cluster_manager.boot()

    def analyze_parquet(self, log_dir: str, delta=0.0001):
        # Load global min max
        global_min_max = self.load_global_min_max(log_dir=log_dir)

        # Compute hlm
        with ThreadPoolExecutor() as executor:

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

            # Compute llc
            llc_dfs = self._compute_llc_dfs(
                executor=executor,
                log_dir=log_dir,
                global_min_max=global_min_max,
                hlm_perm_dfs=hlm_perm_dfs
            )

            # Compute llc alt
            llc_alt_dfs = self._compute_llc_alt_dfs(
                executor=executor,
                log_dir=log_dir,
                global_min_max=global_min_max,
                hlm_perm_dfs=hlm_perm_dfs
            )

        return llc_dfs, llc_alt_dfs

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
                self.cluster_manager.get_client_instances(),
                itertools.repeat(log_dir),
                itertools.repeat(global_min_max),
                self.filter_groups
            )
        ):
            print('hlm', filter_group, len(hlm_df))
            hlm_dfs[filter_group] = hlm_df
        return hlm_dfs

    def _compute_hlm_perm_dfs(self, executor: ThreadPoolExecutor, log_dir: str, global_min_max: dict, hlm_dfs: dict):
        perm_dfs = {}
        for filter_perm, perm_df in zip(
            itertools.permutations(self.filter_groups, N_PERMUTATIONS),
            executor.map(
                compute_hlm_perm,
                np.repeat(self.cluster_manager.get_client_instances(), N_PERMUTATIONS),
                itertools.repeat(log_dir),
                itertools.repeat(global_min_max),
                itertools.permutations(self.filter_groups, N_PERMUTATIONS),
                np.repeat(list(hlm_dfs.values()), N_PERMUTATIONS)
            )
        ):
            print('perm', filter_perm, len(perm_df))
            perm_dfs[filter_perm] = perm_df
        return perm_dfs

    def _compute_llc_dfs(self, executor: ThreadPoolExecutor, log_dir: str, global_min_max: dict, hlm_perm_dfs: dict):
        llc_dfs = {}
        for filter_perm, hlm_perm_df, llc_df in zip(
            itertools.permutations(self.filter_groups, N_PERMUTATIONS),
            list(hlm_perm_dfs.values()),
            executor.map(
                compute_llc,
                np.repeat(self.cluster_manager.get_client_instances(), N_PERMUTATIONS),
                itertools.repeat(log_dir),
                itertools.repeat(global_min_max),
                itertools.permutations(self.filter_groups, N_PERMUTATIONS),
                list(hlm_perm_dfs.values())
            )
        ):
            print('llc', filter_perm, len(hlm_perm_df), len(llc_df))
            llc_dfs[filter_perm] = llc_df
        return llc_dfs

    def _compute_llc_alt_dfs(self, executor: ThreadPoolExecutor, log_dir: str, global_min_max: dict, hlm_perm_dfs: dict):
        llc_dfs = {}
        for filter_perm, hlm_perm_df, llc_df in zip(
            itertools.permutations(self.filter_groups, N_PERMUTATIONS + 1),
            list(hlm_perm_dfs.values()),
            executor.map(
                compute_llc_alt,
                np.repeat(self.cluster_manager.get_client_instances(), N_PERMUTATIONS),
                itertools.repeat(log_dir),
                itertools.repeat(global_min_max),
                itertools.permutations(self.filter_groups, N_PERMUTATIONS + 1),
                list(hlm_perm_dfs.values())
            )
        ):
            print('llc-alt', filter_perm, len(hlm_perm_df), len(llc_df))
            llc_dfs[filter_perm] = llc_df
        return llc_dfs
