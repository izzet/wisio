import abc
import dask.dataframe as dd
import itertools as it
import logging
import os
from datetime import datetime
from typing import Callable, List
from .cluster_management import ClusterConfig, ClusterManager
from .types import AnalysisAccuracy, Metric, ViewType
from .utils.file_utils import ensure_dir
from .utils.logger import setup_logging


class Analyzer(abc.ABC):

    def __init__(
        self,
        name: str,
        working_dir: str,
        checkpoint: bool = False,
        checkpoint_dir: str = '',
        cluster_config: ClusterConfig = None,
        debug=False
    ):
        if checkpoint:
            assert checkpoint_dir != '', 'Checkpoint directory must be defined'

        self.checkpoint = checkpoint
        self.checkpoint_dir = checkpoint_dir
        self.cluster_config = cluster_config
        self.debug = debug
        self.name = name

        # Setup logging
        ensure_dir(working_dir)
        setup_logging(
            filename=f"{working_dir}/{name.lower()}_analyzer.log", debug=debug)

        logging.info(f"Initializing {name} analyzer")

        # Init cluster manager
        self.cluster_manager = ClusterManager(
            working_dir=working_dir, config=cluster_config)

        # Boot cluster
        self.cluster_manager.boot()

    @abc.abstractmethod
    def analyze_parquet(
        self,
        trace_path: str,
        metrics: List[Metric],
        accuracy: AnalysisAccuracy = 'pessimistic',
        slope_threshold: int = 45,
        view_names: List[str] = [],
    ):
        raise NotImplementedError

    def load_view(self, view_name: str, fallback: Callable[[], dd.DataFrame], force=False):
        print(f"loading view {view_name}", datetime.now())
        view_path = f"{self.checkpoint_dir}/{view_name}"
        if force or not os.path.exists(f"{view_path}/_metadata"):
            print(f"calling fallback for {view_name}", datetime.now())
            view = fallback()
            # print(f"waiting view {view_name}", datetime.now())
            # wait(view)
            print(
                f"storing view {view_name} ({view.npartitions})", datetime.now())
            view.to_parquet(f"{view_path}", compute=True,
                            write_metadata_file=True)
            print(f"killing {view_name} memory")
            self.cluster_manager.client.cancel(view)
        print(f"loading stored {view_name}", datetime.now())
        return dd.read_parquet(f"{view_path}")

    def view_permutations(self, view_types: List[ViewType]):
        def _iter_permutations(r: int):
            return it.permutations(view_types, r + 1)
        return it.chain.from_iterable(map(_iter_permutations, range(len(view_types))))
