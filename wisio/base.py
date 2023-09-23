import abc
from typing import List
from .types import Metric
from .utils.file_utils import ensure_dir
from .utils.logger import create_logger, format_log


class Analyzer(abc.ABC):

    def __init__(
        self,
        name: str,
        working_dir: str,
        cluster_mgr_args: dict = None,
        debug=False
    ):
        self.cluster_mgr_args = cluster_mgr_args
        self.debug = debug
        self.name = name

        # Create logger
        ensure_dir(working_dir)
        self.logger = create_logger(__name__, f"{working_dir}/{name}.log")
        self.logger.info(format_log('main', f"Initializing {name} analyzer"))

    @abc.abstractmethod
    def analyze_parquet(
        self,
        log_dir: str,
        metrics: List[Metric],
        view_names: List[str],
        slope_threshold=45,
        checkpoint=False,
        checkpoint_dir='',
        persist=True,
    ):
        raise NotImplementedError
