from .utils.file_utils import ensure_dir
from .utils.logger import create_logger, format_log


class Analyzer(object):

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

    def analyze_parquet(self, log_dir: str, delta=0.0001, cut=0.5):
        raise NotImplementedError
