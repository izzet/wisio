import dask.dataframe as dd
from typing import Dict


class BottleneckDetector(object):

    def __init__(
        self,
        logger,
        log_dir: str,
        views: Dict[tuple, dd.DataFrame],
        view_types: list,
    ):
        self.bottleneck_dir = f"{log_dir}/bottlenecks"
        self.logger = logger
        self.views = views
        self.view_types = view_types

    def detect_bottlenecks(self, max_io_time: dd.core.Scalar, cut=0.5) -> Dict[tuple, object]:
        raise NotImplementedError
