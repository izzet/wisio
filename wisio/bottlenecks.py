import dask.dataframe as dd
from logging import Logger
from typing import Dict


class BottleneckDetector(object):

    def __init__(self, logger: Logger, log_dir: str):
        self.bottleneck_dir = f"{log_dir}/bottlenecks"
        self.logger = logger

    def detect_bottlenecks(self, views: Dict[tuple, dd.DataFrame], view_types: list) -> Dict[tuple, object]:
        raise NotImplementedError
