import abc
import dask.dataframe as dd
from logging import Logger
from typing import Dict
from .base import ViewKey


class BottleneckDetector(abc.ABC):

    def __init__(self, logger: Logger):
        self.logger = logger

    @abc.abstractmethod
    def detect_bottlenecks(
        self,
        views: Dict[ViewKey, dd.DataFrame],
        view_types: list,
        max_io_time: dd.core.Scalar,
        metric='duration',
    ) -> Dict[ViewKey, dd.DataFrame]:
        raise NotImplementedError
