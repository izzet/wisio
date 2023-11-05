import abc
import dask.dataframe as dd
from typing import Dict
from .types import ViewKey


class BottleneckDetector(abc.ABC):

    @abc.abstractmethod
    def detect_bottlenecks(
        self,
        views: Dict[ViewKey, dd.DataFrame],
        view_types: list,
        max_io_time: dd.core.Scalar,
        metric='duration',
    ) -> Dict[ViewKey, dd.DataFrame]:
        raise NotImplementedError
