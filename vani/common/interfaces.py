from dask.dataframe import DataFrame, Series
from numpy import ndarray
from typing import Any, List, Tuple

_BinInfo = Tuple[ndarray, float]


class _Filter(object):

    def apply(self, ddf: DataFrame) -> Any:
        raise NotImplementedError

    def detect_bottlenecks(self, results: Series, threshold=False, inversed=False) -> Any:
        raise NotImplementedError

    def is_inversed(self) -> bool:
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def prepare(self, ddf: DataFrame) -> Any:
        raise NotImplementedError


class _Node(object):

    def analyze(self) -> Any:
        raise NotImplementedError

    def render_tree(self) -> None:
        raise NotImplementedError


class _FilterGroup(object):

    def calculate_bins(self, start: Any, stop: Any) -> _BinInfo:
        raise NotImplementedError

    def create_node(self, ddf: DataFrame, bin: Tuple[float, float], filter: _Filter, parent=None) -> _Node:
        raise NotImplementedError

    def filters(self) -> List[_Filter]:
        raise NotImplementedError

    def metrics_of(self, filter: _Filter) -> List[_Filter]:
        raise NotImplementedError

    def set_bins(self, ddf: DataFrame, bins: ndarray):
        raise NotImplementedError
