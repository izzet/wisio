from dask.dataframe import DataFrame, Series
from numpy import ndarray
from typing import Any, List, Tuple

_BinInfo = Tuple[ndarray, Any]


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

    def create_node(self, ddf: DataFrame, bin: Tuple[float, float], filter: _Filter, label: str, parent=None, children=None) -> _Node:
        raise NotImplementedError

    def filters(self) -> List[_Filter]:
        raise NotImplementedError

    def is_bin_threshold_exceeded(self, bin_step: Any) -> bool:
        raise NotImplementedError

    def metrics_of(self, filter: _Filter) -> List[_Filter]:
        raise NotImplementedError

    def set_bins(self, ddf: DataFrame, start: Any, stop: Any) -> _BinInfo:
        raise NotImplementedError
