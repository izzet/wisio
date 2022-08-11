from dask.dataframe import DataFrame, Series
from numpy import ndarray
from typing import Any, Dict, List, Tuple

_BinInfo = Tuple[ndarray, float]


class _DescribesObservation(object):

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        raise NotImplementedError


class _Filter(_DescribesObservation):

    def apply(self, ddf: DataFrame) -> Any:
        raise NotImplementedError

    def detect_bottlenecks(self, results: Series, threshold=False, inversed=False) -> Any:
        raise NotImplementedError

    def format_value(self, value: float) -> str:
        raise NotImplementedError

    def is_inversed(self) -> bool:
        raise NotImplementedError

    def is_normally_distributed(self) -> bool:
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def prepare(self, ddf: DataFrame) -> Any:
        raise NotImplementedError

    def unit(self) -> str:
        raise NotImplementedError


class _BinNode(_DescribesObservation):

    def analyze(self) -> Any:
        raise NotImplementedError


class _FilterGroup(_DescribesObservation):

    def binned_by(self) -> str:
        raise NotImplementedError

    def calculate_bins(self, start: Any, stop: Any) -> _BinInfo:
        raise NotImplementedError

    def compute_stats(self, ddf: DataFrame, all_ddf: DataFrame) -> Dict:
        raise NotImplementedError

    def create_node(self, ddf: DataFrame, bin: Tuple[float, float], filter: _Filter, parent=None) -> _BinNode:
        raise NotImplementedError

    def create_root(self, ddf: DataFrame, filter: _Filter, parent=None) -> _BinNode:
        raise NotImplementedError

    def filters(self) -> List[_Filter]:
        raise NotImplementedError

    def metrics_of(self, filter: _Filter) -> List[_Filter]:
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def prepare(self, ddf: DataFrame, all_ddf: DataFrame, persist_stats=True, debug=False) -> None:
        raise NotImplementedError

    def set_bins(self, ddf: DataFrame, bins: ndarray):
        raise NotImplementedError
