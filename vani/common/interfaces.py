from dask.dataframe import DataFrame, Series
from numpy import ndarray
from typing import Any, Dict, List, Tuple

_Bin = Tuple[float, float]
_BinInfo = Tuple[ndarray, float]


class _DescribesObservation(object):

    def observation_desc(self, label: int = None, value: Any = None, score: float = None) -> str:
        raise NotImplementedError


class _Filter(_DescribesObservation):

    def apply(self, ddf: DataFrame) -> Any:
        raise NotImplementedError

    def detect_bottlenecks(self, results: Series, threshold=False, inversed=False) -> Any:
        raise NotImplementedError

    def fix_results(self, results: Series, min_max_bins: Series) -> Series:
        raise NotImplementedError

    def format_value(self, value: Any) -> str:
        raise NotImplementedError

    def is_inversed(self) -> bool:
        raise NotImplementedError

    def is_normally_distributed(self) -> bool:
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def numeric_value(self, value: Any) -> float:
        raise NotImplementedError

    def prepare(self, ddf: DataFrame, bin: _Bin) -> Any:
        raise NotImplementedError

    def unit(self) -> str:
        raise NotImplementedError


class _BinNode(_DescribesObservation):

    def analyze(self) -> Any:
        raise NotImplementedError

    def forward(self, results: Tuple) -> Tuple[Any, float]:
        raise NotImplementedError

    def get_tasks(self) -> Tuple:
        raise NotImplementedError


class _FilterGroup(_DescribesObservation):

    def binned_by(self) -> str:
        raise NotImplementedError

    def calculate_bins(self, global_stats: Dict, n_bins: int) -> _BinInfo:
        raise NotImplementedError

    def compute_stats(self, ddf: DataFrame, global_stats: Dict) -> Dict:
        raise NotImplementedError

    def create_node(self, ddf: DataFrame, bin: _Bin, parent=None) -> _BinNode:
        raise NotImplementedError

    def filters(self) -> List[_Filter]:
        raise NotImplementedError

    def get_bins(self) -> _BinInfo:
        raise NotImplementedError

    def name(self) -> str:
        raise NotImplementedError

    def prepare(self, ddf: DataFrame, global_stats: Dict, persist_stats=True, debug=False) -> None:
        raise NotImplementedError

    def set_bins(self, ddf: DataFrame, bins: ndarray):
        raise NotImplementedError
