import dask.dataframe as dd
import pandas as pd
from typing import Dict


class BottleneckGenerator(object):

    def __init__(
        self,
        log_dir: str,
        views: Dict[tuple, dd.DataFrame],
        view_types: list,
    ):
        self.bottleneck_dir = f"{log_dir}/bottlenecks"
        self.views = views
        self.view_types = view_types

    def generate_bottlenecks(self, max_io_time: dd.core.Scalar, cut=0.5) -> Dict[tuple, pd.DataFrame]:
        raise NotImplementedError
