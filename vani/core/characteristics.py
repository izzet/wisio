import numpy as np
import pandas as pd
import portion as P
from dask import delayed
from dask.dataframe import DataFrame
from typing import Dict

from vani.common.enums import IOCategory

IO_CAT_MAP = [('read', 1), ('write', 2), ('metadata', 3)]


@delayed
def low_level_char_delayed(ddf: DataFrame, metric: Dict, agg: Dict):  # , labels: list, cols: list):
    # Return metric back if dataframe is empty
    if ddf.empty:
        return metric

    # Calculate aggregated values
    agg_values = ddf.groupby(['io_cat']).agg(agg)

    print(agg_values)

    # Remove dataframe reference
    del ddf

    # Get found I/O categories
    io_cats = agg_values.index.unique()

    # Extend metrics with characteristics
    for col, func in agg.items():
        col_name = f"{func}_{col}"
        for io_cat_name, io_cat in IO_CAT_MAP:
            metric[io_cat_name][col_name] = []
            if io_cat in io_cats:
                metric[io_cat_name][col_name] = agg_values.loc[io_cat][col]

    # Compute derived characteristics
    for col, func in agg.items():
        col_name = f"{func}_{col}"
        if func == 'unique':
            metric['all'][col] = np.union1d(np.union1d(metric['read'][col_name], metric['write'][col_name]),
                                            metric['metadata'][col_name])
            metric['all'][f"num_{col}"] = len(metric['all'][col])

    # metric['file_per_process'] = len(all_files) / len(all_procs)
    metric['md_io_ratio'] = 0.0
    if metric['all']['agg_dur'] > 0:
        metric['md_io_ratio'] = metric['metadata']['agg_dur'] / metric['all']['agg_dur']

    return metric
