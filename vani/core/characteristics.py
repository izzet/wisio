import copy
import numpy as np
import os
from dask import delayed
from dask.dataframe import DataFrame
from typing import Dict

from vani.common.constants import HDF5_CALLS, MPI_CALLS, POSIX_CALLS, MPI_COLL_CALLS

IO_CAT_MAP = [('read', 1), ('write', 2), ('metadata', 3)]
XFER_SIZE_BINS = [
    -np.inf,
    4 * 1024.0,
    16 * 1024.0,
    64 * 1024.0,
    256 * 1024.0,
    1 * 1024.0 * 1024.0,
    4 * 1024.0 * 1024.0,
    16 * 1024.0 * 1024.0,
    64 * 1024.0 * 1024.0,
    np.inf
]
XFER_SIZE_BIN_NAMES = [
    '<4KB',
    '4KB',
    '16KB',
    '64KB',
    '256KB',
    '1MB',
    '4MB',
    '16MB',
    '64MB',
    '>64MB'
]


@delayed
def low_level_char_delayed(ddf: DataFrame, metric: Dict, agg: Dict):
    # Return metric back if dataframe is empty
    if ddf.empty:
        return metric

    # Copy metric
    metric = copy.deepcopy(metric)

    # Calculate aggregated values
    agg_values = ddf.groupby(['io_cat']).agg(agg)
    print(agg_values)

    # Remove dataframe reference
    del ddf

    # Get I/O categories
    io_cats = agg_values.index.unique()

    # Extend metrics with characteristics
    for col, func in agg.items():
        col_name = get_col_name(func, col)
        for io_cat_name, io_cat in IO_CAT_MAP:
            if func == 'count':
                metric[io_cat_name][col_name] = 0
            elif func == 'unique':
                metric[io_cat_name][col_name] = []
                metric[io_cat_name][f"total_{col_name}"] = 0
            if io_cat in io_cats:
                metric[io_cat_name][col_name] = agg_values.loc[io_cat][col]
                if func == 'unique':
                    values = metric[io_cat_name][col_name]
                    if 'filename' in col_name:
                        # 1- Filter empty paths
                        # 2- Normalize paths
                        # 3- Keep unique ones
                        values = list(set(map(os.path.normpath, filter(None, values))))
                    values = np.array(values)
                    if values.dtype == 'float':
                        values = values.astype(int)
                    metric[io_cat_name][col_name] = list(sorted(values))
                    metric[io_cat_name][f"total_{col_name}"] = len(values)

    # Compute derived characteristics
    for col, func in agg.items():
        col_name = get_col_name(func, col)
        if func == 'count':
            total = 0
            for io_cat_name, _ in IO_CAT_MAP:
                total = total + metric[io_cat_name][col_name]
            metric['all'][col_name] = total
        elif func == 'unique':
            values = []
            for io_cat_name, _ in IO_CAT_MAP:
                values = np.union1d(values, metric[io_cat_name][col_name])
            if values.dtype == 'float':
                values = values.astype(int)
            metric['all'][col_name] = list(sorted(values))
            metric['all'][f"total_{col_name}"] = len(values)

    # Compute additional characteristics
    for io_cat_name, io_cat in IO_CAT_MAP:
        # Xfer size
        if 'agg_size' in metric[io_cat_name] and 'total_index' in metric[io_cat_name]:
            metric[io_cat_name]['xfer_size'] = 0
            metric[io_cat_name]['xfer_size_fmt'] = XFER_SIZE_BIN_NAMES[0]
            if metric[io_cat_name]['total_index'] > 0:
                xfer_size = metric[io_cat_name]['agg_size'] / metric[io_cat_name]['total_index']
                xfer_size_bin = np.digitize(xfer_size, XFER_SIZE_BINS, right=True)
                metric[io_cat_name]['xfer_size'] = xfer_size
                metric[io_cat_name]['xfer_size_fmt'] = XFER_SIZE_BIN_NAMES[xfer_size_bin]

    if 'total_unique_filename' in metric['all'] and 'total_unique_rank' in metric['all']:
        # noinspection PyChainedComparisons
        metric['is_fpp'] = all(
            metric[io_cat_name]['total_unique_filename'] == metric[io_cat_name]['total_unique_rank']
            for io_cat_name in ['read', 'write']
        )

    if 'unique_func_id' in metric['all']:
        # https://github.com/uiuc-hpc/Recorder/blob/caad9c8ec19a39a3cc7ce2a308afbeee8a8e91a4/lib/recorder-hdf5.c#L721
        metric['is_collective'] = any('_coll_' in f or f in MPI_COLL_CALLS for f in metric['all']['unique_func_id'])
        metric['is_sequential'] = not any('seek' in f for f in metric['all']['unique_func_id'])
        for col, func_list in zip(['is_hdf5', 'is_mpi', 'is_posix'], [HDF5_CALLS, MPI_CALLS, POSIX_CALLS]):
            metric[col] = any(f in func_list for f in metric['all']['unique_func_id'])

    metric['md_io_ratio'] = 0.0
    if 'agg_dur' in metric['all'] and metric['all']['agg_dur'] > 0:
        metric['md_io_ratio'] = metric['metadata']['agg_dur'] / metric['all']['agg_dur']

    metric['is_read_only'] = metric['read']['agg_dur'] > 0 and metric['write']['agg_dur'] == 0
    metric['is_write_only'] = metric['write']['agg_dur'] > 0 and metric['read']['agg_dur'] == 0
    metric['is_md_only'] = metric['md_io_ratio'] == 1

    return metric


def get_col_name(func: str, col: str):
    if func == 'count':
        return f"total_{col}"
    return f"{func}_{col}"
