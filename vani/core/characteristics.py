import numpy as np
import pandas as pd
import portion as P
from dask import delayed
from dask.dataframe import DataFrame
from typing import Dict

from vani.common.enums import IOCategory


@delayed
def low_level_char_delayed(ddf: DataFrame, metric: Dict, agg: Dict):  # , labels: list, cols: list):
    # Return metric back if dataframe is empty
    if ddf.empty:
        return metric

    # def f(x):

    # d = {}
    # FIXME change rank vs proc based on app or workflow
    # d['procs'] = x['rank'].unique()
    # d['files'] = x['filename'].unique()
    # d['funcs'] = x['func_id'].unique()
    # for i, col in enumerate(cols):
    #     d[labels[i]] = x[col].unique()
    # TODO

    # High-level filter without uniques to understand focus areas
    # Low-level filter to explore details
    # apply to aggregate (10x faster)

    # return pd.Series(d, index=['procs', 'files', 'funcs'])
    # return pd.Series(d, index=labels)

    # agg_unique = dd.Aggregation(
    #     'agg_unique',
    #     lambda s: s.apply(set),
    #     lambda s: s.apply(lambda chunks: set(it.chain.from_iterable(chunks))),
    # )

    # aggregated_values = ddf.groupby(groupby).apply(f)
    # aggregated_values = target_ddf.groupby('io_cat').apply(f)
    # aggregated_values = ddf.groupby(['io_cat', 'proc_id']).apply(f)
    # aggregated_values = ddf.groupby(['io_cat']).agg({
    # 'filename': pd.Series.unique,
    # 'func_id': pd.Series.unique,
    # 'rank': pd.Series.unique,
    #     'filename': 'unique',
    #     'func_id': 'unique',
    #     'rank': 'unique',
    # })

    aggregated_values = ddf.groupby(['io_cat']).agg(agg)

    print(aggregated_values)

    del ddf

    io_cats = aggregated_values.index.unique()

    for col, func in agg.items():
        col_name = f"{func}_{col}"
        metric['read'][col_name] = []
        metric['write'][col_name] = []
        metric['metadata'][col_name] = []
        if 1 in io_cats:
            metric['read'][col_name] = aggregated_values.loc[1][col]
        if 2 in io_cats:
            metric['write'][col_name] = aggregated_values.loc[2][col]
        if 3 in io_cats:
            metric['metadata'][col_name] = aggregated_values.loc[3][col]

    # if IOCategory.READ in index_values:
    #     metric['read']['files'] = aggregated_values.loc[1]['files']
    #     metric['read']['funcs'] = aggregated_values.loc[1]['funcs']
    #     metric['read']['procs'] = aggregated_values.loc[1]['procs']
    # else:
    #     metric['read']['files'] = []
    #     metric['read']['funcs'] = []
    #     metric['read']['procs'] = []
    # if IOCategory.WRITE in index_values:
    #     metric['write']['files'] = aggregated_values.loc[2]['files']
    #     metric['write']['funcs'] = aggregated_values.loc[2]['funcs']
    #     metric['write']['procs'] = aggregated_values.loc[2]['procs']
    # else:
    #     metric['write']['files'] = []
    #     metric['write']['funcs'] = []
    #     metric['write']['procs'] = []
    # if IOCategory.METADATA in index_values:
    #     metric['metadata']['files'] = aggregated_values.loc[3]['files']
    #     metric['metadata']['funcs'] = aggregated_values.loc[3]['funcs']
    #     metric['metadata']['procs'] = aggregated_values.loc[3]['procs']
    # else:
    #     metric['metadata']['files'] = []
    #     metric['metadata']['funcs'] = []
    #     metric['metadata']['procs'] = []

    for col, func in agg.items():
        col_name = f"{func}_{col}"
        if func == 'unique':
            metric['all'][col] = np.union1d(np.union1d(metric['read'][col_name], metric['write'][col_name]),
                                            metric['metadata'][col_name])
            metric['all'][f"num_{col}"] = len(metric['all'][col])

    # all_files = np.union1d(np.union1d(metric['read']['files'], metric['write']['files']), metric['metadata']['files'])
    # all_funcs = np.union1d(np.union1d(metric['read']['funcs'], metric['write']['funcs']), metric['metadata']['funcs'])
    # all_procs = np.union1d(np.union1d(metric['read']['procs'], metric['write']['procs']), metric['metadata']['procs'])
    #
    # metric['all']['files'] = all_files
    # metric['all']['funcs'] = all_funcs
    # metric['all']['procs'] = all_procs.astype(int)
    #
    # metric['all']['num_files'] = len(all_files)
    # metric['all']['num_funcs'] = len(all_funcs)
    # metric['all']['num_procs'] = len(all_procs)
    #
    # metric['file_per_process'] = len(all_files) / len(all_procs)
    metric['md_io_ratio'] = 0 if metric['all']['agg_dur'] == 0 else metric['metadata']['agg_dur'] / metric['all'][
        'agg_dur']

    return metric
