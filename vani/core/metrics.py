import numpy as np
import pandas as pd
from dask import delayed
from dask.dataframe import DataFrame

@delayed
def filter(ddf: DataFrame, fg_index: str, start: int, stop: int):
    empty = {
        'uniq_ranks': [],
        'agg_dur': 0.0,
        'total_io_size': 0,
        'uniq_filenames': [],
        'bw_sum': 0.0,
        'ops': 0,
    }
    if ddf.empty:
        return {
            'start': start,
            'stop': stop,
            'read': empty,
            'write': empty,
            'metadata': empty
        }

    def f(x):
        d = {}
        d['duration'] = x['duration'].sum()
        d['size'] = x['size'].sum()
        d['bandwidth'] = x['bandwidth'].sum()
        d['index'] = x['index'].count()
        d['proc_id'] = x['proc_id'].unique()
        d['file_id'] = x['file_id'].unique()
        return pd.Series(d, index=['duration', 'size', 'bandwidth', 'index', 'proc_id', 'file_id'])

    aggregated_values = ddf.groupby('io_cat').apply(f)

    del ddf

    index_values = aggregated_values.index.unique()
    read_values = empty
    write_values = empty
    metadata_values = empty
    if 1 in index_values:
        read_values = {
            'uniq_ranks': aggregated_values.loc[1]['proc_id'],
            'agg_dur': aggregated_values.loc[1]['duration'],
            'total_io_size': aggregated_values.loc[1]['size'],
            'uniq_filenames': aggregated_values.loc[1]['file_id'],
            'bw_sum': aggregated_values.loc[1]['bandwidth'],
            'ops': aggregated_values.loc[1]['index'],
        }
    if 2 in index_values:
        write_values = {
            'uniq_ranks': aggregated_values.loc[2]['proc_id'],
            'agg_dur': aggregated_values.loc[2]['duration'],
            'total_io_size': aggregated_values.loc[2]['size'],
            'uniq_filenames': aggregated_values.loc[2]['file_id'],
            'bw_sum': aggregated_values.loc[2]['bandwidth'],
            'ops': aggregated_values.loc[2]['index'],
        }
    if 3 in index_values:
        metadata_values = {
            'uniq_ranks': aggregated_values.loc[3]['proc_id'],
            'agg_dur': aggregated_values.loc[3]['duration'],
            'total_io_size': aggregated_values.loc[3]['size'],
            'uniq_filenames': aggregated_values.loc[3]['file_id'],
            'bw_sum': aggregated_values.loc[3]['bandwidth'],
            'ops': aggregated_values.loc[3]['index'],
        }
    filter_result = {
        'start': start,
        'stop': stop,
        'read': read_values,
        'write': write_values,
        'metadata': metadata_values
    }
    # Return results
    return filter_result


@delayed
def merge(x, y):
    return {
        'start': x['start'],
        'stop': x['stop'],
        'read': {
            'uniq_ranks': np.union1d(x['read']['uniq_ranks'], y['read']['uniq_ranks']),
            'agg_dur': x['read']['agg_dur'] + y['read']['agg_dur'],
            'total_io_size': x['read']['total_io_size'] + y['read']['total_io_size'],
            'uniq_filenames': np.union1d(x['read']['uniq_filenames'], y['read']['uniq_filenames']),
            'bw_sum': x['read']['bw_sum'] + y['read']['bw_sum'],
            'ops': x['read']['ops'] + y['read']['ops'],
        },
        'write': {
            'uniq_ranks': np.union1d(x['write']['uniq_ranks'], y['write']['uniq_ranks']),
            'agg_dur': x['write']['agg_dur'] + y['write']['agg_dur'],
            'total_io_size': x['write']['total_io_size'] + y['write']['total_io_size'],
            'uniq_filenames': np.union1d(x['write']['uniq_filenames'], y['write']['uniq_filenames']),
            'bw_sum': x['write']['bw_sum'] + y['write']['bw_sum'],
            'ops': x['write']['ops'] + y['write']['ops'],
        },
        'metadata': {
            'uniq_ranks': np.union1d(x['metadata']['uniq_ranks'], y['metadata']['uniq_ranks']),
            'agg_dur': x['metadata']['agg_dur'] + y['metadata']['agg_dur'],
            'uniq_filenames': np.union1d(x['metadata']['uniq_filenames'], y['metadata']['uniq_filenames']),
            'ops': x['metadata']['ops'] + y['metadata']['ops'],
        }
    }


@delayed
def summary(x):
    return {
        'start': x['start'],
        'stop': x['stop'],
        'read': {
            'uniq_ranks': len(x['read']['uniq_ranks']),
            'agg_dur': x['read']['agg_dur'],
            'total_io_size': x['read']['total_io_size'],
            'uniq_filenames': len(x['read']['uniq_filenames']),
            'bw_sum': x['read']['bw_sum'],
            'ops': x['read']['ops'],
        },
        'write': {
            'uniq_ranks': len(x['write']['uniq_ranks']),
            'agg_dur': x['write']['agg_dur'],
            'total_io_size': x['write']['total_io_size'],
            'uniq_filenames': len(x['write']['uniq_filenames']),
            'bw_sum': x['write']['bw_sum'],
            'ops': x['write']['ops'],
        },
        'metadata': {
            'uniq_ranks': len(x['metadata']['uniq_ranks']),
            'agg_dur': x['metadata']['agg_dur'],
            'uniq_filenames': len(x['metadata']['uniq_filenames']),
            'ops': x['metadata']['ops']
        }
    }
