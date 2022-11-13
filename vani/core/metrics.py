import numpy as np
import pandas as pd
import portion as P
from dask import delayed
from dask.dataframe import DataFrame


@delayed
def filter_delayed(ddf: DataFrame, fg_index: str, start: int, stop: int):
    # empty = {
    #     'uniq_ranks': [],
    #     'agg_dur': 0.0,
    #     'total_io_size': 0,
    #     'uniq_filenames': [],
    #     'bw_sum': 0.0,
    #     'ops': 0,
    # }
    empty = {
        'uniq_ranks': [],
        'uniq_ranks_interval': P.openclosed(0, 0),
        'agg_dur': 0.0,
        'total_io_size': 0,
        'uniq_filenames': [],
        'uniq_filenames_interval': P.openclosed(0, 0),
        'bw_sum': 0.0,
        'ops': 0,
        'time_interval': P.openclosed(0, 0),
    }
    if ddf.empty:
        return {
            'start': start,
            'stop': stop,
            'read': empty,
            'write': empty,
            'metadata': empty
        }

    def g(x):

        d = {}
        d['duration'] = x['duration'].sum()
        d['size'] = x['size'].sum()
        d['bandwidth'] = x['bandwidth'].sum()
        d['index'] = x['index'].count()

        return pd.Series(d, index=['duration', 'size', 'bandwidth', 'index'])

    def f(x):

        proc_df = x.groupby('proc_id').apply(g)
        proc_df_desc = proc_df.describe()
        # print('duration', proc_df_desc.loc['max']['duration'], x['duration'].sum(), proc_df_desc.loc['count']['index'])

        d = {}
        d['duration'] = proc_df_desc.loc['max']['duration']
        # d['duration'] = x['duration'].sum()
        d['size'] = x['size'].sum()
        # d['size'] = proc_df_desc.loc['max']['size']
        # d['bandwidth'] = x['bandwidth'].sum()
        d['bandwidth'] = proc_df_desc.loc['max']['bandwidth']
        d['index'] = x['index'].count()

        # TODO
        d['proc_id'] = x['proc_id'].unique()
        d['proc_id_interval'] = P.closed(x['proc_id'].min(), x['proc_id'].max())
        d['file_id'] = x['file_id'].unique()
        d['file_id_interval'] = P.closed(x['file_id'].min(), x['file_id'].max())
        d['time_interval'] = P.closed(round(x['tstart'].min(), 2), round(x['tend'].max(), 2))
        # TODO

        # High-level filter without uniques to understand focus areas
        # Low-level filter to explore details
        # apply to aggregate (10x faster)

        return pd.Series(d, index=['duration', 'size', 'bandwidth', 'index', 'proc_id', 'proc_id_interval', 'file_id',
                                   'file_id_interval', 'time_interval'])

    aggregated_values = ddf.groupby('io_cat').apply(f)
    # aggregated_values = ddf.groupby(['io_cat', 'proc_id']).apply(f)

    del ddf

    index_values = aggregated_values.index.unique()
    read_values = empty
    write_values = empty
    metadata_values = empty
    if 1 in index_values:
        read_values = {
            'uniq_ranks': aggregated_values.loc[1]['proc_id'],
            'uniq_ranks_interval': aggregated_values.loc[1]['proc_id_interval'],
            'agg_dur': aggregated_values.loc[1]['duration'],
            'total_io_size': aggregated_values.loc[1]['size'],
            'uniq_filenames': aggregated_values.loc[1]['file_id'],
            'uniq_filenames_interval': aggregated_values.loc[1]['file_id_interval'],
            'bw_sum': aggregated_values.loc[1]['bandwidth'],
            'ops': aggregated_values.loc[1]['index'],
            'time_interval': aggregated_values.loc[1]['time_interval'],
        }
    if 2 in index_values:
        write_values = {
            'uniq_ranks': aggregated_values.loc[2]['proc_id'],
            'uniq_ranks_interval': aggregated_values.loc[2]['proc_id_interval'],
            'agg_dur': aggregated_values.loc[2]['duration'],
            'total_io_size': aggregated_values.loc[2]['size'],
            'uniq_filenames': aggregated_values.loc[2]['file_id'],
            'uniq_filenames_interval': aggregated_values.loc[2]['file_id_interval'],
            'bw_sum': aggregated_values.loc[2]['bandwidth'],
            'ops': aggregated_values.loc[2]['index'],
            'time_interval': aggregated_values.loc[2]['time_interval'],
        }
    if 3 in index_values:
        metadata_values = {
            'uniq_ranks': aggregated_values.loc[3]['proc_id'],
            'uniq_ranks_interval': aggregated_values.loc[3]['proc_id_interval'],
            'agg_dur': aggregated_values.loc[3]['duration'],
            'total_io_size': aggregated_values.loc[3]['size'],
            'uniq_filenames': aggregated_values.loc[3]['file_id'],
            'uniq_filenames_interval': aggregated_values.loc[3]['file_id_interval'],
            'bw_sum': aggregated_values.loc[3]['bandwidth'],
            'ops': aggregated_values.loc[3]['index'],
            'time_interval': aggregated_values.loc[3]['time_interval'],
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
def merge_delayed(x, y):
    return {
        'start': x['start'],
        'stop': y['stop'],
        'read': {
            'uniq_ranks': np.union1d(x['read']['uniq_ranks'], y['read']['uniq_ranks']),
            'uniq_ranks_interval': x['read']['uniq_ranks_interval'].union(y['read']['uniq_ranks_interval']),
            'agg_dur': x['read']['agg_dur'] + y['read']['agg_dur'],
            'total_io_size': x['read']['total_io_size'] + y['read']['total_io_size'],
            'uniq_filenames': np.union1d(x['read']['uniq_filenames'], y['read']['uniq_filenames']),
            'uniq_filenames_interval': x['read']['uniq_filenames_interval'].union(y['read']['uniq_filenames_interval']),
            'bw_sum': x['read']['bw_sum'] + y['read']['bw_sum'],
            'ops': x['read']['ops'] + y['read']['ops'],
            'time_interval': x['read']['time_interval'].union(y['read']['time_interval']),
        },
        'write': {
            'uniq_ranks': np.union1d(x['write']['uniq_ranks'], y['write']['uniq_ranks']),
            'uniq_ranks_interval': x['write']['uniq_ranks_interval'].union(y['write']['uniq_ranks_interval']),
            'agg_dur': x['write']['agg_dur'] + y['write']['agg_dur'],
            'total_io_size': x['write']['total_io_size'] + y['write']['total_io_size'],
            'uniq_filenames': np.union1d(x['write']['uniq_filenames'], y['write']['uniq_filenames']),
            'uniq_filenames_interval': x['write']['uniq_filenames_interval'].union(
                y['write']['uniq_filenames_interval']),
            'bw_sum': x['write']['bw_sum'] + y['write']['bw_sum'],
            'ops': x['write']['ops'] + y['write']['ops'],
            'time_interval': x['write']['time_interval'].union(y['write']['time_interval']),
        },
        'metadata': {
            'uniq_ranks': np.union1d(x['metadata']['uniq_ranks'], y['metadata']['uniq_ranks']),
            'uniq_ranks_interval': x['metadata']['uniq_ranks_interval'].union(y['metadata']['uniq_ranks_interval']),
            'agg_dur': x['metadata']['agg_dur'] + y['metadata']['agg_dur'],
            'uniq_filenames': np.union1d(x['metadata']['uniq_filenames'], y['metadata']['uniq_filenames']),
            'uniq_filenames_interval': x['metadata']['uniq_filenames_interval'].union(
                y['metadata']['uniq_filenames_interval']),
            'ops': x['metadata']['ops'] + y['metadata']['ops'],
            'time_interval': x['metadata']['time_interval'].union(y['metadata']['time_interval']),
        }
    }


@delayed
def flatten_delayed(x):
    return {
        'start': x['start'],
        'stop': x['stop'],
        'read': {
            'uniq_ranks': len(x['read']['uniq_ranks']),
            'uniq_ranks_interval': P.to_string(x['read']['uniq_ranks_interval']),
            'agg_dur': x['read']['agg_dur'],
            'total_io_size': x['read']['total_io_size'],
            'uniq_filenames': len(x['read']['uniq_filenames']),
            'uniq_filenames_interval': P.to_string(x['read']['uniq_filenames_interval']),
            'bw_sum': x['read']['bw_sum'],
            'ops': x['read']['ops'],
            'time_interval': P.to_string(x['read']['time_interval']),
        },
        'write': {
            'uniq_ranks': len(x['write']['uniq_ranks']),
            'uniq_ranks_interval': P.to_string(x['write']['uniq_ranks_interval']),
            'agg_dur': x['write']['agg_dur'],
            'total_io_size': x['write']['total_io_size'],
            'uniq_filenames': len(x['write']['uniq_filenames']),
            'uniq_filenames_interval': P.to_string(x['write']['uniq_filenames_interval']),
            'bw_sum': x['write']['bw_sum'],
            'ops': x['write']['ops'],
            'time_interval': P.to_string(x['write']['time_interval']),
        },
        'metadata': {
            'uniq_ranks': len(x['metadata']['uniq_ranks']),
            'uniq_ranks_interval': P.to_string(x['metadata']['uniq_ranks_interval']),
            'agg_dur': x['metadata']['agg_dur'],
            'uniq_filenames': len(x['metadata']['uniq_filenames']),
            'uniq_filenames_interval': P.to_string(x['metadata']['uniq_filenames_interval']),
            'ops': x['metadata']['ops'],
            'time_interval': P.to_string(x['metadata']['time_interval']),
        }
    }
