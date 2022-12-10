import copy
import numpy as np
import pandas as pd
from dask import delayed
from dask.dataframe import DataFrame


@delayed
def filter_delayed(ddf: DataFrame, fg_index: str, start: int, stop: int):
    empty = {
        'agg_bw': 0.0,
        'agg_dur': 0.0,
        'agg_size': 0,
    }

    if ddf.empty:
        return {
            'start': start,
            'stop': stop,
            'all': empty,
            'read': empty,
            'write': empty,
            'metadata': empty
        }

    def g(x):

        d = {}
        d['duration'] = x['duration'].sum()

        return pd.Series(d, index=['duration'])  # , 'size']) #, 'bandwidth', 'index'])

    def f(x):

        proc_df = x.groupby(['proc_id']).apply(g)
        proc_df_desc = proc_df.describe()
        # print('duration', proc_df_desc.loc['max']['duration'], x['duration'].sum(), proc_df_desc.loc['count']['index'])

        d = {}
        d['duration'] = proc_df_desc.loc['max']['duration']
        d['size'] = x['size'].sum()

        # High-level filter without uniques to understand focus areas
        # Low-level filter to explore details
        # apply to aggregate (10x faster)

        return pd.Series(d, index=['duration', 'size'])

    aggregated_values = ddf.groupby(['io_cat']).apply(f)

    del ddf

    io_cats = aggregated_values.index.unique()

    read_values = empty
    write_values = empty
    metadata_values = empty

    if 1 in io_cats:
        read_agg_dur = aggregated_values.loc[1]['duration']
        read_agg_size = aggregated_values.loc[1]['size']
        read_values = {
            'agg_bw': 0 if read_agg_dur == 0 else read_agg_size / read_agg_dur,
            'agg_dur': read_agg_dur,
            'agg_size': read_agg_size,
        }
    if 2 in io_cats:
        write_agg_dur = aggregated_values.loc[2]['duration']
        write_agg_size = aggregated_values.loc[2]['size']
        write_values = {
            'agg_bw': 0 if write_agg_dur == 0 else write_agg_size / write_agg_dur,
            'agg_dur': write_agg_dur,
            'agg_size': write_agg_size,
        }
    if 3 in io_cats:
        metadata_values = {
            'agg_dur': aggregated_values.loc[3]['duration'],
        }

    total_agg_dur = read_values['agg_dur'] + write_values['agg_dur'] + metadata_values['agg_dur']
    total_agg_size = read_values['agg_size'] + write_values['agg_size']

    all_values = {
        'agg_bw': 0 if total_agg_dur == 0 else total_agg_size / total_agg_dur,
        'agg_dur': total_agg_dur,
        'agg_size': total_agg_size,
    }

    filter_result = {
        'start': start,
        'stop': stop,
        'all': all_values,
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
        'all': {
            'agg_bw': x['all']['agg_bw'] + y['all']['agg_bw'],
            'agg_dur': x['all']['agg_dur'] + y['all']['agg_dur'],
            'agg_size': x['all']['agg_size'] + y['all']['agg_size'],
        },
        'read': {
            'agg_bw': x['read']['agg_bw'] + y['read']['agg_bw'],
            'agg_dur': x['read']['agg_dur'] + y['read']['agg_dur'],
            'agg_size': x['read']['agg_size'] + y['read']['agg_size'],
        },
        'write': {
            'agg_bw': x['write']['agg_bw'] + y['write']['agg_bw'],
            'agg_dur': x['write']['agg_dur'] + y['write']['agg_dur'],
            'agg_size': x['write']['agg_size'] + y['write']['agg_size'],
        },
        'metadata': {
            'agg_dur': x['metadata']['agg_dur'] + y['metadata']['agg_dur'],
        }
    }


@delayed
def flatten_delayed(x):
    return {
        'start': x['start'],
        'stop': x['stop'],
        'all': {
            'agg_bw': x['all']['agg_bw'],
            'agg_dur': x['all']['agg_dur'],
            'agg_size': x['all']['agg_size'],
        },
        'read': {
            'agg_bw': x['read']['agg_bw'],
            'agg_dur': x['read']['agg_dur'],
            'agg_size': x['read']['agg_size'],
        },
        'write': {
            'agg_bw': x['write']['agg_bw'],
            'agg_dur': x['write']['agg_dur'],
            'agg_size': x['write']['agg_size'],
        },
        'metadata': {
            'agg_dur': x['metadata']['agg_dur'],
        }
    }


@delayed
def sort_delayed(metrics: list, by_metric: str, reverse=True):
    # TODO make it parallel
    return sorted(metrics, key=lambda x: x['all'][by_metric], reverse=reverse)


@delayed
def filter_asymptote_delayed(sorted_metrics: list, by_metric: str, threshold=0.2, window=5):
    # Calculate total
    total = 0.0
    for metric in sorted_metrics:
        total = total + float(metric['all'][by_metric])
    percentages = []
    selected_metrics = []
    total_percentage = 0
    is_threshold_exceeded = False
    for metric in sorted_metrics:
        # Copy metric
        metric = copy.deepcopy(metric)
        value = float(metric['all'][by_metric])
        # Ignore metrics that do not have any effect (hence 0)
        if value == 0:
            continue
        percentage = value / total
        metric[f"per_{by_metric}"] = percentage
        total_percentage = total_percentage + percentage
        percentages.append(total_percentage)
        if not is_threshold_exceeded:
            selected_metrics.append(metric)
        if len(percentages) >= window:
            window_med = np.median(percentages[-window:])
            window_std = np.std(percentages[-window:]) * 100
            # Check threshold and ignore percentages that do not have any effect (hence 0)
            if 0 < window_med and 0 < window_std < threshold and not is_threshold_exceeded:
                is_threshold_exceeded = True
                print(f'{by_metric}: %t, %, std', total_percentage, percentage, window_std, '<-')
            else:
                print(f'{by_metric}: %t, %, std', total_percentage, percentage, window_std)
        else:
            print(f'{by_metric}: %t, %, std', total_percentage, percentage, -1)
    return selected_metrics
