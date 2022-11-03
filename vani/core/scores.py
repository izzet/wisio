import numpy as np
import pandas as pd
from dask import delayed
from scipy import stats
from typing import Dict

IS_INVERSED = dict(
    size=False,
    time=False,
    ops=False,
    files=True,
    bw=True,
    parallelism=True,
    xfer=True
)
IS_NORMAL = dict(
    size=False,
    time=False,
    ops=True,
    files=False,
    bw=False,
    parallelism=True,
    xfer=False
)
MAX_LABEL = 10
OBSERVATIONS = ["size", "time", "ops", "files", "bw", "parallelism", "xfer"]


@delayed
def min_max(metric, metrics):
    # print("++min_max++")
    # print("metrics", metrics)
    # print("metric", metric)
    # if hasattr(metric, "__len__"):
    #     metric = metric[0][0]
    bw = (metric['read']['bw_sum'] + metric['write']['bw_sum']) / 1024.0 / 1024.0
    size = (metric['read']['total_io_size'] + metric['write']['total_io_size']) / 1024.0 / 1024.0 / 1024.0
    uniq_files = max(max(metric['read']['uniq_filenames'], metric['write']['uniq_filenames']),
                     metric['metadata']['uniq_filenames'])
    uniq_ranks = max(max(metric['read']['uniq_ranks'], metric['write']['uniq_ranks']),
                     metric['metadata']['uniq_ranks'])
    return {
        'size': (0, size),
        'time': ((metric['start'] * 1.0 / 1e7) / uniq_ranks, (metric['stop'] * 1.0 / 1e7) / uniq_ranks),
        'ops': (0, metric['read']['ops'] + metric['write']['ops'] + metric['metadata']['ops']),
        'files': (0, uniq_files),
        'bw': (0, bw),
        'parallelism': (0, uniq_ranks),
        'xfer': (0, 16)
    }


def score(metric, min_max_values: Dict):
    # print("--score--")
    # print(min_max_values)
    # print(type(min_max_values))
    # print(metric)
    # print(type(metric))
    if isinstance(metric, list):
        return score_internal(metric[0], min_max_values)

    return score_internal(metric, min_max_values)


def score_internal(metric, min_max_values: Dict):
    all_scores = {
        'start': metric['start'],
        'stop': metric['stop'],
    }
    for cat in ['read', 'write', 'metadata']:
        result = metric[cat]
        label_values = []
        scores = {}
        for observation in OBSERVATIONS:
            unit = ""
            value = 0.0
            if observation == 'size' and 'total_io_size' in result:
                unit = "GB"
                value = result['total_io_size'] / 1024.0 / 1024.0 / 1024.0
            elif observation == 'time' and 'agg_dur' in result:
                value = result['agg_dur'] / result['uniq_ranks'] if result['uniq_ranks'] > 0 else 0
            elif observation == 'ops' and 'ops' in result:
                value = result['ops']
            elif observation == 'bw' and 'bw_sum' in result:
                unit = "MB/s"
                value = result['bw_sum'] / 1024.0 / 1024.0
                # total_io_size = result['total_io_size'] / 1024.0 / 1024.0 / 1024.0
                # io_time_per_rank = result['agg_dur'] / result['uniq_ranks'] if result['uniq_ranks'] > 0 else 0
                # value = total_io_size / io_time_per_rank
            elif observation == 'parallelism' and 'uniq_ranks' in result:
                value = result['uniq_ranks']
            elif observation == 'files' and 'uniq_filenames' in result:
                value = result['uniq_filenames']
            elif observation == 'xfer' and 'total_io_size' in result:
                unit = "MB"
                value = result['total_io_size'] / 1024.0 / 1024.0 / result['ops'] if result['ops'] > 0 else 0
            else:
                continue
            min_value, max_value = min_max_values[observation]
            min_max_indices = [np.finfo(float).min, np.finfo(float).max]
            indices = np.insert(min_max_indices, 1, [value])
            values = [min_value, value, max_value]
            if IS_NORMAL[observation]:
                values = [min_value, value, max_value / 2]
            labels = [label for label in range(1, MAX_LABEL + 1)]
            if IS_INVERSED[observation]:
                labels = list(reversed(labels))
                indices = list(reversed(indices))
                values = list(reversed(values))
            results = pd.Series(values, index=indices).sort_values(ascending=False).astype(float)
            if IS_NORMAL[observation]:
                normalized_results = stats.norm.pdf(results, loc=max_value / 2, scale=np.std(results))
                results = pd.Series(normalized_results, index=results.index)
            labeled_results = pd.cut(results, bins=MAX_LABEL, labels=labels)
            for min_max_index in min_max_indices:
                del labeled_results[min_max_index]
            label = labeled_results[labeled_results.index.values[0]]
            label_values.append(float(label))
            scores[observation] = {
                'label': labeled_results[labeled_results.index.values[0]],
                'value': value,
                'unit': unit,
                'percentage': value / max_value
            }
        scores['score'] = np.sum(label_values) / len(scores) / MAX_LABEL
        all_scores[cat] = scores
    return all_scores


@delayed
def score_delayed(metric, min_max_values: Dict):
    print("--score--")
    print(min_max_values)
    print(metric)
    all_scores = {
        'start': metric['start'],
        'stop': metric['stop'],
    }
    for cat in ['read', 'write', 'metadata']:
        result = metric[cat]
        label_values = []
        scores = {}
        for observation in OBSERVATIONS:
            value = 0
            if observation == 'size' and 'total_io_size' in result:
                value = result['total_io_size'] / 1024.0 / 1024.0 / 1024.0
            elif observation == 'time' and 'agg_dur' in result:
                value = result['agg_dur'] / result['uniq_ranks'] if result['uniq_ranks'] > 0 else 0
            elif observation == 'ops' and 'ops' in result:
                value = result['ops']
            elif observation == 'bw' and 'bw_sum' in result:
                value = result['bw_sum'] / 1024.0 / 1024.0
                # total_io_size = result['total_io_size'] / 1024.0 / 1024.0 / 1024.0
                # io_time_per_rank = result['agg_dur'] / result['uniq_ranks'] if result['uniq_ranks'] > 0 else 0
                # value = total_io_size / io_time_per_rank
            elif observation == 'parallelism' and 'uniq_ranks' in result:
                value = result['uniq_ranks']
            elif observation == 'files' and 'uniq_filenames' in result:
                value = result['uniq_filenames']
            elif observation == 'xfer' and 'total_io_size' in result:
                value = result['total_io_size'] / 1024.0 / 1024.0 / result['ops'] if result['ops'] > 0 else 0
            else:
                continue
            min_value, max_value = min_max_values[observation]
            min_max_indices = [np.finfo(float).min, np.finfo(float).max]
            indices = np.insert(min_max_indices, 1, [value])
            values = [min_value, value, max_value]
            if IS_NORMAL[observation]:
                values = [min_value, value, max_value / 2]
            labels = [label for label in range(1, MAX_LABEL + 1)]
            if IS_INVERSED[observation]:
                labels = list(reversed(labels))
                indices = list(reversed(indices))
                values = list(reversed(values))
            results = pd.Series(values, index=indices).sort_values(ascending=False).astype(float)
            if IS_NORMAL[observation]:
                normalized_results = stats.norm.pdf(results, loc=max_value / 2, scale=np.std(results))
                results = pd.Series(normalized_results, index=results.index)
            labeled_results = pd.cut(results, bins=MAX_LABEL, labels=labels)
            for min_max_index in min_max_indices:
                del labeled_results[min_max_index]
            label = labeled_results[labeled_results.index.values[0]]
            label_values.append(float(label))
            scores[observation] = {
                'label': labeled_results[labeled_results.index.values[0]],
                'value': value
            }
        scores['score'] = np.sum(label_values) / len(scores) / MAX_LABEL
        all_scores[cat] = scores
    return all_scores
