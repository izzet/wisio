import dask.dataframe as dd
import itertools as it
import numpy as np
from ..utils.collection_utils import deepflatten


def unique():
    return dd.Aggregation(
        'unique',
        lambda s: s.apply(set),
        lambda s0: s0.apply(lambda x: list(set(it.chain.from_iterable(x))))
    )


def unique_flatten():
    return dd.Aggregation(
        'unique_flatten',
        lambda s: s.apply(lambda x: np.unique(x).tolist()),
        lambda s0: s0.apply(lambda x: np.unique(x).tolist()),
        lambda s1: s1.apply(lambda x: np.unique(list(deepflatten(x))).tolist()),
    )
