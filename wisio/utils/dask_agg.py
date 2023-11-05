import dask.dataframe as dd
import itertools as it
import numpy as np
import portion as P
from ..utils.collection_utils import deepflatten


def unique():
    return dd.Aggregation(
        'unique',
        lambda s: s.apply(set),
        lambda s0: s0.apply(lambda x: list(set(it.chain.from_iterable(x))))
    )


def union_portions():
    def union_s(s):
        emp = P.empty()
        for x in s:
            emp = emp | x
        return emp
    def fin(s):
        val = 0.0
        for i in s:
            if not i.is_empty():
                val += i.upper - i.lower
        return val
    return dd.Aggregation(
        'portion',
        union_s,
        union_s,
        fin,
    )


def unique_flatten():
    return dd.Aggregation(
        'unique_flatten',
        lambda s: s.apply(lambda x: np.unique(x).tolist()),
        lambda s0: s0.apply(lambda x: np.unique(x).tolist()),
        lambda s1: s1.apply(lambda x: np.unique(list(deepflatten(x))).tolist()),
    )
