import pandas as pd
from typing import Iterable, List


def deepflatten(collection, ignore_types=(bytes, str)):
    for x in collection:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types):
            yield from deepflatten(x)
        else:
            yield x


def get_intervals(values: List[int]):
    series = pd.Series(sorted(values))
    grouped = series.groupby(series.diff().fillna(1).ne(1).cumsum())
    output = grouped.apply(lambda x: str(x.iloc[0]) if len(x) else str(x.iloc[0]) + '-' + str(x.iloc[-1]))
    return output.tolist()


def join_with_and(values: List[str]):
    if len(values) == 1:
        return values[0]
    elif len(values) == 2:
        return ' and '.join(values)
    else:
        return ', '.join(values[:-1]) + ', and ' + values[-1]
