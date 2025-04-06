import dask.dataframe as dd
import itertools as it
import portion as P


def nunique():
    return dd.Aggregation(
        name="nunique",
        chunk=lambda s: s.apply(lambda x: list(set(x))),
        agg=lambda s0: s0.obj.groupby(level=list(range(s0.obj.index.nlevels))).sum(),
        finalize=lambda s1: s1.apply(lambda final: len(set(final))),
    )


def unique_set():
    return dd.Aggregation(
        'unique',
        lambda s: s.apply(set),
        lambda s0: s0.apply(lambda x: set(it.chain.from_iterable(x))),
    )


def unique_set_flatten():
    return dd.Aggregation(
        'unique',
        lambda s: s.apply(lambda x: set().union(*x)),
        lambda s0: s0.agg(lambda x: set().union(*x)),
        lambda s1: s1.apply(set),
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
