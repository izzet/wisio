from typing import Iterable


def deepflatten(collection, ignore_types=(bytes, str)):
    """
       Flatten all of the nested lists to the one. Ignoring flatting of iterable types str and bytes by default.
    """
    for x in collection:
        if isinstance(x, Iterable) and not isinstance(x, ignore_types):
            yield from deepflatten(x)
        else:
            yield x
