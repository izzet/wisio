"""Filename handling for uploaded traces.

Separate from `streamlit_app.py` so it can be tested: Streamlit executes app
scripts top to bottom, so a function defined there is unreachable from a test.
This one is worth testing on its own -- it is the boundary where a name chosen
by whoever produced the trace decides where a file gets written.
"""

import os
from typing import Iterable, List


def safe_trace_filenames(names: Iterable[str]) -> List[str]:
    """Flatten uploaded names to unique, writable basenames.

    Directory uploads report a path relative to the chosen folder, so a name
    can be `worker_0/trace.pfw.gz` or, if something upstream is careless or
    hostile, `../../etc/passwd`. Both are handled the same way: take the
    basename, which cannot escape the directory it is joined to.

    Flattening is also what the readers need. `resolve_trace_files` globs
    `<dir>/*.pfw*` rather than walking the tree, so a file left in a
    subdirectory would simply not be found.

    Collisions become real once the tree is flattened -- `worker_0/trace.pfw.gz`
    and `worker_1/trace.pfw.gz` are distinct traces with one basename -- so
    later duplicates are suffixed rather than overwriting each other.

    Args:
        names: Uploaded filenames, in submission order.

    Returns:
        One safe name per input, in the same order.
    """
    safe_names = []
    used = set()

    for name in names:
        base = os.path.basename(str(name).replace('\\', '/')).strip()
        if not base or base in {'.', '..'}:
            base = 'trace'

        candidate = base
        if candidate in used:
            stem, dot, suffix = base.partition('.')
            index = 1
            while candidate in used:
                candidate = f"{stem}-{index}{dot}{suffix}"
                index += 1

        used.add(candidate)
        safe_names.append(candidate)

    return safe_names
