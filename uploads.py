"""Filename handling for uploaded traces.

Separate from `streamlit_app.py` so it can be tested: Streamlit executes app
scripts top to bottom, so a function defined there is unreachable from a test.
This one is worth testing on its own -- it is the boundary where a name chosen
by whoever produced the trace decides where a file gets written.
"""

import os
from typing import Iterable, List


def safe_trace_filenames(names: Iterable[str]) -> List[str]:
    """Reduce uploaded names to unique, writable basenames.

    The uploader accepts files additively, so a run split across directories
    can contribute two files with one basename -- `worker_0/trace.pfw.gz` and
    `worker_1/trace.pfw.gz` are distinct traces. Writing both under the same
    name would silently drop one, so later duplicates are suffixed.

    Taking the basename also means a name can never escape the directory it is
    joined to. A browser sends only the filename for individually picked files,
    so that is insurance rather than a live hole -- but this is the point where
    a name chosen elsewhere decides where a file is written, and the check
    costs nothing.

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
