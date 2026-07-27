"""Tests for uploaded trace filename handling."""

import pytest

from uploads import safe_trace_filenames


def test_plain_names_are_unchanged():
    assert safe_trace_filenames(['a.pfw.gz', 'b.pfw.gz']) == ['a.pfw.gz', 'b.pfw.gz']


def test_directory_paths_are_flattened():
    """The readers glob one directory rather than walking the tree.

    A file left under `worker_0/` would not be found by
    `resolve_trace_files`, which globs `<dir>/*.pfw*`.
    """
    assert safe_trace_filenames(['worker_0/trace.pfw.gz']) == ['trace.pfw.gz']
    assert safe_trace_filenames(['a/b/c/trace.pfw.gz']) == ['trace.pfw.gz']


@pytest.mark.parametrize(
    'name',
    [
        '../../etc/passwd',
        '../trace.pfw.gz',
        '/etc/passwd',
        '..\\..\\windows\\system32',
    ],
)
def test_traversal_cannot_escape_the_directory(name):
    """The name comes from whoever produced the trace, so it is not trusted."""
    (safe,) = safe_trace_filenames([name])

    assert '/' not in safe
    assert '\\' not in safe
    assert safe not in {'.', '..'}


def test_flattening_collisions_are_kept_distinct():
    """Two ranks writing the same basename are different traces."""
    names = [
        'worker_0/trace.pfw.gz',
        'worker_1/trace.pfw.gz',
        'worker_2/trace.pfw.gz',
    ]

    safe = safe_trace_filenames(names)

    assert len(set(safe)) == 3
    assert safe[0] == 'trace.pfw.gz'
    assert all(name.endswith('.pfw.gz') for name in safe)


def test_degenerate_names_get_a_fallback():
    for name in ['', '   ', '.', '..', '/']:
        (safe,) = safe_trace_filenames([name])
        assert safe == 'trace', name


def test_order_is_preserved():
    """Names are zipped against the uploaded files, so order is load-bearing."""
    names = ['b/2.pfw', 'a/1.pfw', 'c/3.pfw']

    assert safe_trace_filenames(names) == ['2.pfw', '1.pfw', '3.pfw']
