"""Unit tests for the DFTracer reader built on dftracer-utils.

These cover the pure transformation helpers -- record classification, size
derivation, file resolution -- without touching Dask or the C++ indexer, so they
run in milliseconds and pin the behavior that replaced the zindex reader.
"""

import pandas as pd
import pytest

pytest.importorskip('dftracer.utils', reason='requires the [dftracer] extra')

from wisio.dftracer import (  # noqa: E402
    CAT_POSIX,
    RECORD_COLUMNS,
    TYPE_EVENT,
    TYPE_FILE_HASH,
    TYPE_HOST_HASH,
    TYPE_METADATA,
    TYPE_STRING_HASH,
    classify_records,
    derive_size,
    empty_records,
    resolve_trace_files,
)


def _frame(rows):
    return pd.DataFrame(rows)


class TestClassifyRecords:
    """Phase-'M' rows are metadata; `name` picks which kind."""

    def test_events_are_type_zero(self):
        df = _frame([{'ph': 'X', 'name': 'read'}, {'ph': 'X', 'name': 'write'}])

        assert classify_records(df).tolist() == [TYPE_EVENT, TYPE_EVENT]

    def test_hash_records_by_name(self):
        df = _frame(
            [
                {'ph': 'M', 'name': 'FH'},
                {'ph': 'M', 'name': 'HH'},
                {'ph': 'M', 'name': 'SH'},
            ]
        )

        assert classify_records(df).tolist() == [
            TYPE_FILE_HASH,
            TYPE_HOST_HASH,
            TYPE_STRING_HASH,
        ]

    def test_other_metadata_falls_through(self):
        df = _frame([{'ph': 'M', 'name': 'PR'}, {'ph': 'M', 'name': 'rank'}])

        assert classify_records(df).tolist() == [TYPE_METADATA, TYPE_METADATA]

    def test_event_named_like_a_hash_record_is_still_an_event(self):
        """Only phase 'M' makes a record metadata."""
        df = _frame([{'ph': 'X', 'name': 'FH'}])

        assert classify_records(df).tolist() == [TYPE_EVENT]

    def test_missing_columns_are_tolerated(self):
        """A file whose records never carried `ph` must not blow up."""
        df = _frame([{'name': 'read'}, {'name': 'write'}])

        assert classify_records(df).tolist() == [TYPE_EVENT, TYPE_EVENT]


class TestDeriveSize:
    """Size is the syscall return value, only where that means bytes moved."""

    def test_posix_read_and_write_use_ret(self):
        df = _frame(
            [
                {'cat': CAT_POSIX, 'name': 'read', 'args.ret': 4096},
                {'cat': CAT_POSIX, 'name': 'write', 'args.ret': 8192},
            ]
        )

        assert derive_size(df).tolist() == [4096, 8192]

    def test_readdir_transfers_no_bytes(self):
        """`readdir` matches 'read' textually but moves no data."""
        df = _frame([{'cat': CAT_POSIX, 'name': 'readdir', 'args.ret': 12}])

        assert derive_size(df).isna().all()

    def test_metadata_calls_have_no_size(self):
        df = _frame([{'cat': CAT_POSIX, 'name': 'open', 'args.ret': 3}])

        assert derive_size(df).isna().all()

    def test_negative_and_zero_returns_are_dropped(self):
        """A failed syscall returns -1; that is not a transfer."""
        df = _frame(
            [
                {'cat': CAT_POSIX, 'name': 'read', 'args.ret': -1},
                {'cat': CAT_POSIX, 'name': 'write', 'args.ret': 0},
            ]
        )

        assert derive_size(df).isna().all()

    def test_non_posix_uses_image_size(self):
        df = _frame([{'cat': 'dftracer', 'name': 'load', 'args.image_size': 2048}])

        assert derive_size(df).tolist() == [2048]

    def test_image_size_ignored_for_posix(self):
        df = _frame(
            [{'cat': CAT_POSIX, 'name': 'open', 'args.ret': 3, 'args.image_size': 999}]
        )

        assert derive_size(df).isna().all()

    def test_missing_ret_column_is_tolerated(self):
        df = _frame([{'cat': CAT_POSIX, 'name': 'read'}])

        assert derive_size(df).isna().all()


class TestResolveTraceFiles:
    def test_picks_up_pfw_and_gz(self, tmp_path):
        for name in ('a.pfw', 'b.pfw.gz'):
            (tmp_path / name).write_text('')

        found = resolve_trace_files(str(tmp_path))

        assert [f.rsplit('/', 1)[-1] for f in found] == ['a.pfw', 'b.pfw.gz']

    def test_ignores_unrelated_files(self, tmp_path):
        (tmp_path / 'a.pfw').write_text('')
        # Stale zindex sidecars ship inside the test fixtures.
        (tmp_path / 'a.pfw.gz.zindex').write_text('')
        (tmp_path / 'notes.txt').write_text('')

        found = resolve_trace_files(str(tmp_path))

        assert [f.rsplit('/', 1)[-1] for f in found] == ['a.pfw']

    def test_empty_directory_yields_nothing(self, tmp_path):
        assert resolve_trace_files(str(tmp_path)) == []

    def test_accepts_an_explicit_glob(self, tmp_path):
        (tmp_path / 'a.pfw').write_text('')
        (tmp_path / 'b.pfw').write_text('')

        assert len(resolve_trace_files(f'{tmp_path}/*.pfw')) == 2


class TestEmptyRecords:
    def test_matches_declared_schema(self):
        empty = empty_records()

        assert list(empty.columns) == list(RECORD_COLUMNS)
        assert len(empty) == 0

    def test_column_order_is_stable(self):
        """Dask matches partition schemas positionally, so order is load-bearing."""
        assert list(empty_records().columns) == list(empty_records().columns)
