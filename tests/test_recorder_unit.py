"""Unit tests for RecorderAnalyzer helpers.

These pin down the behavior that the darshan/recorder ports touch, so a port that
changes bucketing, dtypes, or column handling fails here rather than silently
shifting numbers in an end-to-end run.
"""

import json
import numpy as np
import pandas as pd
import pytest

from wisio.recorder import CAT_POSIX, DROPPED_COLS, RENAMED_COLS, RecorderAnalyzer


class TestLoadGlobalMinMax:
    def test_reads_global_json(self, tmp_path):
        payload = {'tmid': [0.5, 10.5], 'tstart': [0.0, 10.0]}
        (tmp_path / 'global.json').write_text(json.dumps(payload))

        assert RecorderAnalyzer._load_global_min_max(str(tmp_path)) == payload

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            RecorderAnalyzer._load_global_min_max(str(tmp_path))


class TestSetTimeRanges:
    """`_set_time_ranges` buckets events by tmid via np.digitize(right=True).

    The `time_resolution` port (dfanalyzer 7e1c81d) changes how bin edges are
    derived, so the exact bucket assignment is pinned here.
    """

    def test_assigns_time_range_column(self):
        df = pd.DataFrame({'tmid': [0.0, 1.5, 2.5, 3.5]})
        bins = np.array([0.0, 1.0, 2.0, 3.0])

        result = RecorderAnalyzer._set_time_ranges(df, time_ranges=bins)

        assert 'time_range' in result.columns
        # right=True -> an exact edge falls in the bin ending at that edge.
        assert result['time_range'].tolist() == [0, 2, 3, 4]

    def test_exact_edge_maps_to_its_own_bin(self):
        df = pd.DataFrame({'tmid': [1.0, 2.0, 3.0]})
        bins = np.array([0.0, 1.0, 2.0, 3.0])

        result = RecorderAnalyzer._set_time_ranges(df, time_ranges=bins)

        assert result['time_range'].tolist() == [1, 2, 3]

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({'tmid': [0.0, 1.5]})
        original = df.copy(deep=True)

        RecorderAnalyzer._set_time_ranges(df, time_ranges=np.array([0.0, 1.0]))

        pd.testing.assert_frame_equal(df, original)

    def test_preserves_other_columns(self):
        df = pd.DataFrame({'tmid': [0.5], 'io_cat': [1], 'size': [4096]})

        result = RecorderAnalyzer._set_time_ranges(df, time_ranges=np.array([0.0, 1.0]))

        assert result['io_cat'].tolist() == [1]
        assert result['size'].tolist() == [4096]


class TestModuleConstants:
    """The ports rename/drop columns; pin the current contract."""

    def test_posix_category_value(self):
        assert CAT_POSIX == 0

    def test_duration_is_renamed_to_time(self):
        # dfanalyzer maps this through COL_TIME; wisio renames directly.
        assert RENAMED_COLS == {'duration': 'time'}

    def test_dropped_cols_include_raw_timing_columns(self):
        # `tmid`/`tstart`/`tend` are consumed by time bucketing and must not
        # survive into the analyzed frame.
        for col in ('tmid', 'tstart', 'tend'):
            assert col in DROPPED_COLS
