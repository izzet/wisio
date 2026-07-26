"""Unit tests for DarshanAnalyzer frame construction.

`_create_dxt_dataframe` and `_create_file_name_view` are the functions the
darshan ports modify (offset field, host_name handling, raw-stats source), and
they had no coverage. These tests pin the produced schema and values so a port
shows up as an intentional diff.

Constructed via `__new__` to skip Analyzer.__init__, which builds Dask state that
these pure pandas helpers do not touch.
"""

import pandas as pd
import pytest
from glob import glob

darshan = pytest.importorskip('darshan', reason='requires the [darshan] extra')

from wisio.constants import IOCategory
from wisio.darshan import DarshanAnalyzer


DXT_TRACE = 'tests/data/extracted/darshan-dxt/unet3d_a100.darshan'
RAW_TRACE_GLOB = 'tests/data/extracted/darshan-raw/*.darshan'

EXPECTED_DXT_COLUMNS = {
    'file_name',
    'proc_name',
    'size',
    'offset',
    'end_time',
    'start_time',
    'func_id',
    'host_name',
    'io_cat',
    'time_range',
    'cat',
    'acc_pat',
    'count',
    'time',
}


@pytest.fixture(scope='module')
def dxt_report():
    return darshan.DarshanReport(DXT_TRACE, read_all=True)


@pytest.fixture(scope='module')
def analyzer():
    instance = DarshanAnalyzer.__new__(DarshanAnalyzer)
    instance.time_granularity = 1e3
    return instance


@pytest.fixture(scope='module')
def dxt_frame(analyzer, dxt_report):
    return analyzer._create_dxt_dataframe(dxt_report)


@pytest.fixture(scope='module')
def raw_reports():
    paths = sorted(glob(RAW_TRACE_GLOB))
    assert len(paths) > 1, 'fixture should hold several .darshan reports'
    return [darshan.DarshanReport(path, read_all=True) for path in paths]


@pytest.fixture(scope='module')
def raw_frame(analyzer, raw_reports):
    return pd.concat(
        map(analyzer._create_file_name_view, raw_reports), ignore_index=True
    )


class TestCreateDxtDataframe:
    def test_schema(self, dxt_frame):
        assert set(dxt_frame.columns) == EXPECTED_DXT_COLUMNS

    def test_row_count_matches_raw_stats_total(self, dxt_frame):
        # Same 1953 that lands in checkpoints/_raw_stats.json.
        assert len(dxt_frame) == 1953

    def test_only_read_and_write_records(self, dxt_frame):
        assert set(dxt_frame['io_cat'].unique()) == {
            IOCategory.READ.value,
            IOCategory.WRITE.value,
        }

    def test_func_id_matches_io_cat(self, dxt_frame):
        reads = dxt_frame[dxt_frame['io_cat'] == IOCategory.READ.value]
        writes = dxt_frame[dxt_frame['io_cat'] == IOCategory.WRITE.value]

        assert set(reads['func_id'].unique()) == {'read'}
        assert set(writes['func_id'].unique()) == {'write'}

    def test_time_is_segment_duration(self, dxt_frame):
        expected = dxt_frame['end_time'] - dxt_frame['start_time']

        pd.testing.assert_series_equal(
            dxt_frame['time'], expected, check_names=False
        )

    def test_time_range_is_scaled_start_time(self, dxt_frame):
        """time_range = int(start_time * time_granularity).

        dfanalyzer 7e1c81d/84e61a7 multiply in a `time_resolution` factor that
        wisio has no concept of; this pins the current derivation.
        """
        expected = (dxt_frame['start_time'] * 1e3).astype(int)

        pd.testing.assert_series_equal(
            dxt_frame['time_range'], expected, check_names=False, check_dtype=False
        )

    def test_every_record_counts_once(self, dxt_frame):
        assert (dxt_frame['count'] == 1).all()

    def test_access_pattern_is_not_yet_detected(self, dxt_frame):
        """acc_pat is hardcoded to 0 -- sequential/random is never computed.

        Porting the `offset` field (dfanalyzer f56d13c) supplies the data needed
        to implement this. Update this test when that lands.
        """
        assert (dxt_frame['acc_pat'] == 0).all()

    def test_offset_is_captured(self, dxt_frame):
        """Ported from dfanalyzer f56d13c.

        Offsets were already read off the segment frame and discarded; they are
        the input required to classify sequential vs random access.
        """
        assert 'offset' in dxt_frame.columns
        assert dxt_frame['offset'].notna().all()
        assert (dxt_frame['offset'] >= 0).all()

    def test_offset_and_size_describe_distinct_ranges(self, dxt_frame):
        """Offsets must vary independently of size, or they carry no signal."""
        assert dxt_frame['offset'].nunique() > 1


class TestCreateFileNameView:
    """Non-DXT path, built from POSIX counters rather than DXT segments."""

    def test_host_name_column_exists(self, raw_frame):
        """Ported from dfanalyzer bbd4437.

        POSIX counters carry no hostname, so this is a constant placeholder --
        but the column must exist for host-count statistics to be computable.
        """
        assert 'host_name' in raw_frame.columns
        assert set(raw_frame['host_name'].unique()) == {'localhost'}

    def test_proc_name_is_built_from_host_name(self, raw_frame):
        assert raw_frame['proc_name'].str.startswith('app#localhost#').all()

    def test_aggregation_collapses_records_across_reports(self, raw_frame):
        """Why total_count must be measured before the groupby.

        Several single-process reports all key to the same (file_name,
        proc_name), so grouping loses rows. Sourcing total_count from the
        grouped frame (the pre-913b602 behavior) made the retention statistics
        report 100% no matter how much collapsed.
        """
        grouped = raw_frame.groupby(['file_name', 'proc_name']).sum()

        assert len(grouped) < len(raw_frame), 'expected the groupby to collapse rows'
        # Pinning the observed fixture values keeps the regression visible.
        assert len(raw_frame) == 502
        assert len(grouped) == 286


class TestProcNameHostHandling:
    """KNOWN DEFECT: proc_name hardcodes 'localhost'.

    DXT records carry the real hostname (`host_name` column), but proc_name is
    built as 'app#localhost#<rank>#0'. Because `node_name` is derived from
    proc_name in analysis_utils.set_proc_name_parts, node-level views report
    'localhost' for every process regardless of the real host.

    dfanalyzer bbd4437 does NOT fix this -- it only extracts the literal into a
    DEFAULT_HOST_NAME constant. Pinned here as a wisio improvement target.
    """

    def test_real_hostname_is_present_in_records(self, dxt_frame):
        hosts = set(dxt_frame['host_name'].unique())

        assert hosts, 'expected a real hostname from the DXT record'
        assert 'localhost' not in hosts

    def test_proc_name_discards_the_real_hostname(self, dxt_frame):
        assert dxt_frame['proc_name'].str.startswith('app#localhost#').all()

    def test_node_name_derived_from_proc_name_is_localhost(self, dxt_frame):
        node_names = dxt_frame['proc_name'].str.split('#').str[1].unique()

        # The bug in one line: real hosts exist, but every node view says this.
        assert list(node_names) == ['localhost']
