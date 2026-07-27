"""Tests for the web app's bottleneck extraction.

This is the half of the app's bottleneck rendering that can be tested: the walk
over the table, deciding which rules fired, which reasons fired, and how many
files/processes/periods a view is describing. The Streamlit half is covered by
the AppTest smoke test.
"""

import pandas as pd
import pytest

from bottleneck_report import describe_bottlenecks, is_primary_view
from wisio.rules import KNOWN_RULES, BottleneckRule


METRIC = 'iops'


@pytest.fixture
def rules():
    return {
        rule: BottleneckRule(rule_key=rule, rule=KNOWN_RULES[rule], verbose=False)
        for rule in KNOWN_RULES
    }


def _row(view_name, score='critical', **overrides):
    """One bottleneck row with the columns the extraction reads.

    The `_time`/`_size`/`_count` values are here because the reason messages are
    Jinja templates rendered against the row, so a missing column raises rather
    than producing an empty string.
    """
    row = {
        'view_name': view_name,
        'metric': METRIC,
        f"{METRIC}_score": score,
        'subject': 'subject-1',
        'count': 100,
        'time': 1.5,
        'time_overall': 0.5,
        'num_file_name': 2,
        'num_proc_name': 3,
        'num_time_range': 4,
        'read_time': 0.5,
        'write_time': 1.0,
        'metadata_time': 0.0,
        'read_count': 40,
        'write_count': 60,
        'metadata_count': 0,
        'read_size': 4096,
        'write_size': 8192,
        'random_count': 10,
    }
    for rule in KNOWN_RULES:
        row[rule] = False
        for index in range(len(KNOWN_RULES[rule].reasons)):
            row[f"{rule}.reason.{index}"] = False
    row.update(overrides)
    return row


def _frame(rows):
    return pd.DataFrame(rows)


@pytest.mark.parametrize(
    'view_name, expected',
    [
        ('file_name', True),  # root
        ('proc_name', True),
        ('time_range', True),
        ('proc_name.node_name', True),  # logical breakdown of a root view
        ('proc_name.app_name', True),
        ('file_name.file_dir', True),
        ('file_name.file_pattern', True),
        ('time_range.file_name', False),  # permutation
        ('file_name.proc_name', False),
        ('time_range.file_name.proc_name', False),
    ],
)
def test_primary_views_are_root_or_logical(view_name, expected):
    assert is_primary_view(view_name) is expected


def test_permutation_views_are_dropped_by_default(rules):
    """A real trace produces far more permutations than root views."""
    views = describe_bottlenecks(
        _frame(
            [
                _row('file_name'),
                _row('proc_name'),
                _row('time_range'),
                _row('proc_name.node_name'),  # logical, kept
                _row('time_range.file_name'),  # permutation, dropped
                _row('file_name.proc_name.time_range'),  # permutation, dropped
            ]
        ),
        rules,
        metric=METRIC,
    )

    assert sorted(view.name for view in views) == [
        'File View',
        'Process > Node View',
        'Process View',
        'Time View',
    ]


def test_permutations_can_be_kept_explicitly(rules):
    views = describe_bottlenecks(
        _frame([_row('file_name'), _row('time_range.file_name')]),
        rules,
        metric=METRIC,
        primary_views_only=False,
    )

    assert len(views) == 2


def test_all_permutations_leaves_no_views(rules):
    assert (
        describe_bottlenecks(
            _frame([_row('time_range.file_name')]), rules, metric=METRIC
        )
        == []
    )


def test_ratio_is_carried_and_stated_for_iops(rules):
    """Severity is scored on cost per operation, so the finding must say so.

    Without this a critical finding worth 0.0% of I/O time reads like a
    mislabel: the number that justifies the score is the ratio, not the share.
    """
    views = describe_bottlenecks(
        _frame([_row('file_name', iops_slope=0.1)]), rules, metric=METRIC
    )

    bottleneck = views[0].bottlenecks[0]
    assert bottleneck.ops_time_ratio == pytest.approx(10.0)
    assert '10.0x as much of the I/O time' in bottleneck.description


def test_break_even_ratio_is_left_unsaid(rules):
    """At ~1x the clause would round to '1.0x', which explains nothing."""
    views = describe_bottlenecks(
        _frame([_row('file_name', iops_slope=0.99)]), rules, metric=METRIC
    )

    assert 'as much of the I/O time' not in views[0].bottlenecks[0].description


def test_ratio_is_absent_for_non_slope_metrics(rules):
    """For `time` the slope is already a fraction and the sentence states it."""
    views = describe_bottlenecks(
        _frame([_row('file_name', time_slope=0.1)]), rules, metric='time'
    )

    assert views[0].bottlenecks[0].ops_time_ratio is None


def test_counts_are_carried_alongside_the_sentence(rules):
    """A renderer should not have to parse numbers back out of the prose."""
    views = describe_bottlenecks(
        _frame([_row('file_name', count=1234, time=2.5, time_overall=0.25)]),
        rules,
        metric=METRIC,
    )

    bottleneck = views[0].bottlenecks[0]
    assert bottleneck.num_ops == 1234
    assert bottleneck.time == 2.5
    assert bottleneck.time_overall == 0.25
    assert bottleneck.num_processes == 3
    assert bottleneck.num_files == 2


def test_no_bottlenecks_returns_no_views(rules):
    assert describe_bottlenecks(_frame([]), rules, metric=METRIC) == []
    assert describe_bottlenecks(None, rules, metric=METRIC) == []


def test_groups_by_view_and_names_it(rules):
    views = describe_bottlenecks(
        _frame([_row('time_range'), _row('file_name'), _row('file_name')]),
        rules,
        metric=METRIC,
    )

    by_name = {view.name: view for view in views}
    assert set(by_name) == {'Time View', 'File View'}
    assert by_name['File View'].num_bottlenecks == 2
    assert by_name['Time View'].num_bottlenecks == 1


def test_ids_are_assigned_globally_when_absent(rules):
    """Labels must be unique across the whole report, not per view."""
    views = describe_bottlenecks(
        _frame([_row('time_range'), _row('file_name')]), rules, metric=METRIC
    )

    labels = sorted(b.label for view in views for b in view.bottlenecks)
    assert labels == ['CR1', 'CR2']


def test_existing_ids_are_preserved(rules):
    views = describe_bottlenecks(
        _frame([_row('file_name', id=7)]), rules, metric=METRIC
    )

    assert views[0].bottlenecks[0].label == 'CR7'


def test_label_encodes_severity(rules):
    views = describe_bottlenecks(
        _frame([_row('file_name', score='medium', id=1)]), rules, metric=METRIC
    )

    assert views[0].bottlenecks[0].label == 'MD1'
    assert views[0].bottlenecks[0].score == 'medium'


def test_bottlenecks_are_ordered_worst_first(rules):
    views = describe_bottlenecks(
        _frame(
            [
                _row('file_name', score='low', id=1),
                _row('file_name', score='critical', id=2),
                _row('file_name', score='medium', id=3),
            ]
        ),
        rules,
        metric=METRIC,
    )

    assert [b.score for b in views[0].bottlenecks] == ['critical', 'medium', 'low']


def test_only_fired_reasons_are_described(rules):
    rule = 'small_writes'
    views = describe_bottlenecks(
        _frame([_row('file_name', **{rule: True, f"{rule}.reason.0": True})]),
        rules,
        metric=METRIC,
    )

    reasons = views[0].bottlenecks[0].reasons
    assert len(reasons) == 1
    assert reasons[0].rule == rule
    assert reasons[0].description


def test_reason_without_its_rule_is_not_counted(rules):
    """A reason column set while its rule did not fire must be ignored."""
    rule = 'small_writes'
    views = describe_bottlenecks(
        _frame([_row('file_name', **{rule: False, f"{rule}.reason.0": True})]),
        rules,
        metric=METRIC,
    )

    assert views[0].bottlenecks[0].reasons == []
    assert views[0].num_reasons == 0


def test_fired_rule_with_no_reason_is_still_reported(rules):
    rule = 'small_writes'
    views = describe_bottlenecks(
        _frame([_row('file_name', **{rule: True})]), rules, metric=METRIC
    )

    reasons = views[0].bottlenecks[0].reasons
    assert len(reasons) == 1
    assert 'investigation' in reasons[0].description.lower()


def test_view_dimension_overrides_the_generic_count(rules):
    """In a File view the file total comes from that view's own column."""
    views = describe_bottlenecks(
        _frame([_row('file_name', num_file_name=99)]), rules, metric=METRIC
    )

    assert '99 files' in views[0].bottlenecks[0].description


def test_max_bottlenecks_truncates_and_counts_the_rest(rules):
    views = describe_bottlenecks(
        _frame([_row('file_name', id=i) for i in range(1, 6)]),
        rules,
        metric=METRIC,
        max_bottlenecks=2,
    )

    assert len(views[0].bottlenecks) == 2
    assert views[0].num_hidden == 3
    assert views[0].num_bottlenecks == 5


def test_max_bottlenecks_zero_shows_everything(rules):
    views = describe_bottlenecks(
        _frame([_row('file_name', id=i) for i in range(1, 6)]),
        rules,
        metric=METRIC,
        max_bottlenecks=0,
    )

    assert len(views[0].bottlenecks) == 5
    assert views[0].num_hidden == 0
