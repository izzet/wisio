"""Tests for the web app's bottleneck extraction.

This is the half of the app's bottleneck rendering that can be tested: the walk
over the table, deciding which rules fired, which reasons fired, and how many
files/processes/periods a view is describing. The Streamlit half is covered by
the AppTest smoke test.
"""

import pandas as pd
import pytest

from bottleneck_report import describe_bottlenecks
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
