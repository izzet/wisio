"""Tests for scoring helpers in `wisio.analysis`."""

import numpy as np
import pytest

from wisio.analysis import SCORE_NAMES, SLOPE_BINS, ops_time_ratio, set_metric_scores
import pandas as pd


@pytest.mark.parametrize(
    'slope, expected',
    [
        (0.5, 2.0),
        (2.0, 0.5),
        (1.0, 1.0),
        (0.0957, pytest.approx(10.449, rel=1e-3)),
    ],
)
def test_ops_time_ratio_inverts_the_slope(slope, expected):
    assert ops_time_ratio(slope) == expected


@pytest.mark.parametrize('slope', [None, 0, -1, np.nan, np.inf])
def test_ops_time_ratio_rejects_unusable_slopes(slope):
    """A zero or missing slope has no meaningful reciprocal."""
    assert ops_time_ratio(slope) is None


def test_low_slope_scores_worst():
    """Severity runs opposite to the slope, which is easy to get backwards.

    `slope = count_per / time_per`, so a *low* slope means few operations
    consuming a lot of time -- the worst case. The bins are tan(80deg) down to
    tan(10deg), and a finding below tan(10deg) is critical.
    """
    slopes = [
        10.0,  # above tan(80), very efficient
        SLOPE_BINS['iops'][2] - 0.01,  # just under tan(60)
        0.05,  # below tan(10), disproportionately slow
    ]
    df = pd.DataFrame({'iops_slope': slopes, 'iops_pth': [0.0] * len(slopes)})

    scored = set_metric_scores(
        df,
        view_type='file_name',
        metric='iops',
        metric_boundary=1.0,
        is_slope_based=True,
    )

    scores = list(scored['iops_score'])
    assert scores[-1] == 'critical'
    assert scores[0] == SCORE_NAMES[0]
    # Strictly worsening as the slope falls.
    assert [SCORE_NAMES.index(score) for score in scores] == sorted(
        SCORE_NAMES.index(score) for score in scores
    )


def test_break_even_is_tan_45():
    """tan(45deg) is the reference the slope bins are built around."""
    assert np.tan(np.deg2rad(45)) == pytest.approx(1.0)
    # A ratio of 1.0 means time share equals operation share.
    assert ops_time_ratio(np.tan(np.deg2rad(45))) == pytest.approx(1.0)
