"""Turn the bottleneck table into structured, readable findings for the web app.

`streamlit_app.py` cannot hold this. Streamlit executes app scripts top to
bottom, so anything defined there is unreachable from a test without driving a
full upload through the UI -- and the fiddly part of presenting bottlenecks is
not the widgets, it is working out which rules fired, which of their reasons
fired, and how many files/processes/periods a given view is talking about.

The sentences themselves come from `BottleneckRule.describe_bottleneck` and
`describe_reason`, the same methods the console output uses, so the wording
cannot drift between the two. Only the walk over the table is repeated here,
deliberately: the console renders a `rich` tree and the app renders expanders,
and those two want different shapes badly enough that a shared abstraction
would fit neither.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import pandas as pd

from wisio.analysis import SCORE_INITIALS, SCORE_NAMES
from wisio.constants import (
    COL_APP_NAME,
    COL_FILE_DIR,
    COL_FILE_NAME,
    COL_FILE_PATTERN,
    COL_NODE_NAME,
    COL_PROC_NAME,
    COL_RANK,
    LOGICAL_VIEW_TYPES,
)
from wisio.rules import HUMANIZED_KNOWN_RULES
from wisio.types import humanized_view_name

# A view is named after its last component, and that component decides which
# `num_*` column describes "how many files" and "how many processes".
FILE_VIEW_TYPES = (COL_FILE_NAME, COL_FILE_DIR, COL_FILE_PATTERN)
PROCESS_VIEW_TYPES = (COL_APP_NAME, COL_NODE_NAME, COL_PROC_NAME, COL_RANK)

# Views worth reporting on their own terms: the root ones, plus the logical
# breakdowns of a root view. Everything else is a permutation -- 'Time > File',
# 'Time > Process' and so on -- which multiplies the view count without adding a
# perspective a reader asked for. A real trace produces hundreds of bottlenecks,
# so those permutations are the difference between five views and twenty.
LOGICAL_VIEW_NAMES = frozenset('.'.join(key) for key in LOGICAL_VIEW_TYPES)

DEFAULT_MAX_BOTTLENECKS = 20


@dataclass
class Reason:
    """One rule's explanation of why a bottleneck is a bottleneck."""

    rule: str
    rule_name: str
    description: str


@dataclass
class Bottleneck:
    """A single finding, with the reasons that fired for it."""

    id: int
    label: str
    score: str
    severity: int
    subject: str
    description: str
    # Carried alongside the sentence so a renderer can lead with the numbers
    # instead of making a reader parse them back out of prose.
    num_files: int = 0
    num_processes: int = 0
    num_time_periods: int = 0
    num_ops: int = 0
    time: float = 0.0
    time_overall: float = 0.0
    reasons: List[Reason] = field(default_factory=list)


@dataclass
class BottleneckView:
    """All findings for one perspective, e.g. the File view."""

    view_key: Tuple[str, ...]
    name: str
    num_bottlenecks: int
    num_reasons: int
    bottlenecks: List[Bottleneck] = field(default_factory=list)
    num_hidden: int = 0


def _severity(score: str) -> int:
    """Rank of a score, so findings can be ordered worst-first."""
    return SCORE_NAMES.index(score) if score in SCORE_NAMES else 0


def _subject_counts(row: pd.Series, view_type: str) -> Tuple[int, int, int]:
    """How many files, processes and time periods this bottleneck covers.

    The view's own dimension overrides the generic count: in a File view the
    file total comes from that view's column rather than `num_file_name`.
    """
    num_files = int(row.get('num_file_name', 0) or 0)
    num_processes = int(row.get('num_proc_name', 0) or 0)
    num_time_periods = int(row.get('num_time_range', 0) or 0)

    if view_type in FILE_VIEW_TYPES:
        num_files = int(row.get(f"num_{view_type}", 0) or 0)
    if view_type in PROCESS_VIEW_TYPES:
        num_processes = int(row.get(f"num_{view_type}", 0) or 0)

    return num_files, num_processes, num_time_periods


def _reasons_for(row: pd.Series, bottleneck_rules: Dict[str, object]) -> List[Reason]:
    """Every reason that fired, across every rule that fired."""
    reasons = []
    for rule, rule_impl in bottleneck_rules.items():
        if not row.get(rule, False):
            continue

        rule_name = HUMANIZED_KNOWN_RULES.get(rule, rule)
        indices = range(len(rule_impl.rule.reasons))
        fired = [i for i in indices if row.get(f"{rule}.reason.{i}", False)]

        if not fired:
            # The rule matched but nothing explained it. The console says the
            # same thing rather than dropping the finding silently.
            reasons.append(
                Reason(
                    rule=rule,
                    rule_name=rule_name,
                    description='No reason found, investigation needed.',
                )
            )
            continue

        for index in fired:
            reasons.append(
                Reason(
                    rule=rule,
                    rule_name=rule_name,
                    description=rule_impl.describe_reason(
                        bottleneck=dict(row), reason_index=index
                    ),
                )
            )
    return reasons


def is_primary_view(view_name: str) -> bool:
    """Whether a view stands on its own rather than being a permutation.

    Root views are single-component. Logical views have two components but are
    a breakdown of a root view -- processes by node, files by directory -- so
    they are reported when the user asks for them. `Time > File` and friends are
    permutations and are not.
    """
    return '.' not in view_name or view_name in LOGICAL_VIEW_NAMES


def describe_bottlenecks(
    bottlenecks: pd.DataFrame,
    bottleneck_rules: Dict[str, object],
    metric: str,
    max_bottlenecks: int = DEFAULT_MAX_BOTTLENECKS,
    compact: bool = True,
    primary_views_only: bool = True,
) -> List[BottleneckView]:
    """Group the bottleneck table into described findings, per view.

    Args:
        bottlenecks: The computed bottleneck table, one row per finding.
        bottleneck_rules: The rule implementations, from `AnalyzerResultType`.
        metric: The metric being reported, which selects the score column.
        max_bottlenecks: Findings to describe per view; the rest are counted
            in `num_hidden`. Describing is cheap, but a real trace yields
            hundreds of bottlenecks and a page cannot usefully show them all.
        compact: Shorten a file subject to its basename.
        primary_views_only: Drop permutation views, keeping root and logical
            ones. See `is_primary_view`.

    Returns:
        One `BottleneckView` per perspective, each with its findings ordered
        worst-first. Empty when there are no bottlenecks.
    """
    if bottlenecks is None or len(bottlenecks) == 0:
        return []

    if primary_views_only:
        keep = bottlenecks['view_name'].map(is_primary_view)
        bottlenecks = bottlenecks[keep]
        if len(bottlenecks) == 0:
            return []

    # `id` is what makes a finding citable ("the CR2 one"). The console adds it
    # while reading the parquet back; the in-memory table has not been through
    # that, so number the rows the same way -- globally, before grouping, so a
    # label is unique across the whole report rather than per view.
    if 'id' not in bottlenecks.columns:
        bottlenecks = bottlenecks.reset_index(drop=True)
        bottlenecks = bottlenecks.assign(id=range(1, len(bottlenecks) + 1))

    score_column = f"{metric}_score"
    reason_columns = [col for col in bottlenecks.columns if '.reason.' in col]

    views = []
    for view_name in bottlenecks['view_name'].unique():
        rows = bottlenecks[bottlenecks['view_name'] == view_name]
        view_key = tuple(view_name.split('.'))
        view_type = view_key[-1]

        # A reason counts only when its rule fired too, matching the console.
        num_reasons = sum(
            int((rows[col.split('.')[0]] & rows[col]).sum()) for col in reason_columns
        )

        described = []
        for _, row in rows.iterrows():
            num_files, num_processes, num_time_periods = _subject_counts(row, view_type)
            score = row.get(score_column, SCORE_NAMES[0])
            # Any rule can build the description -- it reads only these
            # arguments, not the rule it is called on.
            any_rule = next(iter(bottleneck_rules.values()))
            described.append(
                Bottleneck(
                    id=int(row.get('id', 0) or 0),
                    label=f"{SCORE_INITIALS.get(score, '')}{int(row.get('id', 0) or 0)}",
                    score=score,
                    severity=_severity(score),
                    subject=str(row.get('subject', '')),
                    num_files=num_files,
                    num_processes=num_processes,
                    num_time_periods=num_time_periods,
                    num_ops=int(row.get('count', 0) or 0),
                    time=float(row.get('time', 0) or 0),
                    time_overall=float(row.get('time_overall', 0) or 0),
                    description=any_rule.describe_bottleneck(
                        compact=compact,
                        metric=row.get('metric', metric),
                        num_files=num_files,
                        num_ops=int(row.get('count', 0) or 0),
                        num_processes=num_processes,
                        num_time_periods=num_time_periods,
                        subject=row.get('subject', ''),
                        time=float(row.get('time', 0) or 0),
                        time_overall=float(row.get('time_overall', 0) or 0),
                        view_type=view_type,
                    ),
                    reasons=_reasons_for(row, bottleneck_rules),
                )
            )

        described.sort(key=lambda item: (-item.severity, item.id))
        shown = described[:max_bottlenecks] if max_bottlenecks > 0 else described

        views.append(
            BottleneckView(
                view_key=view_key,
                # 'Time Period' reads as 'Time View' once 'View' is appended.
                name=f"{humanized_view_name(view_key, ' > ').replace(' Period', '')} View",
                num_bottlenecks=len(rows),
                num_reasons=num_reasons,
                bottlenecks=shown,
                num_hidden=len(described) - len(shown),
            )
        )

    return views
