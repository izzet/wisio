import dask.dataframe as dd
import numpy as np
import pandas as pd
from typing import Dict, List
from .analysis import DELTA_BIN_LABELS
from ..rules import Rule, RuleEngine


EXCESSIVE_IO_TIME_RULES = [
    Rule.TIME_RANGE_EXCESSIVE_IO_TIME,
    Rule.FILE_EXCESSIVE_IO_TIME,
    Rule.PROCESS_EXCESSIVE_IO_TIME,
]


class RecorderRuleEngine(RuleEngine):

    def __init__(
        self,
        rules: List[Rule],
        views: Dict[tuple, dd.DataFrame],
        unique_filenames: Dict[int, dict],
        unique_processes: Dict[int, dict],
    ):
        super().__init__(rules, views)
        self.unique_filenames = unique_filenames
        self.unique_processes = unique_processes

    def process_rules(self, cut=0.5) -> Dict[tuple, pd.DataFrame]:
        # Keep processed rules
        processed_views = {}
        # Run through views
        for view_key, view_dict in self.views.items():
            # Processed rules
            processed_rules = []
            # Run through rules
            for rule in self.rules:
                if rule in EXCESSIVE_IO_TIME_RULES:
                    processed_rules.append(
                        self._process_excessive_io_time(
                            rule=rule,
                            view_dict=view_dict,
                            cut=cut
                        )
                    )

            processed_views[view_key] = dd.concat(processed_rules)

        return processed_views

    def _process_excessive_io_time(self, rule: Rule, view_dict: Dict[str, dd.DataFrame], cut: float):
        # Set groupby
        rule_col = 'trange'
        if rule is Rule.FILE_EXCESSIVE_IO_TIME:
            rule_col = 'file_id'
        elif rule is Rule.PROCESS_EXCESSIVE_IO_TIME:
            rule_col = 'proc_id'

        # Get expanded view
        expanded_view = view_dict['expanded_view']

        # Compute group view
        group_view = expanded_view[expanded_view['duration_cut'] >= cut] \
            .groupby([rule_col]) \
            .agg({
                'duration_pero': sum,
                'duration_score': 'first',
                'duration_sum': sum,
            }) \
            .sort_values('duration_pero', ascending=False) \
            .reset_index()

        # Set rule
        group_view['rule'] = rule.name

        # Create rule view
        rule_view = group_view \
            .groupby(['rule']) \
            .agg({
                'duration_pero': list,
                'duration_score': list,
                'duration_sum': list,
                rule_col: list,
            })

        # Get originator
        def originator_desc(value: int):
            if rule is Rule.TIME_RANGE_EXCESSIVE_IO_TIME:
                return f"between {value - 1}-{value}s"
            elif rule is Rule.FILE_EXCESSIVE_IO_TIME:
                return f"on file {self.unique_filenames[value]['filename']}"
            elif rule is Rule.PROCESS_EXCESSIVE_IO_TIME:
                return f"on rank {self.unique_processes[value]['rank']}"

        # Define reason
        def set_reason(row):
            total_duration_pero = np.sum(row['duration_pero']) * 100
            total_duration_sum = np.sum(row['duration_sum'])

            values = row[rule_col]

            if len(values) == 1:
                value = values[0]
                return (
                    f"The application spends {float(total_duration_sum):.2f}s "
                    f"({float(total_duration_pero):.2f}%) of its I/O time "
                    f"{originator_desc(value)}"
                )

            reasons = []
            for index, value in enumerate(values):
                reasons.append(' '.join([
                    f"{index + 1})",
                    f"{float(row['duration_sum'][index]):.2f}s",
                    f"({float(row['duration_pero'][index] * 100):.2f}%)",
                    originator_desc(value),
                    f"({DELTA_BIN_LABELS[int(row['duration_score'][index])]})"
                ]))

            return (
                f"The application spends {float(total_duration_pero):.2f}% "
                f"of its I/O time {' '.join(reasons)}"
            )

        rule_view['file_id'] = rule_view['file_id'] if 'file_id' in rule_view.columns else None
        rule_view['proc_id'] = rule_view['proc_id'] if 'proc_id' in rule_view.columns else None
        rule_view['trange'] = rule_view['trange'] if 'trange' in rule_view.columns else None

        rule_view['reason'] = rule_view.apply(set_reason, axis=1, meta=str)

        rule_view = rule_view.drop(columns=['duration_pero', 'duration_score', 'duration_sum'])

        return rule_view
