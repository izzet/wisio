import random
from anytree import NodeMixin
from typing import List
from vani.common.observations import Observation


class Hypothesis(NodeMixin):

    def __init__(self, desc: str, observations: List[Observation], score: float, weighted_score: float, parent=None) -> None:
        self.desc = desc
        self.observations = observations
        self.parent = parent
        self.score = score
        self.weighted_score = weighted_score

    def start_desc(self) -> str:
        starters = [f"During {self.desc},", f"In {self.desc},", f"Within the scope of {self.desc},"]
        if "-" in self.desc:
            starters.append(f"In the range of {self.desc},")
        return random.choice(starters)

    def reason_desc(self) -> str:
        reasons = []
        for observation in self.observations:
            if observation.label > 5:
                reasons.append(observation.desc)
        cleaned_reasons = []
        for index, reason in enumerate(reasons):
            if index == len(reasons) - 1:
                cleaned_reasons.append(f"and {reason}")
            else:
                cleaned_reasons.append(reason)
        start = random.choice(["because the application", "as the application"])
        return f"{start} {', '.join(cleaned_reasons)}"
