from vani.common.constants import PERCENTAGE_FORMAT


class Observation(object):

    def __init__(self, name: str, desc: str, label: int, value: float, score: float, formatted_value: str = None) -> None:
        self.desc = desc
        self.label = label
        self.name = name
        self.score = score
        self.value = value
        self.formatted_value = formatted_value
        if not formatted_value:
            self.formatted_value = str(value)

    def __repr__(self) -> str:
        return f"{self.name}={str(self.label)} {self.formatted_value} {PERCENTAGE_FORMAT.format(self.score * 100)}"
