from enum import Enum


class AutoStrEnum(str, Enum):
    """
    StrEnum where enum.auto() returns the field name.
    See https://docs.python.org/3.9/library/enum.html#using-automatic-values
    """

    @staticmethod
    def _generate_next_value_(
        name: str, start: int, count: int, last_values: list
    ) -> str:
        return name
        # Or if you prefer, return lower-case member (it's StrEnum default behavior since Python 3.11):
        # return name.lower()

    def __str__(self) -> str:
        return self.value  # type: ignore
