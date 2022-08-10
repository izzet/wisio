import re


def to_snake_case(value: str):
    value = value.replace(' ', '').replace('-', '')
    return re.sub(r'(?<!^)(?=[A-Z])', '_', value).lower()
