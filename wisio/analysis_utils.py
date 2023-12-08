import hashlib
import os
import pandas as pd
import re
from typing import Union
from .constants import (
    COL_FILE_NAME,
    COL_PROC_NAME,
    FILE_PATTERN_PLACEHOLDER,
    PROC_NAME_SEPARATOR,
)


def set_file_dir(df: pd.DataFrame):
    return df.assign(file_dir=df.index.get_level_values(COL_FILE_NAME).map(os.path.dirname))


def set_file_pattern(df: pd.DataFrame):
    def _apply_regex(file_name: str):
        return re.sub('[0-9]+', FILE_PATTERN_PLACEHOLDER, file_name)
    return df.assign(file_pattern=df.index.get_level_values(COL_FILE_NAME).map(_apply_regex))


def set_id(ix: Union[tuple, str, int]):
    ix_str = '_'.join(map(str, ix)) if isinstance(ix, tuple) else str(ix)
    return int(hashlib.md5(ix_str.encode()).hexdigest(), 16)


def set_proc_name_parts(df: pd.DataFrame):
    return df \
        .assign(
            proc_name_parts=lambda df: df.index.get_level_values(COL_PROC_NAME).str.split(
                PROC_NAME_SEPARATOR),
            app_name=lambda df: df.proc_name_parts.str[0].astype(str),
            node_name=lambda df: df.proc_name_parts.str[1].astype(str),
            rank=lambda df: df.proc_name_parts.str[2].astype(str),
        ) \
        .drop(columns=['proc_name_parts'])
