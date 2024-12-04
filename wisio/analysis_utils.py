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


def set_app_name(df: pd.DataFrame):
    return df.assign(
        app_name=lambda df: df.index.get_level_values(COL_PROC_NAME)
        .str.split(PROC_NAME_SEPARATOR)
        .str[0]
        .astype("string[pyarrow]"),
    )


def set_host_name(df: pd.DataFrame):
    return df.assign(
        app_name=lambda df: df.index.get_level_values(COL_PROC_NAME)
        .str.split(PROC_NAME_SEPARATOR)
        .str[1]
        .astype("string[pyarrow]"),
    )


def set_file_dir(df: pd.DataFrame):
    if COL_FILE_NAME not in df.index.names:
        return df
    return df.assign(
        file_dir=df.index.get_level_values(COL_FILE_NAME)
        .map(os.path.dirname)
        .astype("string[pyarrow]"),
    )


def set_file_pattern(df: pd.DataFrame):
    if COL_FILE_NAME not in df.index.names:
        return df

    def _apply_regex(file_name: str):
        return re.sub('[0-9]+', FILE_PATTERN_PLACEHOLDER, file_name)

    return df.assign(
        file_pattern=df.index.get_level_values(COL_FILE_NAME)
        .map(_apply_regex)
        .astype("string[pyarrow]"),
    )


def set_id(ix: Union[tuple, str, int]):
    ix_str = '_'.join(map(str, ix)) if isinstance(ix, tuple) else str(ix)
    return int(hashlib.md5(ix_str.encode()).hexdigest(), 16)


def set_proc_name_parts(df: pd.DataFrame):
    if COL_PROC_NAME not in df.index.names:
        return df

    proc_names = df.index.get_level_values(COL_PROC_NAME)

    first_proc_name_parts = proc_names[0].split(PROC_NAME_SEPARATOR)

    if first_proc_name_parts[0] == 'app':
        return df.assign(
            proc_name_parts=proc_names.str.split(PROC_NAME_SEPARATOR),
            app_name=lambda df: df.proc_name_parts.str[0].astype(str),
            host_name=lambda df: df.proc_name_parts.str[1].astype(str),
            # node_name=lambda df: pd.NA,
            proc_id=lambda df: df.proc_name_parts.str[2].astype(str),
            rank=lambda df: pd.NA,
            thread_id=lambda df: df.proc_name_parts.str[3].astype(str),
        ).drop(columns=['proc_name_parts'])

    return df.assign(
        proc_name_parts=proc_names.str.split(PROC_NAME_SEPARATOR),
        app_name=lambda df: df.proc_name_parts.str[0].astype(str),
        host_name=lambda df: df.proc_name_parts.str[1].astype(str),
        # node_name=lambda df: df.proc_name_parts.str[1].astype(str),
        proc_id=lambda df: pd.NA,
        rank=lambda df: df.proc_name_parts.str[2].astype(str),
        thread_id=lambda df: pd.NA,
    ).drop(columns=['proc_name_parts'])
