import numpy as np
from enum import Enum


class AccessPattern(Enum):
    SEQUENTIAL = 0
    RANDOM = 1


class IOCategory(Enum):
    READ = 1
    WRITE = 2
    METADATA = 3


COL_APP_NAME = 'app_name'
COL_FILE_DIR = 'file_dir'
COL_FILE_NAME = 'file_name'
COL_FILE_PATTERN = 'file_pattern'
COL_HOST_NAME = 'host_name'
COL_NODE_NAME = 'node_name'
COL_PROC_NAME = 'proc_name'
COL_RANK = 'rank'
COL_TIME_RANGE = 'time_range'

VIEW_TYPES = ['time_range', 'file_name', 'proc_name']  # Order matters!
LOGICAL_VIEW_TYPES = [('proc_name', 'app_name'), ('proc_name', 'node_name'), (
    'proc_name', 'rank'), ('file_name', 'file_dir'), ('file_name', 'file_pattern')]

ACC_PAT_SUFFIXES = ['time', 'size', 'count']
DERIVED_MD_OPS = ['close', 'open', 'seek', 'stat']
IO_CATS = [io_cat.value for io_cat in list(IOCategory)]
IO_TYPES = ['read', 'write', 'metadata']

FILE_PATTERN_PLACEHOLDER = '[0-9]'
PROC_NAME_SEPARATOR = '#'

HUMANIZED_VIEW_TYPES = dict(
    app_name='App',
    file_dir='File Directory',
    file_name='File',
    file_pattern='File Pattern',
    node_name='Node',
    proc_name='Process',
    rank='Rank',
    time_range='Time Range',
)

XFER_SIZE_BINS = [
    -np.inf,
    4 * 1024.0,
    16 * 1024.0,
    64 * 1024.0,
    256 * 1024.0,
    1 * 1024.0 * 1024.0,
    4 * 1024.0 * 1024.0,
    16 * 1024.0 * 1024.0,
    64 * 1024.0 * 1024.0,
    np.inf
]
XFER_SIZE_BIN_LABELS = [
    '<4 KB',
    '4-16 KB',
    '16-64 KB',
    '64-256 KB',
    '256 KB-1 MB',
    '1-4 MB',
    '4-16 MB',
    '16-64 MB',
    '>64 MB',
]
XFER_SIZE_BIN_NAMES = [
    '<4 KB',
    '4 KB',
    '16 KB',
    '64 KB',
    '256 KB',
    '1 MB',
    '4 MB',
    '16 MB',
    '64 MB',
    '>64 MB'
]

EVENT_ATT_REASONS = 'attach_reasons'
EVENT_COMP_HLM = 'compute_hlm'
EVENT_COMP_MAIN_VIEW = 'compute_main_view'
EVENT_COMP_METBD = 'compute_metric_boundaries'
EVENT_COMP_PERS = 'compute_perspectives'
EVENT_DET_BOTT = 'detect_bottlenecks'
EVENT_READ_TRACES = 'read_traces'
