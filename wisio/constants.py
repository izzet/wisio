import numpy as np
from enum import Enum, auto
from strenum import StrEnum


class AccessPattern(Enum):
    SEQUENTIAL = 0
    RANDOM = 1


class EventType(StrEnum):
    ATTACH_REASONS = auto()
    COMPUTE_HLM = auto()
    COMPUTE_MAIN_VIEW = auto()
    COMPUTE_METRIC_BOUNDARIES = auto()
    COMPUTE_PERSPECTIVES = auto()
    COMPUTE_VIEW = auto()
    DETECT_BOTTLENECKS = auto()
    DETECT_CHARACTERISTICS = auto()
    EVALUATE_VIEW = auto()
    READ_TRACES = auto()
    SAVE_BOTTLENECKS = auto()
    SAVE_VIEWS = auto()


class IOCategory(Enum):
    READ = 1
    WRITE = 2
    METADATA = 3
    PCTL = 4
    IPC = 5
    OTHER = 6


class Layer(StrEnum):
    APP = auto()
    DATALOADER = auto()
    NETCDF = auto()
    PNETCDF = auto()
    HDF5 = auto()
    MPI = auto()
    POSIX = auto()


COL_ACC_PAT = 'acc_pat'
COL_APP_NAME = 'app_name'
COL_BEHAVIOR = 'behavior'
COL_CATEGORY = 'cat'
COL_COUNT = 'count'
COL_FILE_DIR = 'file_dir'
COL_FILE_NAME = 'file_name'
COL_FILE_PATTERN = 'file_pattern'
COL_FUNC_ID = 'func_id'
COL_HOST_NAME = 'host_name'
COL_IO_CAT = 'io_cat'
COL_NODE_NAME = 'node_name'
COL_PROC_NAME = 'proc_name'
COL_RANK = 'rank'
COL_SIZE = 'size'
COL_TIME = 'time'
COL_TIME_OVERALL = 'time_overall'
COL_TIME_RANGE = 'time_range'


LOGICAL_VIEW_TYPES = [
    ('proc_name', 'app_name'),
    ('proc_name', 'node_name'),
    ('proc_name', 'rank'),
    ('file_name', 'file_dir'),
    ('file_name', 'file_pattern'),
]
VIEW_TYPES = [
    'file_name',
    'proc_name',
    'time_range',
]

ACC_PAT_SUFFIXES = ['time', 'size', 'count']
DERIVED_MD_OPS = ['close', 'open', 'seek', 'stat']
IO_CATS = [io_cat.value for io_cat in list(IOCategory)]
IO_TYPES = ['read', 'write', 'metadata']
COMPACT_IO_TYPES = ['R', 'W', 'M']


# todo(izzet): add mmap
POSIX_IO_CAT_MAPPING = {
    IOCategory.READ: [
        'read',
        'pread',
        'readv',
        'preadv',
    ],
    IOCategory.WRITE: [
        'write',
        'pwrite',
        'writev',
        'pwritev',
    ],
    IOCategory.METADATA: [
        "__fxstat",
        "__fxstat64",
        "__lxstat64",
        "__xstat",
        "__xstat64",
        "lseek64",
        'access',
        'close',
        'closedir',
        'fnctl',
        'fstat',
        'fstatat',
        'mkdir',
        'open',
        'open64',
        'opendir',
        'readdir',
        'readlink',
        'rename',
        'rmdir',
        'seek',
        'stat',
        'unlink',
    ],
    IOCategory.PCTL: [
        'exec',
        'exit',
        'fork',
        'kill',
        'pipe',
        'wait',
    ],
    IOCategory.IPC: [
        'msgctl',
        'msgget',
        'msgrcv',
        'msgsnd',
        'semctl',
        'semget',
        'semop',
        'shmat',
        'shmctl',
        'shmdt',
        'shmget',
    ],
}

FILE_PATTERN_PLACEHOLDER = '[0-9]'
PROC_NAME_SEPARATOR = '#'

HUMANIZED_COLS = dict(
    acc_pat='Access Pattern',
    app_io_time='Application I/O Time',
    app_name='Application',
    behavior='Behavior',
    cat='Category',
    checkpoint_io_time='Checkpoint I/O Time',
    compute_time='Compute Time',
    count='Count',
    file_dir='File Directory',
    file_name='File',
    file_pattern='File Pattern',
    func_id='Function ID',
    host_name='Host',
    io_cat='I/O Category',
    io_time='I/O Time',
    node_name='Node',
    proc_name='Process',
    rank='Rank',
    read_io_time='Read I/O Time',
    size='Size',
    time='Time',
    time_range='Time Period',
    u_app_compute_time='Unoverlapped Application Compute Time',
    u_app_io_time='Unoverlapped Application I/O Time',
    u_checkpoint_io_time='Unoverlapped Checkpoint I/O Time',
    u_compute_time='Unoverlapped Compute Time',
    u_io_time='Unoverlapped I/O Time',
    u_read_io_time='Unoverlapped Read I/O Time',
)
HUMANIZED_METRICS = dict(
    bw='I/O Bandwidth',
    intensity='I/O Intensity',
    iops='I/O Operations per Second',
    time='I/O Time',
)
HUMANIZED_VIEW_TYPES = dict(
    app_name='App',
    file_dir='File Directory',
    file_name='File',
    file_pattern='File Pattern',
    node_name='Node',
    proc_name='Process',
    rank='Rank',
    time_range='Time Period',
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
    np.inf,
]
XFER_SIZE_BIN_LABELS = [
    '<4 kiB',
    '4-16 kiB',
    '16-64 kiB',
    '64-256 kiB',
    '256 kiB-1 MiB',
    '1-4 MiB',
    '4-16 MiB',
    '16-64 MiB',
    '>64 MiB',
]
XFER_SIZE_BIN_NAMES = [
    '<4 kiB',
    '4 kiB',
    '16 kiB',
    '64 kiB',
    '256 kiB',
    '1 MiB',
    '4 MiB',
    '16 MiB',
    '64 MiB',
    '>64 MiB',
]

EVENT_ATT_REASONS = 'attach_reasons'
EVENT_COMP_HLM = 'compute_hlm'
EVENT_COMP_MAIN_VIEW = 'compute_main_view'
EVENT_COMP_METBD = 'compute_metric_boundaries'
EVENT_COMP_PERS = 'compute_perspectives'
EVENT_COMP_ROOT_VIEWS = 'compute_root_views'
EVENT_COMP_VIEW = 'compute_view'
EVENT_DET_BOT = 'detect_bottlenecks'
EVENT_DET_CHAR = 'detect_characteristics'
EVENT_READ_TRACES = 'read_traces'
EVENT_SAVE_BOT = 'save_bottlenecks'
EVENT_SAVE_VIEWS = 'save_views'
