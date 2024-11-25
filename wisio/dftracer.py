import dask
import dask.dataframe as dd
import portion as I
import math
import json
import logging
import os
import zindex_py as zindex
from dask.distributed import wait
from glob import glob
from typing import List

from .analysis import set_unoverlapped_times
from .analyzer import Analyzer
from .constants import (
    COL_ACC_PAT,
    COL_COUNT,
    COL_FILE_NAME,
    COL_FUNC_ID,
    COL_HOST_NAME,
    COL_IO_CAT,
    COL_PROC_NAME,
    COL_TIME,
    COL_TIME_RANGE,
    POSIX_IO_CAT_MAPPING,
    IOCategory,
    Layer,
)
from .types import ViewType


CAT_POSIX = 'POSIX'
CAT_STDIO = 'STDIO'
COND_APP = {
    "cat": {"IO"},
    "name": {
        "NPZReader.read_index",
        "TFReader.parse_image",
        "__getitem__",
        "checkpoint",
        "read_index",
    },
}
COND_COMPUTE = {
    "cat": {"compute"},
    "name": {"compute", "cpu"},
}
COND_IO = {
    "cat": {CAT_POSIX, CAT_STDIO},
    "name": set(),
}
DFTRACER_TIME_RESOLUTION = 1e6
IGNORED_CALLS = [
    "DLIOBenchmark.__init__",
    "DLIOBenchmark._train",
    "DLIOBenchmark.initialize",
    "DLIOBenchmark.run",
]
PFW_COL_MAPPING = {
    'name': COL_FUNC_ID,
    'dur': COL_TIME,
    'trange': COL_TIME_RANGE,
}


def create_index(filename):
    index_file = f"{filename}.zindex"
    if not os.path.exists(index_file):
        status = zindex.create_index(
            filename,
            index_file=f"file:{index_file}",
            regex="id:\b([0-9]+)",
            numeric=True,
            unique=True,
            debug=False,
            verbose=False,
        )
        logging.debug(f"Creating Index for {filename} returned {status}")
    return filename


def generate_line_batches(filename, max_line):
    batch_size = 1024 * 16
    for start in range(0, max_line, batch_size):
        end = min((start + batch_size - 1), (max_line - 1))
        logging.debug(f"Created a batch for {filename} from [{start}, {end}] lines")
        yield filename, start, end


def get_linenumber(filename):
    index_file = f"{filename}.zindex"
    line_number = zindex.get_max_line(
        filename,
        index_file=index_file,
        debug=False,
        verbose=False,
    )
    logging.debug(f" The {filename} has {line_number} lines")
    return (filename, line_number)


def get_size(filename):
    if filename.endswith('.pfw'):
        size = os.stat(filename).st_size
    elif filename.endswith('.pfw.gz'):
        index_file = f"{filename}.zindex"
        line_number = zindex.get_max_line(
            filename,
            index_file=index_file,
            debug=False,
            verbose=False,
        )
        size = line_number * 256
    logging.debug(f" The {filename} has {size/1024**3} GB size")
    return int(size)


def get_conditions_default(json_obj):
    io_cond = "POSIX" == json_obj["cat"]
    return False, False, io_cond


def get_conditions_deepspeed(json_obj):
    app_cond = "__getitem__" in json_obj["name"] or "checkpoint" in json_obj["name"]
    io_cond = "POSIX" == json_obj["cat"] or "STDIO" == json_obj["cat"]
    compute_cond = "compute" in json_obj["name"]
    return app_cond, compute_cond, io_cond


def get_conditions_generic(json_obj: dict):
    app_cond = any(
        any(cond in json_obj[prop] for cond in conditions)
        for prop, conditions in COND_APP.items()
    )
    compute_cond = any(
        any(cond in json_obj[prop] for cond in conditions)
        for prop, conditions in COND_COMPUTE.items()
    )
    io_cond = any(
        any(cond in json_obj[prop] for cond in conditions)
        for prop, conditions in COND_IO.items()
    )
    return app_cond, compute_cond, io_cond


def io_columns(time_approximate=True):
    return {
        # "compute_time": "string" if not time_approximate else np.float64,
        # "io_time": "string" if not time_approximate else np.float64,
        # "app_io_time": "string" if not time_approximate else np.float64,
        # "total_time": "string" if not time_approximate else np.float64,
        # "fhash": "string",
        # # "hostname": "string",
        # "phase": np.int16,
        # "size": np.int64,
        'compute_time': "string[pyarrow]"
        if not time_approximate
        else "uint64[pyarrow]",
        'io_time': "string[pyarrow]" if not time_approximate else "uint64[pyarrow]",
        'app_io_time': "string[pyarrow]" if not time_approximate else "uint64[pyarrow]",
        'total_time': "string[pyarrow]" if not time_approximate else "uint64[pyarrow]",
        'fhash': "uint64[pyarrow]",
        'hhash': "uint64[pyarrow]",
        'phase': "uint16[pyarrow]",
        'size': "uint64[pyarrow]",
    }


def io_function(json_object, current_dict, time_approximate, condition_fn):
    d = {}
    d["phase"] = 0
    if not condition_fn:
        # condition_fn = get_conditions_default
        # condition_fn = get_conditions_deepspeed
        condition_fn = get_conditions_generic
    app_io_cond, compute_cond, io_cond = condition_fn(json_object)
    if time_approximate:
        d["total_time"] = 0
        if compute_cond:
            d["compute_time"] = current_dict["dur"]
            d["total_time"] = current_dict["dur"]
            d["phase"] = 1
        elif io_cond:
            d["io_time"] = current_dict["dur"]
            d["total_time"] = current_dict["dur"]
            d["phase"] = 2
        elif app_io_cond:
            d["total_time"] = current_dict["dur"]
            d["app_io_time"] = current_dict["dur"]
            d["phase"] = 3
    else:
        if compute_cond:
            d["compute_time"] = current_dict["tinterval"]
            d["total_time"] = current_dict["tinterval"]
            d["phase"] = 1
        elif io_cond:
            d["io_time"] = current_dict["tinterval"]
            d["total_time"] = current_dict["tinterval"]
            d["phase"] = 2
        elif app_io_cond:
            d["app_io_time"] = current_dict["tinterval"]
            d["total_time"] = current_dict["tinterval"]
            d["phase"] = 3
        else:
            d["total_time"] = I.to_string(I.empty())
            d["io_time"] = I.to_string(I.empty())
    if "args" in json_object:
        if "fhash" in json_object["args"]:
            if type(json_object["args"]["fhash"]) is str:
                d["fhash"] = int(json_object["args"]["fhash"], 16)
            else:
                d["fhash"] = json_object["args"]["fhash"]
        if "POSIX" == json_object["cat"] and "ret" in json_object["args"]:
            size = int(json_object["args"]["ret"])
            if size > 0:
                if "write" in json_object["name"]:
                    d["size"] = size
                elif (
                    "read" in json_object["name"]
                    and "readdir" not in json_object["name"]
                ):
                    d["size"] = size
        else:
            if "image_size" in json_object["args"]:
                size = int(json_object["args"]["image_size"])
                if size > 0:
                    d["size"] = size
    return d


def load_indexed_gzip_files(filename, start, end):
    index_file = f"{filename}.zindex"
    json_lines = zindex.zquery(
        filename,
        index_file=index_file,
        raw=f"select a.line from LineOffsets a where a.line >= {start} AND a.line <= {end};",
        debug=False,
        verbose=False,
    )
    logging.debug(f"Read {len(json_lines)} json lines for [{start}, {end}]")
    return json_lines


# IGNORE_FILES_PREFIX = [
#     "/g/g92/haridev/.nv",
#     "/usr/WS2/haridev/iopp/venvs",

# ]


def load_objects(line, fn, time_granularity, time_approximate, condition_fn, load_data):
    d = {}
    if (
        line is not None
        and line != ""
        and len(line) > 0
        and "[" != line[0]
        and "]" != line[0]
        and line != "\n"
    ):
        val = {}
        try:
            unicode_line = ''.join([i if ord(i) < 128 else '#' for i in line])
            val = json.loads(unicode_line, strict=False)
            logging.debug(f"Loading dict {val}")
            if "name" in val:
                d["name"] = val["name"]
            if "cat" in val:
                d["cat"] = val["cat"]
            if "pid" in val:
                d["pid"] = val["pid"]
            if "tid" in val:
                d["tid"] = val["tid"]
            d["step"] = 0
            if "args" in val:
                if "hhash" in val["args"]:
                    if type(val["args"]["hhash"]) is str:
                        d["hhash"] = int(val["args"]["hhash"], 16)
                    else:
                        d["hhash"] = val["args"]["hhash"]
                if "level" in val["args"]:
                    d["level"] = int(val["args"]["level"])
                if "step" in val["args"]:
                    d["step"] = int(val["args"]["step"])
            if "M" == val["ph"]:
                if d["name"] == "FH":
                    d["type"] = 1  # 1-> file hash
                    if (
                        "args" in val
                        and "name" in val["args"]
                        and "value" in val["args"]
                    ):
                        d["name"] = val["args"]["name"]
                        if type(val["args"]["value"]) is str:
                            d["hash"] = int(val["args"]["value"], 16)
                        else:
                            d["hash"] = val["args"]["value"]
                            # TODO(izzet): maybe add hash here
                elif d["name"] == "HH":
                    d["type"] = 2  # 2-> hostname hash
                    if (
                        "args" in val
                        and "name" in val["args"]
                        and "value" in val["args"]
                    ):
                        d["name"] = val["args"]["name"]
                        if type(val["args"]["value"]) is str:
                            d["hash"] = int(val["args"]["value"], 16)
                        else:
                            d["hash"] = val["args"]["value"]
                elif d["name"] == "SH":
                    d["type"] = 3  # 3-> string hash
                    if (
                        "args" in val
                        and "name" in val["args"]
                        and "value" in val["args"]
                    ):
                        d["name"] = val["args"]["name"]
                        if type(val["args"]["value"]) is str:
                            d["hash"] = int(val["args"]["value"], 16)
                        else:
                            d["hash"] = val["args"]["value"]
                elif d["name"] == "PR":
                    d["type"] = 5  # 5-> process metadata
                    if (
                        "args" in val
                        and "name" in val["args"]
                        and "value" in val["args"]
                    ):
                        d["name"] = val["args"]["name"]
                        if type(val["args"]["value"]) is str:
                            d["hash"] = int(val["args"]["value"], 16)
                        else:
                            d["hash"] = val["args"]["value"]
                else:
                    d["type"] = 4  # 4-> others
                    if (
                        "args" in val
                        and "name" in val["args"]
                        and "value" in val["args"]
                    ):
                        d["name"] = val["args"]["name"]
                        d["value"] = str(val["args"]["value"])
            else:
                d["type"] = 0  # 0->regular event
                if "dur" in val:
                    val["dur"] = int(val["dur"])
                    val["ts"] = int(val["ts"])
                    d["ts"] = val["ts"]
                    d["dur"] = val["dur"]
                    d["te"] = d["ts"] + d["dur"]
                    if not time_approximate:
                        d["tinterval"] = I.to_string(
                            I.closed(val["ts"], val["ts"] + val["dur"])
                        )
                    d["trange"] = int(
                        ((val["ts"] + val["dur"]) / 2.0) / time_granularity
                    )
                d.update(io_function(val, d, time_approximate, condition_fn))
            logging.debug(f"built an dictionary for line {d}")
            yield d
        except ValueError as error:
            logging.error(f"Processing {line} failed with {error}")
    return {}


class DFTracerAnalyzer(Analyzer):
    def additional_high_level_metrics(self):
        columns = {
            'app_io_time': [sum],
            'checkpoint_io_time': [sum],
            'compute_time': [sum],
            'io_time': [sum],
            'read_io_time': [sum],
        }
        column_names = {
            'app_io_time_sum': 'app_io_time',
            'checkpoint_io_time_sum': 'checkpoint_io_time',
            'compute_time_sum': 'compute_time',
            'io_time_sum': 'io_time',
            'read_io_time_sum': 'read_io_time',
        }
        # print("Additional high level metrics:", list(columns.keys()))
        return columns, column_names

    def read_trace(self, trace_path: str) -> dd.DataFrame:
        conditions = None
        load_cols = {}
        load_data = {}
        load_fn = None
        metadata_cols = {}
        if os.path.isdir(trace_path) and '*' not in trace_path:
            trace_path = f"{trace_path}/*.pfw*"
        # ===============================================
        file_pattern = glob(trace_path)
        all_files = []
        pfw_pattern = []
        pfw_gz_pattern = []
        for file in file_pattern:
            if file.endswith('.pfw'):
                pfw_pattern.append(file)
                all_files.append(file)
            elif file.endswith('.pfw.gz'):
                pfw_gz_pattern.append(file)
                all_files.append(file)
            else:
                logging.warn(f"Ignoring unsuported file {file}")
        if len(all_files) == 0:
            logging.error(f"No files selected for .pfw and .pfw.gz")
            exit(1)
        logging.debug(f"Processing files {all_files}")
        delayed_indices = []
        if len(pfw_gz_pattern) > 0:
            dask.bag.from_sequence(pfw_gz_pattern).map(create_index).compute()
        logging.info(f"Created index for {len(pfw_gz_pattern)} files")
        total_size = dask.bag.from_sequence(all_files).map(get_size).sum().compute()
        logging.info(f"Total size of all files are {total_size} bytes")
        gz_bag = None
        pfw_bag = None
        if len(pfw_gz_pattern) > 0:
            max_line_numbers = (
                dask.bag.from_sequence(pfw_gz_pattern).map(get_linenumber).compute()
            )
            logging.debug(f"Max lines per file are {max_line_numbers}")
            json_line_delayed = []
            total_lines = 0
            for filename, max_line in max_line_numbers:
                total_lines += max_line
                for _, start, end in generate_line_batches(filename, max_line):
                    json_line_delayed.append((filename, start, end))

            logging.info(
                f"Loading {len(json_line_delayed)} batches out of {len(pfw_gz_pattern)} files and has {total_lines} lines overall"
            )
            json_line_bags = []
            for filename, start, end in json_line_delayed:
                num_lines = end - start + 1
                json_line_bags.append(
                    dask.delayed(load_indexed_gzip_files, nout=num_lines)(
                        filename, start, end
                    )
                )
            json_lines = dask.bag.concat(json_line_bags)
            gz_bag = (
                json_lines.map(
                    load_objects,
                    fn=load_fn,
                    time_granularity=self.time_granularity,
                    time_approximate=self.time_approximate,
                    condition_fn=conditions,
                    load_data=load_data,
                )
                .flatten()
                .filter(lambda x: "name" in x)
            )
        main_bag = None
        if len(pfw_pattern) > 0:
            pfw_bag = (
                dask.bag.read_text(pfw_pattern)
                .map(
                    load_objects,
                    fn=load_fn,
                    time_granularity=self.time_granularity,
                    time_approximate=self.time_approximate,
                    condition_fn=conditions,
                    load_data=load_data,
                )
                .flatten()
                .filter(lambda x: "name" in x)
            )
        if len(pfw_gz_pattern) > 0 and len(pfw_pattern) > 0:
            main_bag = dask.bag.concat([pfw_bag, gz_bag])
        elif len(pfw_gz_pattern) > 0:
            main_bag = gz_bag
        elif len(pfw_pattern) > 0:
            main_bag = pfw_bag
        if main_bag:
            # columns = {
            #     'name': "string",
            #     'cat': "string",
            #     'pid': np.int64,  # 'Int64',
            #     'tid': np.int64,  # 'Int64',
            #     'ts': np.float64,  # 'Int64',
            #     'te': np.float64,  # 'Int64',
            #     'dur': np.float64,  # 'Int64',
            #     # 'tinterval': "string" if not time_approximate else np.int64, # 'Int64',
            #     # 'trange': np.float64,  # 'Int64'
            # }
            # columns.update(io_columns())
            # # columns.update(load_cols)
            # traces = main_bag.to_dataframe(meta=columns)
            columns = {
                'name': "string[pyarrow]",
                'cat': "string[pyarrow]",
                'type': "uint8[pyarrow]",
                'pid': "uint64[pyarrow]",
                'tid': "uint64[pyarrow]",
                'ts': "uint64[pyarrow]",
                'te': "uint64[pyarrow]",
                'dur': "uint64[pyarrow]",
                'tinterval': "string[pyarrow]"
                if not self.time_approximate
                else "uint64[pyarrow]",
                'trange': "uint64[pyarrow]",
                'level': "uint8[pyarrow]",
                'step': "uint64[pyarrow]",
            }
            columns.update(io_columns())
            columns.update(load_cols)
            file_hash_columns = {
                'name': "string[pyarrow]",
                'hash': "uint64[pyarrow]",
                'pid': "uint64[pyarrow]",
                'tid': "uint64[pyarrow]",
                'hhash': "uint64[pyarrow]",
            }
            hostname_hash_columns = {
                'name': "string[pyarrow]",
                'hash': "uint64[pyarrow]",
                'pid': "uint64[pyarrow]",
                'tid': "uint64[pyarrow]",
                'hhash': "uint64[pyarrow]",
            }
            string_hash_columns = {
                'name': "string[pyarrow]",
                'hash': "uint64[pyarrow]",
                'pid': "uint64[pyarrow]",
                'tid': "uint64[pyarrow]",
                'hhash': "uint64[pyarrow]",
            }
            other_metadata_columns = {
                'name': "string[pyarrow]",
                'value': "string[pyarrow]",
                'pid': "uint64[pyarrow]",
                'tid': "uint64[pyarrow]",
                'hhash': "uint64[pyarrow]",
            }
            if "FH" in metadata_cols:
                file_hash_columns.update(metadata_cols["FH"])
            if "HH" in metadata_cols:
                hostname_hash_columns.update(metadata_cols["HH"])
            if "SH" in metadata_cols:
                string_hash_columns.update(metadata_cols["SH"])
            if "M" in metadata_cols:
                other_metadata_columns.update(metadata_cols["M"])
            columns.update(file_hash_columns)
            columns.update(hostname_hash_columns)
            columns.update(string_hash_columns)
            columns.update(other_metadata_columns)

            self.all_events = main_bag.to_dataframe(meta=columns)
            events = self.all_events.query("type == 0")
            self.file_hash = (
                self.all_events.query("type == 1")[list(file_hash_columns.keys())]
                .groupby('hash')
                .first()
                .persist()
            )
            self.host_hash = (
                self.all_events.query("type == 2")[list(hostname_hash_columns.keys())]
                .groupby('hash')
                .first()
                .persist()
            )
            self.string_hash = (
                self.all_events.query("type == 3")[list(string_hash_columns.keys())]
                .groupby('hash')
                .first()
                .persist()
            )
            self.metadata = self.all_events.query("type == 4")[
                list(other_metadata_columns.keys())
            ].persist()
            self.n_partition = math.ceil(total_size / (128 * 1024**2))
            logging.debug(f"Number of partitions used are {self.n_partition}")
            self.events = events.repartition(npartitions=self.n_partition).persist()
            _ = wait(self.events)
            self.events['ts'] = (self.events['ts'] - self.events['ts'].min()).astype(
                'uint64[pyarrow]'
            )
            self.events['te'] = (self.events['ts'] + self.events['dur']).astype(
                'uint64[pyarrow]'
            )
            self.events['trange'] = (self.events['ts'] // self.time_granularity).astype(
                'uint16[pyarrow]'
            )
            self.events = self.events.persist()
            _ = wait(
                [
                    self.file_hash,
                    self.host_hash,
                    self.string_hash,
                    self.metadata,
                    self.events,
                ]
            )
        else:
            logging.error("Unable to load traces")
            exit(1)
        # ===============================================
        self.events['dur'] = self.events['dur'] / DFTRACER_TIME_RESOLUTION

        file_hashes = self.file_hash[['name']].rename(columns={'name': COL_FILE_NAME})
        host_hashes = self.host_hash.set_index('hhash')[['name']].rename(
            columns={'name': COL_HOST_NAME}
        )

        self.events = self.events.merge(
            file_hashes, how='left', left_on='fhash', right_index=True
        ).drop(columns=['fhash'])
        self.events = self.events.merge(
            host_hashes, how='left', left_on='hhash', right_index=True
        ).drop(columns=['hhash'])

        return self.events.rename(columns=PFW_COL_MAPPING)

    def postread_trace(
        self,
        traces: dd.DataFrame,
        view_types: List[ViewType],
    ) -> dd.DataFrame:
        # ignore the ignored calls
        traces = traces[~traces[COL_FUNC_ID].isin(IGNORED_CALLS)]

        traces['app_io_time'] = traces['app_io_time'] / DFTRACER_TIME_RESOLUTION
        traces['compute_time'] = traces['compute_time'] / DFTRACER_TIME_RESOLUTION
        traces['io_time'] = traces['io_time'] / DFTRACER_TIME_RESOLUTION
        traces['read_io_time'] = 0.0
        traces['read_io_time'] = traces['read_io_time'].mask(
            traces['func_id'].str.contains('__getitem__'), traces['time']
        )
        traces['checkpoint_io_time'] = 0.0
        traces['checkpoint_io_time'] = traces['checkpoint_io_time'].mask(
            traces['func_id'].str.contains('checkpoint'), traces['time']
        )

        traces[COL_ACC_PAT] = 0
        traces[COL_COUNT] = 1

        if COL_PROC_NAME in view_types:
            traces[COL_PROC_NAME] = (
                'app#'
                + traces[COL_HOST_NAME].astype(str)
                + '#'
                + traces['pid'].astype(str)
                + '#'
                + traces['tid'].astype(str)
            )

        traces[COL_IO_CAT] = IOCategory.OTHER.value
        for io_cat, io_funcs in POSIX_IO_CAT_MAPPING.items():
            if io_cat == IOCategory.METADATA:
                traces[COL_IO_CAT] = traces[COL_IO_CAT].mask(
                    (traces['cat'] == CAT_POSIX)
                    & traces[COL_FUNC_ID].str.contains('|'.join(io_funcs)),
                    io_cat.value,
                )
            else:
                metadata_funcs = POSIX_IO_CAT_MAPPING[IOCategory.METADATA]
                traces[COL_IO_CAT] = traces[COL_IO_CAT].mask(
                    (traces['cat'] == CAT_POSIX)
                    & traces[COL_FUNC_ID].str.contains('|'.join(io_funcs))
                    & ~traces[COL_FUNC_ID].str.contains('|'.join(metadata_funcs)),
                    io_cat.value,
                )

        # drop columns that are not needed
        if COL_FILE_NAME not in view_types:
            traces = traces.drop(columns=[COL_FILE_NAME], errors='ignore')
        if COL_HOST_NAME not in view_types:
            traces = traces.drop(columns=[COL_HOST_NAME], errors='ignore')

        return traces

    def compute_job_time(self, traces: dd.DataFrame) -> float:
        return (traces['te'].max() - traces['ts'].min()) / DFTRACER_TIME_RESOLUTION

    def set_layer_columns(self, layer: Layer, hlm: dd.DataFrame) -> dd.DataFrame:
        hlm = super().set_layer_columns(layer, hlm)
        hlm = set_unoverlapped_times(hlm)
        return hlm
