import dask
import dask.bag as db
import json
import logging
import numpy as np
import os
import zindex_py as zindex
from dask import delayed
from glob import glob
from typing import List

from .analyzer import Analyzer
from .config import AnalysisConfig, CheckpointConfig, ClusterConfig, OutputConfig
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
    EVENT_READ_TRACES,
    IOCategory,
)
from .types import AnalysisAccuracy, RawStats, ViewType
from .utils.dask_utils import EventLogger


CAT_POSIX = 'POSIX'
CHECKPOINT_RAW_STATS = '_raw_stats'
DFTRACER_TIME_RESOLUTION = 1e6
PFW_COL_MAPPING = {
    'name': COL_FUNC_ID,
    'dur': COL_TIME,
    'hostname': COL_HOST_NAME,
    'filename': COL_FILE_NAME,
}


def _create_index(filename):
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


def _load_objects(line, time_granularity, time_approximate, condition_fn):
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
            val = json.loads(line)
            logging.debug(f"Loading dict {val}")
            if "name" in val and "ts" in val:
                val["ts"] = int(val["ts"])
                if val["ts"] > 0:
                    d["name"] = val["name"]
                    d["cat"] = val["cat"]
                    d["pid"] = int(val["pid"])
                    d["tid"] = int(val["tid"])
                    d["ts"] = float(val["ts"])
                    d["dur"] = float(val["dur"])
                    d["te"] = d["ts"] + d["dur"]
                    # if not time_approximate:
                    #     d["tinterval"] = I.to_string(I.closed(val["ts"] , val["ts"] + val["dur"]))
                    # d["trange"] = int(((d["ts"] + d["dur"]) / 2.0))
                    d.update(_io_function(val, d, time_approximate, condition_fn))
                    logging.debug(f"built an dictionary for line {d}")
        except ValueError as error:
            logging.error(f"Processing {line} failed with {error}")
    return d


def _generate_line_batches(filename, max_line):
    batch_size = 1024 * 16
    for start in range(0, max_line, batch_size):
        end = min((start + batch_size - 1), (max_line - 1))
        logging.debug(f"Created a batch for {filename} from [{start}, {end}] lines")
        yield filename, start, end


def _get_conditions_default(json_obj):
    io_cond = "POSIX" == json_obj["cat"]
    return False, False, io_cond


def _get_line_number(filename):
    index_file = f"{filename}.zindex"
    line_number = zindex.get_max_line(
        filename,
        index_file=index_file,
        debug=False,
        verbose=False,
    )
    logging.debug(f" The {filename} has {line_number} lines")
    return (filename, line_number)


def _get_size(filename):
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


def _io_columns(time_approximate=True):
    return {
        'hostname': "string",
        'compute_time': "string" if not time_approximate else np.float64,
        'io_time': "string" if not time_approximate else np.float64,
        'app_io_time': "string" if not time_approximate else np.float64,
        'total_time': "string" if not time_approximate else np.float64,
        'filename': "string",
        'phase': np.int16,
        'size': np.int64,
    }


def _io_function(json_object, current_dict, time_approximate, condition_fn):
    d = {}
    d['app_io_time'] = 0
    d['compute_time'] = 0
    d['filename'] = ''
    d['io_time'] = 0
    d['phase'] = 0
    d['size'] = 0
    d['total_time'] = 0
    if not condition_fn:
        condition_fn = _get_conditions_default
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
    # else:
    #     if compute_cond:
    #         d["compute_time"] = current_dict["tinterval"]
    #         d["total_time"] = current_dict["tinterval"]
    #         d["phase"] = 1
    #     elif io_cond:
    #         d["io_time"] = current_dict["tinterval"]
    #         d["total_time"] = current_dict["tinterval"]
    #         d["phase"] = 2
    #     elif app_io_cond:
    #         d["app_io_time"] = current_dict["tinterval"]
    #         d["total_time"] = current_dict["tinterval"]
    #         d["phase"] = 3
    #     else:
    #         d["total_time"] = I.to_string(I.empty())
    #         d["io_time"] = I.to_string(I.empty())
    if "args" in json_object:
        if "fname" in json_object["args"]:
            d["filename"] = json_object["args"]["fname"]
        if "hostname" in json_object["args"]:
            d["hostname"] = json_object["args"]["hostname"]

        if "POSIX" == json_object["cat"] and "ret" in json_object["args"]:
            if "write" in json_object["name"]:
                d["size"] = int(json_object["args"]["ret"])
            elif "read" in json_object["name"] and "readdir" not in json_object["name"]:
                d["size"] = int(json_object["args"]["ret"])
        else:
            if "image_size" in json_object["args"]:
                d["size"] = int(json_object["args"]["image_size"])
    return d


def _load_indexed_gzip_files(filename, start, end):
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


class DFTracerAnalyzer(Analyzer):
    def __init__(
        self,
        analysis_config: AnalysisConfig,
        checkpoint_config: CheckpointConfig,
        cluster_config: ClusterConfig,
        output_config: OutputConfig,
        debug=False,
        verbose=False,
    ):
        super().__init__(
            name='DFTracer',
            analysis_config=analysis_config,
            checkpoint_config=checkpoint_config,
            cluster_config=cluster_config,
            output_config=output_config,
            debug=debug,
            verbose=verbose,
        )

    def analyze_pfw(
        self,
        trace_path: str,
        accuracy: AnalysisAccuracy = 'pessimistic',
        exclude_bottlenecks: List[str] = [],
        exclude_characteristics: List[str] = [],
        logical_view_types: bool = False,
        metrics=['duration'],
        threshold: int = 45,
        time_granularity: int = 1e6,
        view_types: List[ViewType] = ['file_name', 'proc_name', 'time_range'],
    ):
        # Read traces
        with EventLogger(key=EVENT_READ_TRACES, message='Read traces'):
            traces = self.read_pfw(
                trace_path=trace_path,
                time_granularity=time_granularity,
            )

        job_time = traces['te'].max() - traces['ts'].min()

        # Prepare raw stats
        raw_stats = self.restore_extra_data(
            name=CHECKPOINT_RAW_STATS,
            fallback=lambda: dict(
                job_time=delayed(job_time),
                time_granularity=time_granularity,
                total_count=traces.index.count().persist(),
            ),
        )

        # Analyze traces
        return self.analyze_traces(
            accuracy=accuracy,
            exclude_bottlenecks=exclude_bottlenecks,
            exclude_characteristics=exclude_characteristics,
            logical_view_types=logical_view_types,
            metrics=metrics,
            raw_stats=RawStats(**raw_stats),
            threshold=threshold,
            traces=traces,
            view_types=view_types,
        )

    def read_pfw(self, trace_path: str, time_granularity: int, time_approximate=True):
        trace_paths = glob(trace_path)
        all_files = []
        pfw_pattern = []
        pfw_gz_pattern = []
        for trace_path in trace_paths:
            if trace_path.endswith('.pfw'):
                pfw_pattern.append(trace_path)
                all_files.append(trace_path)
            elif trace_path.endswith('.pfw.gz'):
                pfw_gz_pattern.append(trace_path)
                all_files.append(trace_path)
            else:
                logging.warn(f"Ignoring unsuported file {trace_path}")
        if len(all_files) == 0:
            logging.error("No files selected for .pfw and .pfw.gz")
            exit(1)
        logging.debug(f"Processing {all_files}")
        if len(pfw_gz_pattern) > 0:
            db.from_sequence(pfw_gz_pattern).map(_create_index).compute()
            logging.info(f"Created index for {len(pfw_gz_pattern)} files")
        total_size = db.from_sequence(all_files).map(_get_size).sum().compute()
        logging.info(f"Total size of all files are {total_size} bytes")
        pfw_bag = None
        pfw_gz_bag = None
        if len(pfw_gz_pattern) > 0:
            max_line_numbers = (
                db.from_sequence(pfw_gz_pattern).map(_get_line_number).compute()
            )
            logging.debug(f"Max lines per file are {max_line_numbers}")
            json_line_delayed = []
            total_lines = 0
            for filename, max_line in max_line_numbers:
                total_lines += max_line
                for _, start, end in _generate_line_batches(filename, max_line):
                    json_line_delayed.append((filename, start, end))

            logging.info(
                f"Loading {len(json_line_delayed)} batches out of {len(pfw_gz_pattern)} files and has {total_lines} lines overall"
            )
            json_line_bags = []
            for filename, start, end in json_line_delayed:
                num_lines = end - start + 1
                json_line_bags.append(
                    dask.delayed(_load_indexed_gzip_files, nout=num_lines)(
                        filename, start, end
                    )
                )
            json_lines = db.concat(json_line_bags)
            pfw_gz_bag = json_lines.map(
                _load_objects,
                time_granularity=time_granularity,
                time_approximate=time_approximate,
                condition_fn=None,
            ).filter(lambda x: "name" in x)
        main_bag = None
        if len(pfw_pattern) > 0:
            pfw_bag = (
                db.read_text(pfw_pattern)
                .map(
                    _load_objects,
                    time_granularity=time_granularity,
                    time_approximate=time_approximate,
                    condition_fn=None,
                )
                .filter(lambda x: "name" in x)
            )
        if len(pfw_gz_pattern) > 0 and len(pfw_pattern) > 0:
            main_bag = db.concat([pfw_bag, pfw_gz_bag])
        elif len(pfw_gz_pattern) > 0:
            main_bag = pfw_gz_bag
        elif len(pfw_pattern) > 0:
            main_bag = pfw_bag
        if main_bag:
            columns = {
                'name': "string",
                'cat': "string",
                'pid': np.int64,  # 'Int64',
                'tid': np.int64,  # 'Int64',
                'ts': np.float64,  # 'Int64',
                'te': np.float64,  # 'Int64',
                'dur': np.float64,  # 'Int64',
                # 'tinterval': "string" if not time_approximate else np.int64, # 'Int64',
                # 'trange': np.float64,  # 'Int64'
            }
            columns.update(_io_columns())
            # columns.update(load_cols)
            events = main_bag.to_dataframe(meta=columns)
            events = events[(events['cat'] == CAT_POSIX) & (events['ts'] > 0)]
            # self.n_partition = math.ceil(total_size.compute() / (128 * 1024 ** 2))
            # logging.debug(f"Number of partitions used are {self.n_partition}")
            # self.events = events.repartition('256MB').persist()
            # _ = wait(self.events)
            events['dur'] = events['dur'] / DFTRACER_TIME_RESOLUTION
            events['ts'] = (
                events['ts'] - events['ts'].min()
            ) / DFTRACER_TIME_RESOLUTION
            events['te'] = events['ts'] + events['dur']
            events[COL_TIME_RANGE] = (
                ((events['te'] / time_granularity) * DFTRACER_TIME_RESOLUTION)
                .round()
                .astype(int)
            )
            # self.events = self.events.persist()
            # _ = wait(self.events)
        else:
            logging.error("Unable to load traces")
            exit(1)

        events[COL_PROC_NAME] = (
            'app#'
            + events['hostname']
            + '#'
            + events['pid'].astype(str)
            + '#'
            + events['tid'].astype(str)
        )

        # ddf[col_name] = ddf[col_name].mask(ddf['func_id'].str.contains(
        # md_op) & ~ddf['func_id'].str.contains('dir'), ddf[col])

        read_cond = 'read'
        write_cond = 'write'
        metadata_cond = 'readlink'

        events[COL_ACC_PAT] = 0
        events[COL_COUNT] = 1
        events[COL_IO_CAT] = 0
        events[COL_IO_CAT] = events[COL_IO_CAT].mask(
            (events['cat'] == 'POSIX')
            & ~events['name'].str.contains(read_cond)
            & ~events['name'].str.contains(write_cond),
            IOCategory.METADATA.value,
        )
        events[COL_IO_CAT] = events[COL_IO_CAT].mask(
            (events['cat'] == 'POSIX')
            & events['name'].str.contains(read_cond)
            & ~events['name'].str.contains(metadata_cond),
            IOCategory.READ.value,
        )
        events[COL_IO_CAT] = events[COL_IO_CAT].mask(
            (events['cat'] == 'POSIX')
            & events['name'].str.contains(write_cond)
            & ~events['name'].str.contains(metadata_cond),
            IOCategory.WRITE.value,
        )

        return events.rename(columns=PFW_COL_MAPPING)
