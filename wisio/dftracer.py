import json
import logging
import numpy as np
from dask import delayed
from dask.bag import read_text
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


def _load_objects(line, time_granularity, time_approximate, condition_fn):
    d = {}
    if (
        line is not None
        and line != ""
        and len(line) > 0
        and "[" != line[0]
        and line != "\n"
    ):
        val = {}
        try:
            val = json.loads(line)
            logging.debug(f"Loading dict {val}")
            if "name" in val:
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


def _get_conditions_default(json_obj):
    io_cond = "POSIX" == json_obj["cat"]
    return False, False, io_cond


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
        trace_path_pattern: str,
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
                trace_path_pattern=trace_path_pattern,
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

    def read_pfw(
        self, trace_path_pattern: str, time_granularity: int, time_approximate=True
    ):
        trace_paths = glob(trace_path_pattern)
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

        main_bag = None
        if len(pfw_pattern) > 0:
            main_bag = (
                read_text(pfw_pattern)
                .map(
                    _load_objects,
                    time_granularity=time_granularity,
                    time_approximate=time_approximate,
                    condition_fn=None,
                )
                .filter(lambda x: "name" in x)
            )

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
