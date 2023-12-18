import darshan
import pandas as pd
from datetime import datetime
from ..constants import PROC_NAME_SEPARATOR, IOCategory


def create_dxt_dataframe(trace_path: str, time_granularity=1e3):

    rep = darshan.DarshanReport(trace_path, read_all=True)

    job = rep.metadata['job']
    if 'start_time' in job:
        job_time = job['end_time'] - job['start_time']
    else:
        job_time = job['end_time_sec'] - job['start_time_sec']

    df = rep.records['DXT_POSIX'].to_df()

    dxt_posix = pd.DataFrame(df)

    read_id = []
    read_rank = []
    read_length = []
    read_offsets = []
    read_end_time = []
    read_start_time = []
    read_operation = []
    read_hostname = []
    read_io_cat = []

    write_id = []
    write_rank = []
    write_length = []
    write_offsets = []
    write_end_time = []
    write_start_time = []
    write_operation = []
    write_hostname = []
    write_io_cat = []

    # start = time.time()
    for r in zip(dxt_posix['rank'], dxt_posix['read_segments'], dxt_posix['write_segments'], dxt_posix['id'], dxt_posix['hostname']):
        if not r[1].empty:
            read_id.append([r[3]] * len((r[1]['length'].to_list())))
            read_rank.append([r[0]] * len((r[1]['length'].to_list())))
            read_length.append(r[1]['length'].to_list())
            read_end_time.append(r[1]['end_time'].to_list())
            read_start_time.append(r[1]['start_time'].to_list())
            read_operation.append(['read'] * len((r[1]['length'].to_list())))
            read_offsets.append(r[1]['offset'].to_list())
            read_hostname.append([r[4]] * len((r[1]['length'].to_list())))
            read_io_cat.append([2]*len((r[1]['length'].to_list())))

        if not r[2].empty:
            write_id.append([r[3]] * len((r[2]['length'].to_list())))     
            write_rank.append([r[0]] * len((r[2]['length'].to_list())))
            write_length.append(r[2]['length'].to_list())
            write_end_time.append(r[2]['end_time'].to_list())
            write_start_time.append(r[2]['start_time'].to_list())
            write_operation.append(['write'] * len((r[2]['length'].to_list())))
            write_offsets.append(r[2]['offset'].to_list())
            write_hostname.append([r[4]] * len((r[2]['length'].to_list())))
            write_io_cat.append([1]*len((r[2]['length'].to_list())))

    file_name = [rep.data['name_records'][element] for nestedlist in read_id for element in nestedlist]
    rank = [f"app#localhost#{element}#0" for nestedlist in read_rank for element in nestedlist]
    length = [element for nestedlist in read_length for element in nestedlist]
    offsets = [element for nestedlist in read_offsets for element in nestedlist]
    end_time = [element for nestedlist in read_end_time for element in nestedlist]
    operation = [element for nestedlist in read_operation for element in nestedlist]
    start_time = [element for nestedlist in read_start_time for element in nestedlist]
    time_range = [int(element*time_granularity) for nestedlist in read_start_time for element in nestedlist]
    hostname = [element for nestedlist in read_hostname for element in nestedlist]
    io_cat = [element for nestedlist in read_io_cat for element in nestedlist]

    file_name.extend([rep.data['name_records'][element] for nestedlist in write_id for element in nestedlist])
    rank.extend([f"app#localhost#{element}#0" for nestedlist in write_rank for element in nestedlist])
    length.extend([element for nestedlist in write_length for element in nestedlist])
    offsets.extend([element for nestedlist in write_offsets for element in nestedlist])
    end_time.extend([element for nestedlist in write_end_time for element in nestedlist])
    operation.extend([element for nestedlist in write_operation for element in nestedlist])
    start_time.extend([element for nestedlist in write_start_time for element in nestedlist])
    time_range.extend([int(element*time_granularity) for nestedlist in write_start_time for element in nestedlist])
    hostname.extend([element for nestedlist in write_hostname for element in nestedlist])
    io_cat.extend([element for nestedlist in write_io_cat for element in nestedlist])

    dxt_posix_df = pd.DataFrame(
        {
        'file_name': file_name,
        'proc_name': rank,
        'size': length,
        'end_time': end_time,
        'start_time': start_time,
        'func_id': operation,
        # 'offsets': offsets,
        'hostname': hostname,
        'io_cat': io_cat,
        'time_range': time_range,
        })

    dxt_posix_df['cat'] = 0
    dxt_posix_df['acc_pat'] = 0
    dxt_posix_df['count'] = 1
    dxt_posix_df['time'] = dxt_posix_df['end_time'] - dxt_posix_df['start_time']

    return dxt_posix_df, job_time  


def generate_dxt_records(trace_path: str, time_granularity=1e3):
    def get_dict(row):
        d = {}
        d["size"] = row["length"]
        start_time = row['start_time']
        d["time"] = row["end_time"] - start_time
        d["time_range"] = int(
            (((start_time * 1e6) + (d["time"] * 1e6)/2.0) / time_granularity))
        return d
    report = darshan.DarshanReport(trace_path, read_all=True)
    if "DXT_POSIX" in report.modules:
        t0 = datetime.now()
        df = report.records['DXT_POSIX'].to_df()
        
        print('records',  datetime.now()-t0)
        for val in df.iterrows():
            d = {}

            file_id = val["id"]
            pid = val["rank"]
            tid = 0

            d["hostname"] = val["hostname"]
            d["cat"] = 0  # POSIX
            d["file_name"] = report.data['name_records'][file_id]
            d["proc_name"] = PROC_NAME_SEPARATOR.join(
                ['app', d['hostname'], f"{pid}", f"{tid}"])
            d["acc_pat"] = 0
            d["io_cat"] = IOCategory.METADATA.value

            write_segments = val["write_segments"]
            write_offset = None
            # t0 = datetime.now()
            for _, row in write_segments.iterrows():
                if write_offset is not None and write_offset > row['offset']:
                    d["acc_pat"] = 1
                write_offset = row['offset'] + row['length']
                d["count"] = 1
                d["func_id"] = "write"
                d["io_cat"] = IOCategory.WRITE.value
                d.update(get_dict(row))

                yield dict(**d)
            # print('write_segments', datetime.now() - t0)

            read_segments = val["read_segments"]
            read_offset = None
            # t1 = datetime.now()
            for _, row in read_segments.iterrows():
                if read_offset is not None and read_offset > row['offset']:
                    d["acc_pat"] = 1
                read_offset = row['offset'] + row['length']
                d["count"] = 1
                d["func_id"] = "read"
                d["io_cat"] = IOCategory.READ.value
                d.update(get_dict(row))
                yield dict(**d)
            # print('read_segments', datetime.now() - t1)
