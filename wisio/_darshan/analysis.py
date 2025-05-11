import darshan
import pandas as pd
from datetime import datetime
from ..constants import PROC_NAME_SEPARATOR, IOCategory


def create_dxt_dataframe(trace_path: str, time_granularity=1e3):
    """
    Create a DataFrame from Darshan DXT_POSIX trace data with optimized performance.
    
    Args:
        trace_path: Path to the Darshan trace file
        time_granularity: Time granularity for time_range calculation
        
    Returns:
        Tuple of (DataFrame with DXT data, job execution time)
    """
    # Load the darshan report
    report = darshan.DarshanReport(trace_path, read_all=True)

    # Calculate job time
    job = report.metadata['job']
    if 'start_time' in job:
        job_time = job['end_time'] - job['start_time']
    else:
        job_time = job['end_time_sec'] - job['start_time_sec']
    
    # Get the DXT_POSIX records
    dxt_posix = pd.DataFrame(report.records['DXT_POSIX'].to_df())
    
    # Initialize data structures
    data_rows = []
    
    # Process each record
    for _, record in dxt_posix.iterrows():
        file_id = record['id']
        rank = record['rank']
        hostname = record['hostname']
        file_name = report.data['name_records'][file_id]
        proc_name = f"app#localhost#{rank}#0"
        
        # Process read segments
        if not record['read_segments'].empty:
            read_segments = record['read_segments']
            
            # Convert dataframe to dict of lists for faster processing
            lengths = read_segments['length'].tolist()
            start_times = read_segments['start_time'].tolist()
            end_times = read_segments['end_time'].tolist()
            offsets = read_segments['offset'].tolist()
            
            # Create a batch of rows
            for i in range(len(lengths)):
                data_rows.append({
                    'file_name': file_name,
                    'proc_name': proc_name,
                    'size': lengths[i],
                    'end_time': end_times[i],
                    'start_time': start_times[i],
                    'func_id': 'read',
                    'hostname': hostname,
                    'io_cat': IOCategory.READ.value,
                    'time_range': int(start_times[i] * time_granularity),
                    'cat': 0,
                    'acc_pat': 0,  # Would need more logic for random access patterns
                    'count': 1,
                    'time': end_times[i] - start_times[i]
                })
        
        # Process write segments
        if not record['write_segments'].empty:
            write_segments = record['write_segments']
            
            # Convert dataframe to dict of lists for faster processing
            lengths = write_segments['length'].tolist()
            start_times = write_segments['start_time'].tolist()
            end_times = write_segments['end_time'].tolist()
            offsets = write_segments['offset'].tolist()
            
            # Create a batch of rows
            for i in range(len(lengths)):
                data_rows.append({
                    'file_name': file_name,
                    'proc_name': proc_name,
                    'size': lengths[i],
                    'end_time': end_times[i],
                    'start_time': start_times[i],
                    'func_id': 'write',
                    'hostname': hostname,
                    'io_cat': IOCategory.WRITE.value,
                    'time_range': int(start_times[i] * time_granularity),
                    'cat': 0,
                    'acc_pat': 0,  # Would need more logic for random access patterns
                    'count': 1,
                    'time': end_times[i] - start_times[i]
                })
    
    # Create the final dataframe in one go
    if data_rows:
        dxt_posix_df = pd.DataFrame(data_rows)
    else:
        # Empty dataframe with the required columns
        dxt_posix_df = pd.DataFrame(columns=[
            'file_name', 'proc_name', 'size', 'end_time', 'start_time', 'func_id',
            'hostname', 'io_cat', 'time_range', 'cat', 'acc_pat', 'count', 'time'
        ])
    
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
