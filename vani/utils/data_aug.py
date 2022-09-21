import numpy as np
from dask.dataframe import DataFrame


def set_bandwidths(ddf: DataFrame, duration_column_name='duration'):
    correct_dur = ((ddf['tend'] - ddf['tstart']) > 0)
    ddf['bandwidth'] = 0
    ddf['bandwidth'] = ddf['bandwidth'].mask(correct_dur, ddf['size'] / ddf[duration_column_name])


def set_durations(ddf: DataFrame, duration_column_name='duration', time_start_column_name='tstart', time_end_column_name='tend'):
    # Set durations according tend - tstart
    ddf[duration_column_name] = ddf[time_end_column_name] - ddf[time_start_column_name]
    ddf['tmid'] = (ddf[time_end_column_name] + ddf[time_start_column_name]) / 2.0


def set_filenames(ddf: DataFrame):
    # Prepare conditions
    read_condition, write_condition = _read_write_cond_io_df(ddf)
    open_condition = ddf['func_id'].str.contains("open")
    mpi_condition = ddf['func_id'].str.contains("MPI")
    fread_condition = ddf['func_id'].isin(["fread"])
    close_condition = ddf['func_id'].str.contains("close")
    fwrite_condition = ddf['func_id'].isin(["fwrite"])
    # TODO(hari): your missing HDF5 APIs
    #  https://github.com/hariharan-devarajan/Recorder/blob/pilgrim/include/recorder.h#L182
    readdir_condition = ddf['func_id'].isin(["readdir"])
    # Then set corresponding filenames
    ddf['filename'] = ""
    ddf['filename'] = ddf['filename'].mask(open_condition & ~mpi_condition, ddf['args_1'])
    ddf['filename'] = ddf['filename'].mask(open_condition & mpi_condition, ddf['args_2'])
    ddf['filename'] = ddf['filename'].mask(close_condition, ddf['args_1'])
    ddf['filename'] = ddf['filename'].mask(read_condition, ddf['args_1'])
    ddf['filename'] = ddf['filename'].mask(fread_condition, ddf['args_4'])
    ddf['filename'] = ddf['filename'].mask(write_condition, ddf['args_1'])
    ddf['filename'] = ddf['filename'].mask(fwrite_condition, ddf['args_4'])
    # Lastly, fix slashes
    ddf['filename'] = ddf['filename'].str.replace('//', '/')


def set_sizes_counts(ddf: DataFrame):
    # Prepare conditions
    read_condition, fread_condition, write_condition, fwrite_condition = _read_write_cond_io_df_ext(ddf)
    # TODO(hari): your missing HDF5 APIs
    #  https://github.com/hariharan-devarajan/Recorder/blob/pilgrim/include/recorder.h#L182
    readdir_condition = ddf['func_id'].isin(["readdir"])
    # Then set corresponding sizes & counts
    ddf['size'] = 0
    ddf['count'] = 1
    ddf['size'] = ddf['size'].mask(read_condition, ddf['args_3'])
    ddf['size'] = ddf['size'].mask(fread_condition, ddf['args_3'])
    ddf['count'] = ddf['count'].mask(fread_condition, ddf['args_2'])
    ddf['size'] = ddf['size'].mask(write_condition, ddf['args_3'])
    ddf['size'] = ddf['size'].mask(fwrite_condition, ddf['args_3'])
    ddf['count'] = ddf['count'].mask(fwrite_condition, ddf['args_2'])
    # Handle corner cases
    ddf['size'] = ddf['size'].mask(readdir_condition, "0")
    ddf['count'] = ddf['count'].mask(readdir_condition, "1")
    # Set data types
    ddf[['size', 'count']] = ddf[['size', 'count']].astype(float)
    # Size fix
    ddf['size'] = ddf['size'] * ddf['count']


def set_xfer_sizes(ddf: DataFrame):
    xfer_sizes = np.multiply([0, 4, 16, 64, 256, 1024, 4*1024, 16*1024], 1024.0)
    
    ddf['xfer_size'] = ""


def _read_write_cond_io_df(ddf: DataFrame):
    # Prepare conditions
    read_condition = ddf['func_id'].isin(["read", "pread", "pread64", "readv"])
    write_condition = ddf['func_id'].isin(["write", "pwrite", "pwrite64", "writev"])
    # Return conditions
    return read_condition, write_condition


def _read_write_cond_io_df_ext(ddf: DataFrame):
    # Prepare conditions
    read_condition = ddf['func_id'].isin(["read", "pread", "pread64", "readv"])
    fread_condition = ddf['func_id'].isin(["fread"])
    write_condition = ddf['func_id'].isin(["write", "pwrite", "pwrite64", "writev"])
    fwrite_condition = ddf['func_id'].isin(["fwrite"])
    # Return conditions
    return read_condition, fread_condition, write_condition, fwrite_condition
