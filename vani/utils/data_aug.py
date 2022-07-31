from dask.dataframe import DataFrame


def set_bandwidths(ddf: DataFrame, duration_column_name='duration'):
    correct_dur = ((ddf['tend'] - ddf['tstart']) > 0)
    ddf['bandwidth'] = 0
    ddf['bandwidth'] = ddf['bandwidth'].mask(correct_dur, ddf['size'] / ddf[duration_column_name])


def set_durations(io_df: DataFrame, duration_column_name='duration', time_start_column_name='tstart', time_end_column_name='tend'):
    # Set durations according tend - tstart
    io_df[duration_column_name] = io_df[time_end_column_name] - io_df[time_start_column_name]
    io_df['tmid'] = (io_df[time_end_column_name] + io_df[time_start_column_name])/2.0


def set_filenames(io_df: DataFrame):
    # Prepare conditions
    read_condition, write_condition = _read_write_cond_io_df(io_df)
    open_condition = io_df['func_id'].str.contains("open")
    mpi_condition = io_df['func_id'].str.contains("MPI")
    fread_condition = io_df['func_id'].isin(["fread"])
    close_condition = io_df['func_id'].str.contains('close')
    fwrite_condition = io_df['func_id'].isin(["fwrite"])
    readdir_condition = io_df['func_id'].isin(["readdir"])
    # Then set corresponding filenames
    io_df['filename'] = ""
    io_df['filename'] = io_df['filename'].mask(open_condition & ~mpi_condition, io_df['args_1'])
    io_df['filename'] = io_df['filename'].mask(open_condition & mpi_condition, io_df['args_2'])
    io_df['filename'] = io_df['filename'].mask(close_condition, io_df['args_1'])
    io_df['filename'] = io_df['filename'].mask(read_condition, io_df['args_1'])
    io_df['filename'] = io_df['filename'].mask(fread_condition, io_df['args_4'])
    io_df['filename'] = io_df['filename'].mask(write_condition, io_df['args_1'])
    io_df['filename'] = io_df['filename'].mask(fwrite_condition, io_df['args_4'])
    # Lastly, fix slashes
    io_df['filename'] = io_df['filename'].str.replace('//', '/')


def set_sizes_counts(io_df_read_write: DataFrame):
    # Prepare conditions
    read_condition, fread_condition, write_condition, fwrite_condition = _read_write_cond_io_df_ext(io_df_read_write)
    readdir_condition = io_df_read_write['func_id'].isin(["readdir"])
    # Then set corresponding sizes & counts
    io_df_read_write['size'] = 0
    io_df_read_write['count'] = 1
    io_df_read_write['size'] = io_df_read_write['size'].mask(read_condition, io_df_read_write['args_3'])
    io_df_read_write['size'] = io_df_read_write['size'].mask(fread_condition, io_df_read_write['args_3'])
    io_df_read_write['count'] = io_df_read_write['count'].mask(fread_condition, io_df_read_write['args_2'])
    io_df_read_write['size'] = io_df_read_write['size'].mask(write_condition, io_df_read_write['args_3'])
    io_df_read_write['size'] = io_df_read_write['size'].mask(fwrite_condition, io_df_read_write['args_3'])
    io_df_read_write['count'] = io_df_read_write['count'].mask(fwrite_condition, io_df_read_write['args_2'])
    # Handle corner cases
    io_df_read_write['size'] = io_df_read_write['size'].mask(readdir_condition, "0")
    io_df_read_write['count'] = io_df_read_write['count'].mask(readdir_condition, "1")
    # Set data types
    io_df_read_write[['size', 'count']] = io_df_read_write[['size', 'count']].astype(float)


def _read_write_cond_io_df(io_df: DataFrame):
    # Prepare conditions
    read_condition = io_df['func_id'].isin(["read", "pread", "pread64", "readv"])
    write_condition = io_df['func_id'].isin(["write", "pwrite", "pwrite64", "writev"])
    # Return conditions
    return read_condition, write_condition


def _read_write_cond_io_df_ext(io_df: DataFrame):
    # Prepare conditions
    read_condition = io_df['func_id'].isin(["read", "pread", "pread64", "readv"])
    fread_condition = io_df['func_id'].isin(["fread"])
    write_condition = io_df['func_id'].isin(["write", "pwrite", "pwrite64", "writev"])
    fwrite_condition = io_df['func_id'].isin(["fwrite"])
    # Return conditions
    return read_condition, fread_condition, write_condition, fwrite_condition
