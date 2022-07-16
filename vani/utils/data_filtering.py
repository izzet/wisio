from dask.dataframe import DataFrame

from .data_aug import set_durations, set_filenames, set_sizes_counts, set_bandwidths, _read_write_cond_io_df


def filter_non_io_traces(df: DataFrame):
    # Filter non-I/O traces (except for MPI)
    df = df[(df['level'] == 0) | df['cat'].isin([0, 1, 3])]
    # Return dataframe
    return df


def split_io_mpi_trace(df: DataFrame):
    # Split dataframe into I/O, MPI and trace
    io_df = df[df['cat'].isin([0, 1, 3])]
    mpi_df = df[df['cat'] == 2]
    trace_df = df[df['cat'] == 4]
    # Set additional columns
    set_durations(io_df)
    set_filenames(io_df)
    # Return splitted dataframes
    return io_df, mpi_df, trace_df


def split_read_write_metadata(io_df: DataFrame, compute=False):
    # Prepare conditions
    read_condition, write_condition = _read_write_cond_io_df(io_df)
    # Then compute read & write and metadata dataframes
    io_df_read_write = io_df[read_condition | write_condition]
    io_df_metadata = io_df[~read_condition & ~write_condition]
    # Set additional columns
    set_sizes_counts(io_df_read_write)
    set_bandwidths(io_df_read_write)
    # Compute if specified
    if compute:
        io_df_read_write = io_df_read_write.compute()
        io_df_metadata = io_df_metadata.compute()
    # Return computed dataframes
    return io_df_read_write, io_df_metadata
