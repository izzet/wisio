from dask.dataframe import DataFrame
from typing import Tuple
from vani.utils.data_aug import set_durations, set_filenames, set_sizes_counts, set_bandwidths, _read_write_cond_io_df


def filter_non_io_traces(ddf: DataFrame) -> DataFrame:
    # Filter non-I/O traces (except for MPI)
    ddf = ddf[(ddf['level'] == 0) | ddf['cat'].isin([0, 1, 3])]
    # Return dataframe
    return ddf


def split_io_mpi_trace(ddf: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    # Split dataframe into I/O, MPI and trace
    io_ddf = ddf[ddf['cat'].isin([0, 1, 3])]
    mpi_ddf = ddf[ddf['cat'] == 2]
    trace_ddf = ddf[ddf['cat'] == 4]
    # Set additional columns
    set_durations(io_ddf)
    set_filenames(io_ddf)
    # Return splitted dataframes
    return io_ddf, mpi_ddf, trace_ddf


def split_read_write_metadata(io_ddf: DataFrame, compute=False) -> Tuple[DataFrame, DataFrame, DataFrame]:
    # Prepare conditions
    read_condition, write_condition = _read_write_cond_io_df(io_ddf)
    # Then compute read & write and metadata dataframes
    io_ddf_read = io_ddf[read_condition]
    io_ddf_write = io_ddf[write_condition]
    io_ddf_metadata = io_ddf[~read_condition & ~write_condition]
    # Set additional columns
    set_sizes_counts(io_ddf_read)
    set_sizes_counts(io_ddf_write)
    set_bandwidths(io_ddf_read)
    set_bandwidths(io_ddf_write)
    # Compute if specified
    if compute:
        io_ddf_read = io_ddf_read.compute()
        io_ddf_write = io_ddf_write.compute()
        io_ddf_metadata = io_ddf_metadata.compute()
    # Return computed dataframes
    return io_ddf_read, io_ddf_write, io_ddf_metadata
