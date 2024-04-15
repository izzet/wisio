
import dask.dataframe as dd
import numpy as np
import pandas as pd

XFER_SIZE_BINS = [
    -np.inf,
    4 * 1024.0,
    # 16 * 1024.0,
    64 * 1024.0,
    # 256 * 1024.0,
    1 * 1024.0 * 1024.0,
    # 4 * 1024.0 * 1024.0,
    16 * 1024.0 * 1024.0,
    # 64 * 1024.0 * 1024.0,
    np.inf
]


def calc_job_time(ddf):
    return ddf['tend'].max() - ddf['tstart'].min()


def calc_read_size(ddf):
    return ddf[ddf['io_cat'] == 1]['size'].sum()


def calc_write_size(ddf):
    return ddf[ddf['io_cat'] == 2]['size'].sum()


def calc_num_files(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2, 3]))]['file_name'].nunique()


def calc_num_procs(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2, 3]))]['proc_name'].nunique()


def calc_fpp(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2, 3]))].groupby(['file_name'])['proc_name'].nunique().to_frame().query('proc_name == 1')


def calc_acc_pat(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))]['acc_pat'].value_counts()


def calc_size(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))]['size'].sum()


def calc_ops_dist(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2, 3]))]['io_cat'].value_counts()


def calc_xfer_dist(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))]['size'].map_partitions(pd.cut, XFER_SIZE_BINS).value_counts()


def calc_agg_bw(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))]['size'].sum() / ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))]['duration'].sum()


def char_summary_tasks(ddf):
    return [
        calc_job_time(ddf),
        calc_read_size(ddf),
        calc_write_size(ddf),
        calc_num_files(ddf),
        calc_num_procs(ddf),
        calc_fpp(ddf),
        calc_acc_pat(ddf),
        calc_size(ddf),
        calc_ops_dist(ddf),
        calc_xfer_dist(ddf),
        calc_agg_bw(ddf),
    ]


def cm1_issue1_file_size_per_rank(ddf):
    ddf0 = ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))] \
        .groupby(['file_name', 'io_cat']) \
        .agg({'size': ['mean', sum], 'rank': [min, max, 'count']})

    ddf0.columns = ['_'.join(tup).rstrip('_') for tup in ddf0.columns.values]

    ddf0 = ddf0.assign(rank_rank=lambda x: x['rank_min'].astype(str) + '-' + x['rank_max'].astype(str)) \
        .reset_index() \
        .groupby(['rank_rank', 'io_cat']) \
        .agg({'size_mean': 'mean', 'size_sum': sum})

    ddf0['size_mean'] = ddf0['size_mean'] / 1024 ** 2
    ddf0['size_sum'] = ddf0['size_sum'] / 1024 ** 3

    return ddf0


def cm1_issue3_rank_0_write_low_bw(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'] == 2)] \
        .groupby(['rank']) \
        .agg({'size': sum, 'duration': sum}) \
        .assign(bw=lambda x: x['size'] / x['duration'] / 1024 ** 3)


def hacc_issue1_open_close(ddf):
    return ddf[ddf['func_id'].str.contains('open|close')].groupby(['file_name', 'func_id'])['index'].count()


def montagep_issue1_io_size_per_app(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))].groupby(['app', 'io_cat']).agg({'size': sum})


def montagep_issue2_io_size_per_app_per_time(ddf):
    def assign_time_bin(df):
        df['time_bin'] = np.digitize(df['tmid'], bins=np.arange(434) * 1e7)
        return df

    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))] \
        .map_partitions(assign_time_bin) \
        .groupby(['app', 'time_bin']) \
        .agg({'size': sum}) \
        .sort_values('size', ascending=False)


def generic_issue_bw_by_rank(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))] \
        .groupby(['rank']) \
        .agg({'size': sum, 'duration': sum}) \
        .assign(bw=lambda x: x['size'] / x['duration'] / 1024 ** 3)


def generic_issue_low_bw(ddf):

    def assign_size_bin(df):
        df['size_bin'] = pd.cut(df['size'], XFER_SIZE_BINS)
        return df

    ddf0 = ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2]))]

    return ddf0 \
        .map_partitions(assign_size_bin) \
        .groupby(['size_bin', 'io_cat']) \
        .agg({'index': 'count', 'size': sum, 'duration': sum}) \
        .assign(bw=lambda x: x['size'] / x['duration'] / 1024 ** 3) \
        .dropna()


def generic_issue_metadata_access_per(ddf):
    return ddf[(ddf['cat'] == 0) & (ddf['io_cat'].isin([1, 2, 3]))] \
        .groupby(['proc_name', 'io_cat']) \
        .sum() \
        .reset_index() \
        .groupby('io_cat')['duration'] \
        .max()

import dask
import time
from dask.distributed import Client
from dask_jobqueue import LSFCluster

n_workers = 8
n_threads_per_worker = 16

cluster = LSFCluster(
    cores=n_workers * n_threads_per_worker,
    # death_timeout=self.config.death_timeout,
    job_directives_skip=['-n', '-R', '-M', '-P', '-W 00:30'],
    job_extra_directives=['-nnodes 1', '-G asccasc', '-q pdebug', '-W 120'],
    # local_directory=self.config.local_dir,
    memory=f"1600GB",
    processes=n_workers,
    scheduler_options=dict(
        # dashboard_address=dashboard_address,
        # host=self.config.host,
    ),
    use_stdin=True,
)

client = Client(cluster)

cluster.scale(n_workers)

def _wait_until_workers_alive(client, sleep_seconds=2):
    current_n_workers = len(client.scheduler_info()['workers'])
    while client.status == 'running' and current_n_workers < n_workers:
        current_n_workers = len(client.scheduler_info()['workers'])
        print(f"Waiting for workers ({current_n_workers}/{n_workers})")
        # Try to force cluster to boot workers
        cluster._correct_state()
        # Wait
        time.sleep(sleep_seconds)
    print('All workers alive')

print('client dashboard', client.dashboard_link)

_wait_until_workers_alive(client)

app_traces = {
    'hacc': '/usr/workspace/iopp/wisio_logs/recorder_hacc_32_0/_parquet',
    'lbann_jag': '/usr/workspace/iopp/wisio_logs/recorder_lbann_jag_32/_parquet',
    'cm1': '/p/gpfs1/iopp/wisio_logs/recorder_cm1_32_4/_parquet',
    'montagep': '/usr/workspace/iopp/wisio_logs/recorder_montage_pegasus_32/_parquet',
    'lbann_cosmoflow': '/usr/workspace/iopp/wisio_logs/recorder_lbann_cosmoflow_32/_parquet',
}

for app, trace in app_traces.items():

    ddf = dd.read_parquet(trace)

    char_tasks = char_summary_tasks(ddf)
    char_t0 = time.perf_counter()
    for i, t in enumerate(char_tasks):
        t0 = time.perf_counter()
        r, = dask.compute(t)
        print(f"{app} char {i + 1}/{len(char_tasks)} completed {time.perf_counter() - t0}")
    char_elapsed = time.perf_counter() - char_t0

    app_tasks = []
    if app == 'cm1':
        app_tasks.extend([
            generic_issue_low_bw(ddf),
            generic_issue_metadata_access_per(ddf),
            cm1_issue1_file_size_per_rank(ddf),
            cm1_issue3_rank_0_write_low_bw(ddf),
        ])
    elif app == 'hacc':
        app_tasks.extend([
            generic_issue_bw_by_rank(ddf),
            generic_issue_low_bw(ddf),
            generic_issue_metadata_access_per(ddf),
            hacc_issue1_open_close(ddf),
        ])
    elif app == 'montagep':
        app_tasks.extend([
            generic_issue_low_bw(ddf),
            generic_issue_metadata_access_per(ddf),
            montagep_issue1_io_size_per_app(ddf),
            montagep_issue2_io_size_per_app_per_time(ddf),
        ])
    else:
        app_tasks.extend([
            generic_issue_bw_by_rank(ddf),
            generic_issue_low_bw(ddf),
            generic_issue_metadata_access_per(ddf),
        ])

    app_t0 = time.perf_counter()
    for i, t in enumerate(app_tasks):
        t0 = time.perf_counter()
        r, = dask.compute(t)
        print(f"{app} issue {i + 1}/{len(app_tasks)} completed {time.perf_counter() - t0}")
    app_elapsed = time.perf_counter() - app_t0

    print(f"{app} total {char_elapsed + app_elapsed}")

    client.restart()

    _wait_until_workers_alive(client)
