import altair as alt
import dask
import importlib
import os
import shutil
import streamlit as st
import numpy as np
import pandas as pd
import sys
import tempfile
from bottleneck_report import describe_bottlenecks
from uploads import safe_trace_filenames
from wisio import init_with_hydra
from wisio.constants import XFER_SIZE_BIN_LABELS
from wisio.rules import KnownCharacteristics
from wisio.types import Characteristics, RawStats

DEFAULT_THRESHOLD = 45
DEFAULT_TIME_GRANULARITY_IN_SECONDS = 5  # 5 seconds

# Sized for Streamlit Community Cloud, which caps at 2.7GB and 2 CPU cores.
# Measured by replaying the dftracer fixture at increasing multiples, one
# worker. The baseline depends on whether that worker is threaded or a child
# process (see THREADED_WORKER_MAX_MB); memory then grows about 60MB per MB of
# trace either way, more steeply when threaded:
#
#            32KB    1.3MB    7.4MB     15MB     20MB
#   process  614MB    660MB   1037MB   1687MB   1932MB
#   thread   296MB    430MB   1217MB   2116MB   2382MB
#
# 20MB on a process worker lands near 1.9GB, leaving headroom under the ceiling.
# The worker limit has to clear that peak or dask kills the worker mid-run --
# at 1.5GB a 15MB upload died with KilledWorker.
#
# Note this spends the burst headroom: ~1GB is what Community Cloud reliably
# guarantees, so a large upload may be evicted under memory pressure. Wall time
# is the other limit -- 20MB took 70s on 40 cores, and Cloud allows at most 2.
#
# Worker count dominates everything else: the default fan-out peaked at 2.9GB
# on the dftracer fixture against 955MB with a single worker.
CLUSTER_N_WORKERS = 1
CLUSTER_MEMORY_LIMIT = 2_200_000_000  # bytes; dask spills, pauses, then restarts
MAX_TOTAL_UPLOAD_MB = 20

# Whether the single worker runs in this process or a child of it, decided from
# the upload size because the two trade against each other. A threaded worker
# does not re-import pandas, pyarrow and dask into a child, which halves memory
# on a small trace -- 296MB against 614MB at 32KB, 429MB against 660MB at
# 1.3MB -- and is faster at every size measured. But its memory grows more
# steeply, since intermediates and results share one heap: the two cross around
# 4MB, and by the 20MB cap the threaded worker reaches 2.4GB against 1.9GB,
# which is 88% of Community Cloud's ceiling.
#
# Most uploads are small, so the common case gets the cheaper worker and the
# tail keeps the headroom.
THREADED_WORKER_MAX_MB = 4

# Third-party module each analyzer needs, for the pre-flight check below.
# Recorder reads Parquet through dask and needs no extra.
ANALYZER_READERS = {'darshan': 'darshan', 'dftracer': 'dftracer'}

# Findings shown per view before collapsing to a count. Expanders are cheap
# collapsed, but a pathological run should not render thousands of rows.
MAX_BOTTLENECKS_PER_VIEW = 20

# Severity as a Streamlit badge colour. The score already carries this, so the
# badge replaces repeating it as text -- `[LO1]` said "low" twice.
SCORE_COLORS = {
    'critical': 'red',
    'very high': 'red',
    'high': 'orange',
    'medium': 'orange',
    'low': 'green',
    'very low': 'green',
    'trivial': 'gray',
    'none': 'gray',
}


def pluralize(noun: str, count: int) -> str:
    return noun if count == 1 else f"{noun}s"


def _render_bottleneck(bottleneck) -> None:
    """One finding: a scannable headline, then the detail behind it.

    The headline leads with the numbers rather than the full sentence, which
    runs to about 150 characters and wraps badly at this width. The sentence is
    still the first thing inside.
    """
    color = SCORE_COLORS.get(bottleneck.score, 'gray')
    # The ratio comes before the time share deliberately: severity is scored on
    # cost per operation, so a finding worth 0.0% of I/O time can still rank
    # critical. Leading with the share alone reads like a mislabel.
    ratio = bottleneck.ops_time_ratio
    headline = ' · '.join(
        part
        for part in (
            f"#{bottleneck.id}",
            f"{bottleneck.num_processes:,} "
            f"{pluralize('process', bottleneck.num_processes)}"
            if bottleneck.num_processes
            else '',
            f"{bottleneck.num_files:,} {pluralize('file', bottleneck.num_files)}"
            if bottleneck.num_files
            else '',
            f"{bottleneck.num_ops:,} {pluralize('op', bottleneck.num_ops)}"
            if bottleneck.num_ops
            else '',
            f"{ratio:,.1f}x time vs. ops" if ratio and ratio >= 1.05 else '',
            f"{bottleneck.time_overall * 100:.1f}% of I/O time",
        )
        if part
    )

    with st.expander(f":{color}-badge[{bottleneck.score.title()}] {headline}"):
        st.markdown(bottleneck.description)

        col_time, col_share, col_ops, col_ratio = st.columns(4)
        col_time.metric("I/O Time", f"{bottleneck.time:.2f} s", border=True)
        col_share.metric(
            "Share of I/O", f"{bottleneck.time_overall * 100:.1f}%", border=True
        )
        col_ops.metric("Operations", f"{bottleneck.num_ops:,}", border=True)
        col_ratio.metric(
            "Time vs. ops",
            f"{ratio:,.1f}x" if ratio else "—",
            border=True,
            help="Share of I/O time divided by share of operations. Above 1x "
            "means this costs more time than its operation count suggests, "
            "which is what the severity is ranked on.",
        )

        if bottleneck.subject:
            st.caption(f"Subject: `{bottleneck.subject}`")

        if bottleneck.reasons:
            for reason in bottleneck.reasons:
                st.markdown(
                    f":blue-badge[{reason.rule_name}] {reason.description}"
                )
        else:
            st.markdown("_No reasons were attached._")


XFER_SIZE_CAT_TYPE = pd.CategoricalDtype(categories=XFER_SIZE_BIN_LABELS, ordered=True)
VIEW_TYPE_MAPPING = {
    'File': 'file_name',
    'Process': 'proc_name',
    'Timeline': 'time_range',
}


def _reader_available(module_name: str) -> bool:
    """Whether a trace reader can actually be imported.

    Not `find_spec`: pydarshan installs cleanly and then raises `RuntimeError`
    at import when it cannot locate libdarshan-util.so, so the module has to be
    imported to know whether it works.
    """
    try:
        importlib.import_module(module_name)
    except Exception:
        return False
    return True


st.set_page_config(
    page_title="WisIO Web",
    layout="centered",
    menu_items={
        'About': 'https://grc.iit.edu/research/projects/wisio',
        'Report a bug': 'https://github.com/grc-iit/wisio/issues',
    },
)

st.write(
    r'''
    <style>
        [data-testid="stImageContainer"] img {border-radius: 0;}
        [data-testid="stMainBlockContainer"] {max-width: 812px;}
    </style>
    ''',
    unsafe_allow_html=True,
)

st.image("assets/logo.png", width=200)
st.title("Welcome to WisIO Web")
st.markdown(
    """
    Analyze, visualize, and understand I/O performance issues in HPC workloads.
    """
)

result = None
bottlenecks = None
characteristics: Characteristics = {}
raw_stats: RawStats = {}

with st.form('analysis_form'):
    trace_files = st.file_uploader(
        "Upload trace files",
        type=["darshan", "parquet", "pfw", "pfw.gz"],
        accept_multiple_files=True,
        # Per file, and authoritative over config.toml, which keeps the limit
        # next to the total check below rather than in a separate file.
        max_upload_size=MAX_TOTAL_UPLOAD_MB,
        help=(
            f"Up to {MAX_TOTAL_UPLOAD_MB} MB in total. Select every file in a "
            "run at once. Larger runs are better analyzed locally with the "
            "`wisio` command."
        ),
    )

    view_types = st.multiselect(
        "Select perspectives to analyze",
        options=VIEW_TYPE_MAPPING.keys(),
        default=VIEW_TYPE_MAPPING.keys(),
    )

    time_granularity = st.slider(
        "Set time granularity for analysis (in seconds)",
        min_value=1,
        max_value=100,
        value=DEFAULT_TIME_GRANULARITY_IN_SECONDS,
        step=1,
        help="This sets the granularity of time intervals for analysis.",
        disabled='Timeline' not in view_types,
    )

    threshold = st.slider(
        "Set the threshold for bottleneck detection",
        min_value=0,
        max_value=90,
        format="%d%%",
        value=DEFAULT_THRESHOLD,
        step=1,
        help="This threshold determines the sensitivity of bottleneck detection.",
    )

    logical_view_types = st.checkbox(
        "Enable logical view types",
        value=False,
        help="Logical view types allow for more complex analysis but may take longer to compute.",
    )

    submit = st.form_submit_button("Analyze")

if submit:
    # Check if all trace files have the same type
    if not trace_files or len(trace_files) == 0:
        st.error("Please upload at least one trace file.")
        st.stop()
    if len(set(file.name.split('.')[-1] for file in trace_files)) > 1:
        st.error("All trace files must be of the same type.")
        st.stop()

    # `server.maxUploadSize` is enforced per file, so several accepted files can
    # still add up to more than the analysis has memory for.
    total_upload_mb = sum(file.size for file in trace_files) / (1024 * 1024)
    if total_upload_mb > MAX_TOTAL_UPLOAD_MB:
        st.error(
            f"These traces total {total_upload_mb:.1f} MB, over the "
            f"{MAX_TOTAL_UPLOAD_MB} MB this deployment can analyze. Upload a "
            "shorter run or a subset of the ranks, or run WisIO locally with "
            "`wisio +analyzer=... trace_path=...` where the limit is your own "
            "machine."
        )
        st.stop()

    analyzer = 'darshan'
    if all(file.name.endswith('.parquet') for file in trace_files):
        analyzer = 'recorder'
    elif all(file.name.endswith('.pfw') or file.name.endswith('.pfw.gz') for file in trace_files):
        analyzer = 'dftracer'

    # Hydra builds the analyzer from its `_target_` path, so a reader that is
    # not installed surfaces as an InstantiationException traceback rather than
    # anything a user can act on. Check first and say what is actually wrong.
    reader = ANALYZER_READERS.get(analyzer)
    if reader and not _reader_available(reader):
        st.error(
            f"This deployment cannot read {analyzer.title()} traces: the "
            f"`{reader}` reader is unavailable."
            + (
                " pydarshan ships CPython wheels only up to 3.12, and this "
                f"deployment runs Python {'.'.join(map(str, sys.version_info[:2]))}."
                if analyzer == 'darshan'
                else ""
            )
            + " Recorder and DFTracer traces work here, or run WisIO locally"
            " with `pip install 'wisio[darshan]'`."
        )
        st.stop()

    with st.status("Analyzing trace files", expanded=True) as status:
        st.write(f"Detected analyzer type: {analyzer.title()}")

        with tempfile.TemporaryDirectory() as temp_dir:
            st.write(f"Using temporary directory: {temp_dir}")

            safe_names = safe_trace_filenames(
                trace_file.name for trace_file in trace_files
            )
            for trace_file, safe_name in zip(trace_files, safe_names):
                with open(os.path.join(temp_dir, safe_name), "wb") as temp_trace_file:
                    temp_trace_file.write(trace_file.getbuffer())

            wis = init_with_hydra(
                hydra_overrides=[
                    f"+analyzer={analyzer}",
                    f"cluster.n_workers={CLUSTER_N_WORKERS}",
                    f"cluster.memory_limit={CLUSTER_MEMORY_LIMIT}",
                    f"cluster.processes={total_upload_mb > THREADED_WORKER_MAX_MB}",
                    f"analyzer.bottleneck_dir={temp_dir}",
                    f"analyzer.checkpoint={False}",
                    # The slider is in seconds; the analyzer counts microseconds.
                    f"analyzer.time_granularity={time_granularity * 1e6}",
                    f"hydra.run.dir={temp_dir}",
                    f"hydra.runtime.output_dir={temp_dir}",
                    f"logical_view_types={logical_view_types}",
                    f"threshold={threshold}",
                    f"trace_path={temp_dir}",
                    f"view_types=[{','.join([VIEW_TYPE_MAPPING[view_type] for view_type in view_types])}]",
                ]
            )
            st.write("Initialized WisIO analyzer.")

            st.write("Analyzing trace files...")
            result = wis.analyze_trace()
            (bottlenecks, characteristics, raw_stats) = dask.compute(
                result._bottlenecks,
                result.characteristics,
                result.raw_stats,
            )
            st.write("Analysis complete.")

            try:
                st.write("Shutting down analyzer...")
                wis.client.close()
                wis.cluster.close()
                st.write("Analyzer shut down.")
            except Exception as e:
                st.error(f"Error shutting down analyzer: {e}")
                st.write("Please restart the application.")

            st.write("Cleaning up temporary directory...")
            shutil.rmtree(temp_dir, ignore_errors=True)
            st.write("Temporary directory cleaned up.")

            status.update(label="Analysis complete.", expanded=False, state="complete")

            st.session_state['result'] = result
            st.session_state['bottlenecks'] = bottlenecks
            st.session_state['characteristics'] = characteristics
            st.session_state['raw_stats'] = raw_stats

# if 'result' in st.session_state:
#     result = st.session_state['result']
#     bottlenecks = st.session_state['bottlenecks']
#     characteristics = st.session_state['characteristics']
#     raw_stats = st.session_state['raw_stats']
# else:
#     result = None

if result:
    st.subheader("Analysis Results")

    characteristics_tab, bottlenecks_tab = st.tabs(["I/O Characteristics", "I/O Bottlenecks"])

    with characteristics_tab:
        file_count = characteristics[KnownCharacteristics.FILE_COUNT.value].value
        proc_count = characteristics[KnownCharacteristics.PROC_COUNT.value].value
        io_ops = characteristics[KnownCharacteristics.IO_COUNT.value].value
        io_size_fmt = characteristics[KnownCharacteristics.IO_SIZE.value].value_fmt
        io_time = characteristics[KnownCharacteristics.IO_TIME.value].value
        node_count = characteristics[KnownCharacteristics.NODE_COUNT.value].value
        app_count = characteristics[KnownCharacteristics.APP_COUNT.value].value
        time_periods = characteristics[KnownCharacteristics.TIME_PERIOD.value].value
        read_xfer_bins = characteristics[KnownCharacteristics.READ_XFER_SIZE.value]._dataframe
        write_xfer_bins = characteristics[KnownCharacteristics.WRITE_XFER_SIZE.value]._dataframe

        col11, col12, col13 = st.columns(3)
        col11.metric("Runtime", f"{raw_stats.job_time:.2f} s", border=True)
        col12.metric(r"\# of Processes", f"{proc_count:,}", border=True)
        col13.metric(r"\# of Files", f"{file_count:,}", border=True)

        col21, col22, col23 = st.columns(3)
        col21.metric("I/O Time", f"{io_time:.2f} s", border=True)
        col22.metric("I/O Operations", f"{io_ops:,}", border=True)
        col23.metric("I/O Size", io_size_fmt, border=True)

        # Computed all along and shown by the console, but never surfaced here.
        # Access Pattern is deliberately still omitted: it is hardcoded for the
        # DXT and DFTracer readers, so it would report an unmeasured
        # "100% sequential" rather than a missing value.
        col31, col32, col33 = st.columns(3)
        col31.metric("Nodes", f"{node_count:,}", border=True)
        col32.metric("Apps", f"{app_count:,}", border=True)
        col33.metric("Time Periods", f"{time_periods:,}", border=True)

        col41, col42 = st.columns(2)
        col41.markdown("**Read Request Size Distribution**")
        read_xfer_bins_full = read_xfer_bins['read_count'].reindex(XFER_SIZE_BIN_LABELS).fillna(0)
        read_xfer_bins_fixed = pd.DataFrame(
            {"Size Range": read_xfer_bins_full.index, "Operations": read_xfer_bins_full.values}
        )
        read_xfer_bins_fixed['Size Range'] = read_xfer_bins_fixed['Size Range'].astype(XFER_SIZE_CAT_TYPE)
        col41.write(
            alt.Chart(read_xfer_bins_fixed)
            .mark_bar()
            .encode(
                x=alt.X('Operations', title='# of I/O Operations'),
                y=alt.Y('Size Range', sort=None, title=None),
            )
        )
        # col41.bar_chart(read_xfer_bins_fixed.set_index('Size Range'), horizontal=True)
        col42.markdown("**Write Request Size Distribution**")
        write_xfer_bins_fixed = write_xfer_bins['write_count'].reindex(XFER_SIZE_BIN_LABELS).fillna(0)
        write_xfer_bins_fixed = pd.DataFrame(
            {"Size Range": write_xfer_bins_fixed.index, "Operations": write_xfer_bins_fixed.values}
        )
        write_xfer_bins_fixed['Size Range'] = write_xfer_bins_fixed['Size Range'].astype(XFER_SIZE_CAT_TYPE)
        col42.write(
            alt.Chart(write_xfer_bins_fixed)
            .mark_bar()
            .encode(
                x=alt.X('Operations', title='# of I/O Operations'),
                y=alt.Y('Size Range', sort=None, title=None),
            )
        )

    with bottlenecks_tab:
        if bottlenecks is None or len(bottlenecks) == 0:
            st.info(
                "No bottlenecks were detected. Lower the threshold and analyze "
                "again to surface less severe findings."
            )
        else:
            for reported_metric in bottlenecks['metric'].unique():
                for_metric = bottlenecks[bottlenecks['metric'] == reported_metric]
                views = describe_bottlenecks(
                    for_metric,
                    result.bottleneck_rules,
                    metric=reported_metric,
                    max_bottlenecks=MAX_BOTTLENECKS_PER_VIEW,
                )

                # One accordion per view. A real trace yields hundreds of
                # findings across a handful of views, so collapsing by view is
                # what makes the page navigable; the first opens so the page is
                # not a row of shut boxes.
                for position, view in enumerate(views):
                    summary = (
                        f"{view.num_bottlenecks:,} "
                        f"{pluralize('bottleneck', view.num_bottlenecks)} · "
                        f"{view.num_reasons:,} "
                        f"{pluralize('reason', view.num_reasons)}"
                    )
                    with st.expander(
                        f"**{view.name}** — {summary}", expanded=position == 0
                    ):
                        st.caption(
                            "Ranked by cost per operation, not by total time, so "
                            "a small but disproportionately slow operation can "
                            "outrank a large one."
                        )
                        for bottleneck in view.bottlenecks:
                            _render_bottleneck(bottleneck)

                        if view.num_hidden:
                            st.caption(
                                f"{view.num_hidden:,} more not shown, worst first. "
                                "Run WisIO locally for the full report."
                            )
