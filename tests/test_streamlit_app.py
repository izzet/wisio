"""Smoke tests for streamlit_app.py.

The app is not importable as a module -- Streamlit executes it as a script on a
ScriptRunner *thread*, which is what makes it worth testing separately: an
import that only works on the main thread renders nothing at all, and the
failure never shows up in the analyzer test suite.
"""

import os

import pytest

streamlit = pytest.importorskip(
    'streamlit', reason='requires the [web] extra'
)
pytest.importorskip('altair', reason='requires the [web] extra')

from streamlit.testing.v1 import AppTest  # noqa: E402

from wisio.rules import KnownCharacteristics  # noqa: E402


APP = 'streamlit_app.py'


def test_app_renders_without_exceptions():
    """The form must render before any trace is uploaded.

    `dask_jobqueue` installs a SIGINT handler at import and `signal.signal`
    raises off the main thread, so importing it eagerly took the whole app down
    here with `ValueError: signal only works in main thread`.
    """
    at = AppTest.from_file(APP, default_timeout=120).run()

    assert not at.exception, [str(e) for e in at.exception]
    assert at.title[0].value == 'Welcome to WisIO Web'
    assert any(b.label == 'Analyze' for b in at.button)


def test_wisio_imports_off_the_main_thread():
    """Guards the same failure directly, without Streamlit in the picture."""
    import threading

    outcome = {}

    def _import():
        try:
            import wisio  # noqa: F401

            outcome['ok'] = True
        except Exception as exc:  # pragma: no cover - failure path is the point
            outcome['error'] = f"{type(exc).__name__}: {exc}"

    thread = threading.Thread(target=_import)
    thread.start()
    thread.join()

    assert outcome.get('ok'), outcome.get('error')


def test_process_and_file_counts_are_not_swapped():
    """The two metrics read different characteristics and are easy to transpose.

    They were: `# of Processes` rendered FILE_COUNT and `# of Files` rendered
    PROC_COUNT, so a 122-file, 8-process trace reported 8 files and 122
    processes.
    """
    source = open(APP).read()

    proc_metric = source.index(r'col12.metric(r"\# of Processes"')
    file_metric = source.index(r'col13.metric(r"\# of Files"')

    proc_line = source[proc_metric : source.index('\n', proc_metric)]
    file_line = source[file_metric : source.index('\n', file_metric)]

    assert 'proc_count' in proc_line, proc_line
    assert 'file_count' in file_line, file_line


def test_upload_cap_is_declared_once():
    """The per-file cap and the total check must come from one constant.

    They used to be two numbers -- `server.maxUploadSize` in config.toml and
    `MAX_TOTAL_UPLOAD_MB` in the app -- which is the kind of pair that drifts.
    `max_upload_size` on the widget overrides the config file, so the config
    file is gone and the constant is the single source.
    """
    source = open(APP).read()

    assert 'max_upload_size=MAX_TOTAL_UPLOAD_MB' in source
    assert not os.path.exists('.streamlit/config.toml'), (
        'config.toml is back; the cap is now set on the widget'
    )


def test_folder_upload_is_enabled():
    """Traces arrive as directories, not single files."""
    source = open(APP).read()

    assert 'accept_multiple_files="directory"' in source
    # Directory uploads carry relative paths, so names must be sanitised.
    assert 'safe_trace_filenames' in source


def test_cluster_is_pinned_for_constrained_hosting():
    """Community Cloud caps at 2 cores and 2.7GB.

    The default worker fan-out peaked at 2.9GB on the dftracer fixture against
    955MB with one worker, so leaving this to dask's autodetection is what
    would exhaust the container.
    """
    source = open(APP).read()

    assert 'cluster.n_workers={CLUSTER_N_WORKERS}' in source
    assert 'cluster.memory_limit={CLUSTER_MEMORY_LIMIT}' in source


def test_requirements_file_covers_the_reader_extras():
    """Community Cloud must not fall through to pyproject.toml.

    Cloud takes the first of uv.lock, Pipfile, environment.yml,
    requirements.txt and pyproject.toml. With no requirements.txt it installed
    pyproject.toml with poetry, which resolves `[project.dependencies]` but not
    `[project.optional-dependencies]` -- so no trace reader was installed and
    the deployed app raised `ModuleNotFoundError: No module named 'dftracer'`
    on the first analysis.
    """
    import tomllib

    requirement = [
        line.strip()
        for line in open('requirements.txt')
        if line.strip() and not line.startswith('#')
    ]
    assert len(requirement) == 1, requirement

    with open('pyproject.toml', 'rb') as fh:
        extras = set(tomllib.load(fh)['project']['optional-dependencies'])

    declared = set(requirement[0].partition('[')[2].rstrip(']').split(','))
    assert declared == extras, (
        f"requirements.txt installs {sorted(declared)} but pyproject.toml "
        f"defines {sorted(extras)}"
    )


def test_unavailable_reader_is_reported_rather_than_raised():
    """Hydra builds analyzers from a `_target_` path.

    A missing reader therefore surfaces as an InstantiationException traceback
    in the browser. The app checks first so the user gets told which reader is
    missing instead.
    """
    source = open(APP).read()

    assert 'ANALYZER_READERS' in source
    assert '_reader_available' in source
    # The check must run before the analyzer is built, not after it throws.
    assert source.index('_reader_available(reader)') < source.index(
        'wis = init_with_hydra('
    )


@pytest.mark.full
def test_analysis_renders_findings_end_to_end():
    """Drive a real trace through the form and check what comes out.

    Everything above this asserts on source text or an empty form. This is the
    only test that runs an analysis and looks at what the app actually shows,
    which is where a rendering bug would otherwise hide.
    """
    from glob import glob

    traces = sorted(glob('tests/data/extracted/dftracer-posix/*.pfw.gz'))
    assert traces, 'fixture not extracted'
    name = traces[0].rsplit('/', 1)[-1]

    at = AppTest.from_file(APP, default_timeout=600).run()
    at.file_uploader[0].set_value(
        (name, open(traces[0], 'rb').read(), 'application/gzip')
    )
    # 1-second granularity, so the trace spans several time periods and the
    # bottleneck path is actually reached.
    at.slider[0].set_value(1)
    at.button[0].click().run()

    assert not at.exception, [str(e) for e in at.exception]

    metrics = {metric.label: metric.value for metric in at.metric}
    assert metrics['I/O Operations'] == '2,053'
    assert metrics['Time Periods'] == '3'
    # Granularity is set in seconds but the analyzer counts microseconds. When
    # that conversion went missing every event landed in its own period, and
    # this read 2,024.
    assert metrics['Nodes'] == '1'
    assert metrics['Apps'] == '1'

    # Two accordion levels: one per view, one per finding inside it.
    labels = [expander.label for expander in at.expander]
    assert labels, 'no bottlenecks rendered'

    view_labels = [label for label in labels if 'bottleneck' in label]
    assert view_labels, labels
    assert 'View' in view_labels[0], view_labels[0]

    finding_labels = [label for label in labels if '-badge[' in label]
    assert finding_labels, labels
    # Severity as a badge and the numbers up front, not a raw dataframe.
    assert 'of I/O time' in finding_labels[0], finding_labels[0]

    # Permutation views are dropped, so only root (and logical) views appear.
    assert not any('>' in label for label in view_labels), view_labels


def test_characteristics_the_app_reads_exist():
    """Every characteristic the app indexes must be a real rule key."""
    source = open(APP).read()

    referenced = {
        name
        for name in dir(KnownCharacteristics)
        if not name.startswith('_')
        and f"KnownCharacteristics.{name}.value" in source
    }

    assert referenced, 'app no longer reads any characteristics'
    for name in referenced:
        assert getattr(KnownCharacteristics, name).value
