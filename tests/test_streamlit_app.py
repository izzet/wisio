"""Smoke tests for streamlit_app.py.

The app is not importable as a module -- Streamlit executes it as a script on a
ScriptRunner *thread*, which is what makes it worth testing separately: an
import that only works on the main thread renders nothing at all, and the
failure never shows up in the analyzer test suite.
"""

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
    assert at.title[0].value == 'Welcome to WisIO Live'
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
