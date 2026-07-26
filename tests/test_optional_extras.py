"""Optional analyzer extras must never break the base package.

wisio 0.1.1 shipped with `wisio/__init__.py` importing `.dftracer`
unconditionally, and `wisio/dftracer.py` importing `zindex_py` at module scope.
A missing (or broken) `zindex_py` therefore made `import wisio` and the `wisio`
CLI fail outright -- taking down darshan and recorder, which do not use it.

These tests pin the guard so that regression cannot recur.
"""

import builtins
import importlib
import subprocess
import sys

import pytest


OPTIONAL_MODULES = ['zindex_py', 'darshan']


def _reimport_wisio_without(monkeypatch, blocked):
    """Re-import wisio with `blocked` modules raising on import."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name in blocked or name.split('.')[0] in blocked:
            raise ImportError(f"blocked for test: {name}")
        return real_import(name, *args, **kwargs)

    for mod in list(sys.modules):
        if mod == 'wisio' or mod.startswith('wisio.'):
            monkeypatch.delitem(sys.modules, mod, raising=False)
    for mod in blocked:
        monkeypatch.delitem(sys.modules, mod, raising=False)

    monkeypatch.setattr(builtins, '__import__', fake_import)
    return importlib.import_module('wisio')


@pytest.mark.parametrize('blocked', [['zindex_py'], ['darshan'], OPTIONAL_MODULES])
def test_import_survives_missing_optional_dependency(monkeypatch, blocked):
    wisio = _reimport_wisio_without(monkeypatch, blocked)

    # The always-available analyzer must remain the real implementation.
    assert wisio.RecorderAnalyzer.__name__ == 'RecorderAnalyzer'
    assert wisio.Analyzer is not None


def test_recorder_analyzer_unaffected_by_missing_zindex(monkeypatch):
    """Recorder shares no code path with the dftracer reader."""
    wisio = _reimport_wisio_without(monkeypatch, ['zindex_py'])

    assert wisio.RecorderAnalyzer.__name__ == 'RecorderAnalyzer'
    assert issubclass(wisio.RecorderAnalyzer, wisio.Analyzer)


def test_guard_catches_importerror_not_just_modulenotfounderror(monkeypatch):
    """A broken-but-present extra raises ImportError, not ModuleNotFoundError.

    zindex_py 0.0.5 installs a wheel containing only dist-info metadata; a
    partially built native extension fails the same way.
    """
    wisio = _reimport_wisio_without(monkeypatch, ['zindex_py'])

    # Falls back to the base class rather than propagating.
    assert wisio.DFTracerAnalyzer is wisio.Analyzer


def test_cli_entry_point_imports_in_clean_subprocess():
    """End-to-end: `python -c 'import wisio'` must not depend on extras.

    Runs out-of-process so it reflects real interpreter startup rather than
    whatever this session already has cached in sys.modules.
    """
    result = subprocess.run(
        [sys.executable, '-c', 'import wisio; from wisio.__main__ import main; print("ok")'],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert 'ok' in result.stdout
