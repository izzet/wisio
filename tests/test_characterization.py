"""Characterization tests: pin the *numbers* wisio currently produces.

`test_main.py` only asserts that some parquet file exists, which passes even if
every value is wrong. These tests lock the analysis output for the darshan and
recorder analyzers so that porting work (dtype changes, time bucketing,
`host_name`/`offset` additions, raw-stats fixes) cannot silently shift results.

Golden values were captured from wisio 0.1.1 on Python 3.10 with the fixtures in
`tests/data/`. When a port intentionally changes a number, update the golden
value in the same commit and say why -- that diff is the point of these tests.
"""

import json
import pandas as pd
import pytest
from glob import glob
from hydra import compose, initialize
from hydra.core.hydra_config import HydraConfig
from typing import List

from wisio.__main__ import main
from wisio.config import init_hydra_config_store


@pytest.fixture(scope="function")
def override_hydra_config():
    def _override_hydra_config(overrides: List[str]):
        with initialize(version_base=None, config_path=None):
            init_hydra_config_store()
            cfg = compose(
                config_name="config",
                overrides=overrides,
                return_hydra_config=True,
            )
            HydraConfig.instance().set_config(cfg)
            return cfg

    yield _override_hydra_config

    _override_hydra_config([])


def _run(analyzer, trace_path, tmp_path, override_hydra_config, extra=()):
    cfg = override_hydra_config(
        [
            f"+analyzer={analyzer}",
            f"analyzer.bottleneck_dir={tmp_path}/bottlenecks",
            "analyzer.checkpoint=True",
            f"analyzer.checkpoint_dir={tmp_path}/checkpoints",
            f"hydra.run.dir={tmp_path}",
            f"hydra.runtime.output_dir={tmp_path}",
            "percentile=0.99",
            f"trace_path={trace_path}",
            *extra,
        ]
    )
    main(cfg)
    return cfg


def _raw_stats(tmp_path):
    with open(f"{tmp_path}/checkpoints/_raw_stats.json") as fh:
        return json.load(fh)


def _bottlenecks(tmp_path):
    assert any(glob(f"{tmp_path}/bottlenecks/*.parquet")), "no bottlenecks written"
    return pd.read_parquet(f"{tmp_path}/bottlenecks")


def _darshan_available():
    """pydarshan raises RuntimeError, not ImportError, without its native lib."""
    try:
        import darshan  # noqa: F401
    except Exception:
        return False
    return True


requires_darshan = pytest.mark.skipif(
    not _darshan_available(), reason="requires a working [darshan] extra"
)


@pytest.mark.full
@requires_darshan
class TestDarshanDxtCharacterization:
    """DXT path: tests/data/darshan-dxt (unet3d_a100.darshan)."""

    def test_raw_stats(self, tmp_path, override_hydra_config):
        _run("darshan", "tests/data/extracted/darshan-dxt", tmp_path, override_hydra_config)

        stats = _raw_stats(tmp_path)
        assert stats["job_time"] == 76
        assert stats["time_granularity"] == 1000.0
        # `total_count` is what dfanalyzer's 913b602 changes; pin it explicitly.
        assert stats["total_count"] == 1953

    def test_bottleneck_shape(self, tmp_path, override_hydra_config):
        _run("darshan", "tests/data/extracted/darshan-dxt", tmp_path, override_hydra_config)

        df = _bottlenecks(tmp_path)
        assert len(df) == 20
        assert "subject" in df.columns
        for col in ("time", "count", "read_time", "write_time", "metadata_time"):
            assert col in df.columns


@pytest.mark.full
class TestRecorderCharacterization:
    """Parquet path: tests/data/recorder-parquet (CM1)."""

    def test_raw_stats(self, tmp_path, override_hydra_config):
        _run("recorder", "tests/data/extracted/recorder-parquet", tmp_path, override_hydra_config)

        stats = _raw_stats(tmp_path)
        assert stats["job_time"] == pytest.approx(667.808837890625)
        assert stats["time_granularity"] == 10000000.0
        assert stats["total_count"] == 27463

    def test_bottleneck_shape(self, tmp_path, override_hydra_config):
        _run("recorder", "tests/data/extracted/recorder-parquet", tmp_path, override_hydra_config)

        df = _bottlenecks(tmp_path)
        assert len(df) == 37


@pytest.mark.full
@requires_darshan
class TestDarshanNonDxtCharacterization:
    """Non-DXT path: tests/data/darshan-raw (multiple .darshan reports).

    KNOWN DEFECT: every report collapses into a single `app#localhost#0#0`
    process because `proc_name` is built from a hardcoded host and a rank that
    is 0 in each report. dfanalyzer bbd4437 (real `host_name`) and 913b602
    (raw-stats source) address this. These assertions pin the *buggy* current
    behavior so the fix shows up as an intentional diff.
    """

    def test_reports_collapse_into_one_process(self, tmp_path, override_hydra_config):
        _run(
            "darshan",
            "tests/data/extracted/darshan-raw",
            tmp_path,
            override_hydra_config,
            extra=["view_types=[file_name,proc_name]", "metrics=[time]"],
        )

        df = _bottlenecks(tmp_path)
        assert len(df) == 1, "currently collapses to a single process bottleneck"
        assert "app#localhost#0#0" in str(df["subject"].iloc[0])
