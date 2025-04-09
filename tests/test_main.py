import pathlib
import pytest
import random
from glob import glob
from hydra import compose, initialize
from hydra.core.hydra_config import HydraConfig
from typing import List

from wisio.config import init_hydra_config_store
from wisio.__main__ import main


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


# Full test matrix for comprehensive testing
full_analyzer_trace_params = [
    ("darshan", "tests/data/extracted/darshan-dxt"),
    ("dftracer", "tests/data/extracted/dftracer-raw"),
    ("recorder", "tests/data/extracted/recorder-parquet"),
]
full_checkpoint_params = [True, False]
full_metric_params = ["time", "iops"]
full_percentile_params = [0.95]

# Reduced matrix for smoke testing (fast runs)
smoke_analyzer_trace_params = [random.choice(full_analyzer_trace_params)]
smoke_checkpoint_params = [False]  # Skip checkpoint to make tests faster
smoke_metric_params = ["time"]  # Most common metric
smoke_percentile_params = [0.95]


@pytest.mark.full
@pytest.mark.parametrize("analyzer, trace_path", full_analyzer_trace_params)
@pytest.mark.parametrize("checkpoint", full_checkpoint_params)
@pytest.mark.parametrize("metric", full_metric_params)
@pytest.mark.parametrize("percentile", full_percentile_params)
def test_e2e_full(
    analyzer: str,
    trace_path: str,
    checkpoint: bool,
    metric: str,
    percentile: float,
    tmp_path: pathlib.Path,
    override_hydra_config,
) -> None:
    """Full test suite with all parameter combinations."""
    _test_e2e(
        analyzer,
        trace_path,
        checkpoint,
        metric,
        percentile,
        tmp_path,
        override_hydra_config,
    )


@pytest.mark.smoke
@pytest.mark.parametrize("analyzer, trace_path", smoke_analyzer_trace_params)
@pytest.mark.parametrize("checkpoint", smoke_checkpoint_params)
@pytest.mark.parametrize("metric", smoke_metric_params)
@pytest.mark.parametrize("percentile", smoke_percentile_params)
def test_e2e_smoke(
    analyzer: str,
    trace_path: str,
    checkpoint: bool,
    metric: str,
    percentile: float,
    tmp_path: pathlib.Path,
    override_hydra_config,
) -> None:
    """Smoke test with minimal parameter combinations for quick validation."""
    _test_e2e(
        analyzer,
        trace_path,
        checkpoint,
        metric,
        percentile,
        tmp_path,
        override_hydra_config,
    )


def _test_e2e(
    analyzer: str,
    trace_path: str,
    checkpoint: bool,
    metric: str,
    percentile: float,
    tmp_path: pathlib.Path,
    override_hydra_config,
) -> None:
    """Common test logic extracted to avoid duplication."""
    bottleneck_dir = f"{tmp_path}/bottlenecks"
    checkpoint_dir = f"{tmp_path}/checkpoints"

    cfg = override_hydra_config(
        [
            f"+analyzer={analyzer}",
            f"analyzer.bottleneck_dir={bottleneck_dir}",
            f"analyzer.checkpoint={checkpoint}",
            f"analyzer.checkpoint_dir={checkpoint_dir}",
            f"hydra.run.dir={tmp_path}",
            f"hydra.runtime.output_dir={tmp_path}",
            f"metrics=[{metric}]",
            f"percentile={percentile}",
            f"trace_path={trace_path}",
        ]
    )

    hydra_config = HydraConfig.get()
    assert hydra_config.run.dir == tmp_path.as_posix()
    assert hydra_config.runtime.output_dir == tmp_path.as_posix()
    assert cfg.analyzer.bottleneck_dir == bottleneck_dir
    assert cfg.analyzer.checkpoint == checkpoint
    assert cfg.analyzer.checkpoint_dir == checkpoint_dir
    assert cfg.metrics == [metric]
    assert cfg.percentile == percentile
    assert cfg.trace_path == trace_path

    # Run the main function
    main(cfg)

    assert any(glob(f"{bottleneck_dir}/*.parquet")), "No bottleneck found"
    if checkpoint:
        assert any(glob(f"{checkpoint_dir}/*.json")), "No checkpoint found"
