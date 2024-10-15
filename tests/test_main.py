import pathlib
import pytest
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


@pytest.mark.parametrize(
    "analyzer, trace_path",
    [
        ("dftracer", "tests/data/extracted/dftracer-raw"),
        ("recorder", "tests/data/extracted/recorder-parquet"),
    ],
)
@pytest.mark.parametrize("checkpoint", [True, False])
@pytest.mark.parametrize("metric", ["time", "iops"])
@pytest.mark.parametrize("percentile", [0.95])
def test_e2e(
    analyzer: str,
    trace_path: str,
    checkpoint: bool,
    metric: str,
    percentile: float,
    tmp_path: pathlib.Path,
    override_hydra_config,
) -> None:
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
