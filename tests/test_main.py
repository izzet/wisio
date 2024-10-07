import os
import pathlib
import pytest
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
    [("recorder", "tests/data/extracted/recorder-parquet")],
)
@pytest.mark.parametrize("checkpoint", [True, False])
def test_e2e(
    analyzer: str,
    trace_path: str,
    checkpoint: bool,
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
            f"trace_path={trace_path}",
        ]
    )

    assert HydraConfig.get().runtime.output_dir == tmp_path.as_posix()
    assert cfg.trace_path == trace_path

    main(cfg)

    assert os.path.exists(bottleneck_dir)
    if checkpoint:
        assert os.path.exists(checkpoint_dir)
