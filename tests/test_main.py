import os
import pathlib
import pytest
import wisio
from hydra import compose, initialize_config_dir
from hydra.core.config_store import ConfigStore
from wisio.config import AnalysisType, MainConfig
from wisio.__main__ import main

config_dir = os.path.dirname(wisio.__file__) + "/configs"

cs = ConfigStore.instance()
cs.store(name="base_config", node=MainConfig)


@pytest.mark.parametrize(
    "analysis_type, trace_path",
    [(AnalysisType.RECORDER, "tests/data/extracted/recorder-parquet")],
)
@pytest.mark.parametrize("checkpoint", ["enabled", "disabled"])
def test_e2e(
    analysis_type: AnalysisType,
    checkpoint: str,
    trace_path: str,
    tmp_path: pathlib.Path,
) -> None:
    with initialize_config_dir(config_dir=config_dir, version_base=None):
        config = compose(
            config_name="config",
            overrides=[
                f"analysis={analysis_type.value}",
                f"analysis.bottleneck_dir={tmp_path}/bottlenecks",
                f"analysis.trace_path={trace_path}",
                f"checkpoint={checkpoint}",
                f"checkpoint.dir={tmp_path}/checkpoints",
            ],
        )
        assert config.analysis.type == analysis_type
        assert config.analysis.trace_path == trace_path

    main(config)

    assert os.path.exists(f"{tmp_path}/bottlenecks")

    if checkpoint == "enabled":
        assert os.path.exists(f"{tmp_path}/checkpoints")
