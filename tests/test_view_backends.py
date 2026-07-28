"""The Dask and pandas view paths must produce the same analysis.

Views small enough to materialise are computed in pandas rather than Dask,
which is worth roughly a 3x speedup but only if the two engines agree. They can
disagree in ways that are easy to miss: Dask coerces every partition to a meta
schema and infers one by running the function against a synthetic non-empty
frame, so an empty result can carry dtypes pandas would never produce.

These run the whole analyzer twice on the same trace, once with materialisation
disabled, and compare the bottlenecks written to Parquet.
"""

import pathlib

import pandas as pd
import pytest
from glob import glob
from hydra import compose, initialize
from hydra.core.hydra_config import HydraConfig
from pandas.testing import assert_frame_equal

from wisio.config import init_hydra_config_store
from wisio.__main__ import main


FIXTURES = [
    ('dftracer', 'tests/data/extracted/dftracer-posix'),
    ('dftracer', 'tests/data/extracted/dftracer-dlio'),
    ('recorder', 'tests/data/extracted/recorder-parquet'),
]


def _run(analyzer, trace_path, tmp_path, materialize_max_bytes, checkpoint=False):
    """Run one analysis and return the bottlenecks it wrote."""
    out = tmp_path / ('pandas' if materialize_max_bytes else 'dask')
    out.mkdir(parents=True, exist_ok=True)

    with initialize(version_base=None, config_path=None):
        init_hydra_config_store()
        cfg = compose(
            config_name='config',
            return_hydra_config=True,
            overrides=[
                f"+analyzer={analyzer}",
                'percentile=0.99',
                'cluster.n_workers=1',
                f"analyzer.bottleneck_dir={out}/bottlenecks",
                f"analyzer.checkpoint={checkpoint}",
                f"analyzer.checkpoint_dir={out}/checkpoints",
                f"analyzer.view_materialize_max_bytes={materialize_max_bytes}",
                f"trace_path={trace_path}",
                f"hydra.run.dir={out}",
                f"hydra.runtime.output_dir={out}",
            ],
        )
        HydraConfig.instance().set_config(cfg)
        main(cfg)

    files = sorted(glob(f"{out}/bottlenecks/*.parquet"))
    assert files, f"no bottlenecks written for {analyzer} {trace_path}"
    frame = pd.read_parquet(f"{out}/bottlenecks")
    # Row order is an artifact of partitioning, not of the analysis.
    return frame.sort_values(list(frame.columns)[:4]).reset_index(drop=True)


@pytest.mark.full
@pytest.mark.parametrize('analyzer, trace_path', FIXTURES)
def test_engines_agree(analyzer, trace_path, tmp_path):
    """The same trace must analyse the same way on either engine.

    Dtypes are compared exactly: they decide the Parquet schema, and letting
    them drift would mean the same analysis wrote a different file depending
    only on how large the trace was.

    Values are compared to a tolerance rather than bit-for-bit. `time_per` is
    `time / time.sum()`, and a sum has a different rounding path on each engine
    -- Dask sums per partition and combines, pandas sums straight through -- so
    the last bits of a few rows differ, and `iops_slope` inherits it. Measured
    at ~1 ULP: worst relative delta 2.4e-16 against a float64 epsilon of
    2.2e-16. The bound below is four orders of magnitude tighter than any real
    disagreement would be.
    """
    on_dask = _run(analyzer, trace_path, tmp_path, materialize_max_bytes=0)
    on_pandas = _run(analyzer, trace_path, tmp_path, materialize_max_bytes=64 * 1024**2)

    assert len(on_pandas) == len(on_dask)
    # By name, not position: the engines may order columns differently, which
    # `check_like` below tolerates and a positional comparison would not.
    assert on_pandas.dtypes.to_dict() == on_dask.dtypes.to_dict()
    assert_frame_equal(
        on_dask, on_pandas, check_like=True, check_exact=False, rtol=1e-12, atol=1e-12
    )


@pytest.mark.full
def test_materialized_views_survive_checkpointing(tmp_path):
    """A pandas view still has to reach Parquet in the Dask on-disk shape.

    `store_view` and `save_bottlenecks` both assumed a Dask frame. With
    materialisation on, a view arrives as pandas and neither `repartition` nor
    `npartitions` exists on it. The checkpoint has to keep one shape -- a
    Parquet directory with the `_metadata` file `has_checkpoint` looks for --
    so a pandas view is wrapped back into a single Dask partition.

    Runs cold then warm, and asserts a checkpoint was actually written so this
    cannot pass without exercising the path.
    """
    analyzer, trace_path = 'dftracer', 'tests/data/extracted/dftracer-posix'

    cold = _run(analyzer, trace_path, tmp_path / 'cold', 64 * 1024**2, checkpoint=True)

    checkpoints = list((tmp_path / 'cold' / 'pandas' / 'checkpoints').glob('_view_*'))
    assert checkpoints, 'no view checkpoint written; the pandas path was not exercised'

    warm = _run(analyzer, trace_path, tmp_path / 'cold', 64 * 1024**2, checkpoint=True)

    assert_frame_equal(cold, warm, check_like=True)
