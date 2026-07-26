# Handoff: darshan/recorder ownership + modernization

Working notes for picking this up on another machine. **Delete this file before
merging `chore/modernize-deps`** -- it is scaffolding, not documentation.

## Context

wisio is becoming the long-term home for darshan and recorder support as
dfanalyzer (LLNL) narrows to DFTracer. The work is: learn what dfanalyzer got
right, port the darshan/recorder fixes worth having, and make the whole thing
verifiable.

wisio keeps its own core -- analyzer interface, rule engine, console output.
dfanalyzer is a donor, not a template. Its core API changes (`ReadTraceResult`,
`compute_*`->`get_*`, preset/layer machinery, fact engine) are deliberately
**not** adopted.

**Useful habit:** when a migration problem comes up, check how dfanalyzer solved
it first -- they already did this migration (Python 3.9-3.12, numpy>=2,
dask>=2024.1). Their answer for `dd.core.Scalar` (below) was simpler than the
shim originally written here.

## Branch state

### `feat/darshan-recorder-hardening` -- DONE, in review

PR: https://github.com/grc-iit/wisio/pull/2 (5 commits, 58 tests green)

Guard fix for optional imports · unit + characterization tests · four ports from
dfanalyzer · DXT hostname fix · dftracer reader rewritten on dftracer-utils.
Verified byte-identical to pre-change baselines on all four fixtures.

### `chore/modernize-deps` -- IN PROGRESS, stacked on the above

Stacked deliberately: the characterization tests from PR #2 are the safety net
for this change. Rebase or merge PR #2 first.

## Where modernization stands

`pyproject.toml` now targets:

```
requires-python = ">=3.10"          # was ">=3.8, <3.11" (all EOL)
dask[...]>=2024.1.0                 # was ~=2023.4.0
numpy>=1.26                         # was ==1.24.3
pandas>=2.0,<3.0                    # bounded: pandas 3 defaults to CoW +
                                    #   string dtype, own migration
rich>=13.6                          # was ==13.6.0
scikit-learn>=1.3 / scipy>=1.10     # were ~=1.3.0 / ~=1.10.0
```

Classifiers updated to 3.10-3.13.

On Python 3.12 this resolves to dask 2026.7.1, numpy 2.5.1, pandas 2.3.3,
pyarrow 25.0.0, scipy 1.18.0, scikit-learn 1.9.0, rich 15.0.0 -- roughly a
three-year jump on dask.

### Fixed

**`dd.core.Scalar` no longer exists** (dask-expr rewrite moved it). 13 type
annotations across `types.py`, `scoring.py`, `rule_engine.py`, `analyzer.py`,
`rules.py`.

Fix follows dfanalyzer: plain **string annotations** `"dd.Scalar"`. Never
evaluated at import, so the attribute's location stops mattering and no
version shim is needed. (`dd.Scalar` does not exist in dask 2023.4 and was
still absent in 2024.12, so a hard import would have pinned the floor much
higher than necessary.)

Result: `import wisio` works, and **all 57 unit tests pass** on the modern stack.

### BLOCKED -- pick up here

**The Dask end-to-end path hangs.** `pytest -m "not full"` ran past 10 minutes
with no output; the pure-unit subset finishes in 7s. So the hang is in the
cluster path, not in analysis logic.

Not yet diagnosed. The next step is to run one analyzer directly and watch where
it stalls:

```bash
python -m wisio +analyzer=recorder percentile=0.99 \
  trace_path=tests/data/extracted/recorder-parquet hydra.run.dir=/tmp/m_rec
```

Leading suspects, in order:

1. **dask-expr query planning.** Default since dask 2024.3 and a genuine
   behavior change. `wisio/analyzer.py:4` still carries
   `dask.config.set({'dataframe.query-planning-warning': False})`, a
   transition-era workaround modern dask no longer recognizes. Custom
   `dd.Aggregation` objects in `wisio/utils/dask_agg.py` and the `.reduction()`
   calls are the most likely things to misbehave under it. Worth trying
   `dask.config.set({"dataframe.query-planning": False})` as a diagnostic to
   confirm the cause -- **not** as the fix, since that escape hatch is going away.
2. **LocalCluster startup.** Modern distributed changed defaults; a deadlock at
   cluster construction would look exactly like this. Check whether it hangs
   before or after the "Reading ... files" log line.
3. **`silence_logs: logging.CRITICAL`** in `LocalClusterConfig`
   (`wisio/config.py`) could be swallowing the real error.

Run with a timeout and `HYDRA_FULL_ERROR=1`; consider `cluster.processes=false`
to get a synchronous traceback.

## Verifying you have not broken anything

Golden values, captured pre-modernization on Python 3.10. Any change here is a
regression unless deliberate and explained.

| Fixture | job_time | total_count | bottlenecks |
|---|---|---|---|
| `darshan-dxt` | 76 | 1,953 | 20 |
| `recorder-parquet` | 667.808837890625 | 27,463 | 37 |
| `dftracer-raw` | 145.363989 | 231,337 | -- |
| `darshan-raw` (non-DXT) | -- | -- | 1 |

`tests/test_characterization.py` asserts these. The strongest check is a console
diff against a known-good run:

```bash
python -m wisio +analyzer=<a> percentile=0.99 trace_path=<fixture> \
  hydra.run.dir=/tmp/run_new
diff /tmp/run_baseline/result.txt /tmp/run_new/result.txt
```

dftracer additionally matches ground truth computed straight from the raw
`.pfw.gz` files: 231,337 POSIX events, 48 processes, 2,556 files, 690.06 MiB.

## Environment

The machine this was developed on had only Python 3.12/3.13 system-wide, so
`uv` supplied interpreters.

```bash
uv venv --python 3.12 .venv && source .venv/bin/activate
uv pip install '.[darshan,dftracer]' pytest
tar -xzf tests/data/<name>.tar.gz -C tests/data/extracted/<name>   # per fixture
```

The core build needs no C++ toolchain -- `wisio/meson.build` is pure
`py.install_sources`. Arrow/HDF5/MPI are only for `tools/`, off by default.

Test tiers: `pytest -m "not full"` (fast, ~15s) and `pytest -m full` (12 e2e
runs, several minutes).

## Remaining work, roughly prioritized

**Correctness**

1. **`acc_pat` is hardcoded to 0** for darshan DXT (`darshan.py:173,204`) and
   dftracer (`dftracer.py:247`), so the Access Pattern panel reports an
   unmeasured "100% sequential" for both. It is a fabricated statistic, not a
   missing one. The `offset` field ported in PR #2 supplies what DXT needs to
   compute it for real. recorder and non-DXT darshan report genuine values.
2. **dftracer hashes are unresolved** -- output shows `app#5692#3537780#3537780`
   instead of host and file names. Cheap now: `self.host_hash` / `self.file_hash`
   are populated in the new reader.
3. **Non-DXT darshan reports collapse** into one `app#localhost#0#0`. Needs a
   design decision first: `jobid` is identical across the four fixture reports,
   so identity has to come from the filename PID, report index, or start time.
   dfanalyzer has this defect too.

**CI / hygiene**

4. `tests/test_main.py:42` picks the smoke analyzer with `random.choice`, so CI
   tests a random analyzer per push.
5. CI matrix is 3.8/3.9/3.10 (all EOL) and needs to follow `requires-python`;
   `codecov-action@v3` is deprecated.
6. `LICENSE` is 0 bytes despite MIT being declared in `pyproject.toml`,
   `CITATION.cff` and the README.
7. Dead code: `wisio/analyzer_result.py` (62KB) and `wisio/cluster_manager.py`
   (3.9KB) are orphaned -- excluded from the meson install list, imported by
   nothing. `AnalyzerResultType` lives in `types.py`, not in `analyzer_result.py`.
8. Version is hand-bumped in three files; dfanalyzer uses `setuptools_scm`.
   `origin/feature/streamlit` is fully merged and deletable.

**Performance**

9. wisio takes 19.4s on the dftracer fixture vs dfanalyzer's 6.8s (~2.8x), at
   ~500MB vs ~286MB. The new reader is only 3.2s of that -- the analysis stage
   is the target. Worth revisiting after modernization, since three years of
   dask improvements land here.

## Note on dfanalyzer

A cross-check on the same trace found dfanalyzer **silently drops ~3/4 of the
data when running multi-worker**, which is its default configuration:

| | processes | events |
|---|---|---|
| default (4 workers) | 12 | 72,633 |
| `cluster.n_workers=1` | 48 | 284,041 |

With one worker its numbers match wisio and ground truth exactly (job time
145.363989 to six decimals). Reported to the dfanalyzer team. Relevant here only
as a caution: **do not treat dfanalyzer's default output as a reference** when
comparing results.
