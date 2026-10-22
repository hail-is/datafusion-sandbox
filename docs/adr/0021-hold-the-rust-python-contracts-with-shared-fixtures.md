# Hold the contracts between Rust and Python with shared fixtures

Rust writes what the Python tools read, and each side states the contract in its own code. The
stopping rule's final step is copied into the probe viewer
([ADR 0019](0019-copy-the-stopping-rules-final-step-into-the-probe-viewer.md)). The metrics
directory's table and column names are hard-coded in both the writers (`metrics_directory` and its
children) and the readers (`probe_viewer` and `sweep_report`). Neither side's suite can see the
other side's code, so the two can drift apart while both suites pass. We hold each such contract
with a fixture both languages check. Decided for the metrics directory in
[issue #300](https://github.com/hail-is/datafusion-sandbox/issues/300). ADR 0019's stop fixture is
the first fixture of this kind.

## Decision

- **Rust writes the fixture with its real code from fixed synthetic inputs**, so its contents are
  deterministic. The fixture is checked in under `python/tests/fixtures/`.
- **A Rust module test fails if the checked-in fixture is stale.** The failure says how to
  regenerate the fixture: the stop fixture's test prints the regenerated JSON, and the metrics
  directory's test names `DATAFUSION_SANDBOX_REWRITE_METRICS_FIXTURE`, which rewrites the fixture.
  A fixture of Parquet files is compared by decoded schema and rows, not by bytes, since a
  Parquet footer records the writer's version. A test that reads or rewrites the fixture on disk
  carries the comment [ADR 0010](0010-keep-tests-on-in-memory-object-stores.md) requires.
- **The Python suites read the fixture through their real loading code.** Python test runs never
  need a Rust build first.
- **The code on each side names the fixture**, so whoever changes one side finds the check.

The metrics directory's fixture, `python/tests/fixtures/metrics_directory`, holds a measured
write's run record and a drained shadow probe's run record, progress samples, checks, replay
baselines and replays. It leaves out the run metrics table: no Python reader reads it, and its
values are the timings of the plan the test executes.

## Considered options

- **A CLI command that writes a synthetic metrics directory**, run before the Python tests. The
  data would always be current, but the command would be a production item kept only for tests,
  and every Python test run would need a Rust build.
- **Byte comparison of the Parquet files.** Simpler, but the test would fail whenever the Parquet
  writer's version changed, even if the tables were the same.
