# Shared metrics directory fixture

The tables the Rust writers in `src/metrics_directory.rs` write from fixed synthetic inputs: a
measured write `write-r1`, and a drained shadow probe `shadow-r1` replayed under a small grid. The
`probe_viewer` and `sweep_report` suites read them
([ADR 0021](../../../../docs/adr/0021-hold-the-rust-python-contracts-with-shared-fixtures.md)).

Do not edit the tables by hand. The module test `the_shared_metrics_directory_fixture_is_current`
in `src/tests/metrics_directory/shared_fixture.rs` fails when they are stale, and rewrites them
when run with `DATAFUSION_SANDBOX_REWRITE_METRICS_FIXTURE=1`:

```
DATAFUSION_SANDBOX_REWRITE_METRICS_FIXTURE=1 cargo test --lib shared_metrics_directory_fixture
```
