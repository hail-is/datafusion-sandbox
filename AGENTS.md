# datafusion-sandbox

## Agent skills

### Issue tracker

Issues live in GitHub Issues on `hail-is/datafusion-sandbox`, managed via the `gh` CLI. External pull requests are **not** a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles use their default names verbatim (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Tests

Library tests are module tests in `#[cfg(test)]` sibling modules declared by the tested module's parent. The tests of module X live under X's parent and name X's items through X's path, never through private items of the parent. A crate test under `tests/` is only for behavior that needs the built binary; today that is `tests/cli.rs`. An inline child test module in the library is the wrong default: use one only when private access is unavoidable, and start it with a module doc explaining why and naming the surface the tests should move behind. Inline tests in the binary are unaffected. Shared dataset fixtures are inventoried in `src/fixture.rs`; check it before writing a new one. Only two Rust test modules read a filesystem; see `CODING_STANDARDS.md`.

### Builds

For optimized local runs, use `--profile release-nonlto`. The fat-LTO `-r` profile runs out of memory linking in the lima sandbox; on the host it builds but links slowly, so use it there only when a run genuinely needs it.

### Checks

Before finishing, if the change touches Rust sources (`src/`, `tests/`, `benches/`, `examples/`, `Cargo.toml`), run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test`, and fix new warnings. If it touches `python/`, run `uv run --directory python pytest`.
