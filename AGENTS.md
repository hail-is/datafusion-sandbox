# datafusion-sandbox

## Agent skills

### Issue tracker

Issues live in GitHub Issues on `hail-is/datafusion-sandbox`, managed via the `gh` CLI. External pull requests are **not** a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles use their default names verbatim (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Modules and tests

The crate is a set of modules, each with a suite of module tests that checks the contract of its interface. Tests exercise everything a module hard-wires and use adapters only at its seams. Before adding a module, a test, or a test adapter, read `docs/modules-and-tests.md`.

### Builds

For optimized local runs, use `--profile release-nonlto`. The fat-LTO `-r` profile runs out of memory linking in the lima sandbox; on the host it builds but links slowly, so use it there only when a run genuinely needs it.

### Checks

Before finishing, if the change touches Rust sources (`src/`, `tests/`, `benches/`, `examples/`, `Cargo.toml`), run `cargo fmt`, `cargo clippy --all-targets`, and `cargo test`, and fix new warnings. If it touches `python/`, run `uv run --directory python pytest`.
