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

### Usage docs

`docs/README.md` indexes one usage doc per subsystem. Before changing what a command does, read its doc, and update that doc in the same change. A new subsystem gets its own doc and an index line.

### Builds

For optimized local runs, use `--profile release-nonlto`. The fat-LTO `-r` profile runs out of memory linking on machines with 16 GB of RAM or less, such as the lima sandbox and cloud sessions; on larger machines it builds but links slowly, so use it there only when a run genuinely needs it.

### Code search

`ast-grep` beats grep when a match spans lines or varies in shape:
- a file's items: `ast-grep outline <file>`, not `grep -n '^pub\|^fn'`
- multi-line items (`use` lists, signatures with `pub(crate)`/`async`): `ast-grep --lang rust -p 'use $P;' src`
- call sites: `ast-grep --lang rust -p '$X.write_unordered($$$)' src`

Set `--lang` to the file's language. Attributes and decorators are separate nodes, so grep for `#[cfg(test)]`. Call `ast-grep`; on Linux `sg` is a different program.

### Cloud sessions

In a Claude Code cloud session (`CLAUDE_CODE_REMOTE=true`), `gh issue` and `gh pr` fail; use the `gh api` REST forms instead. See `docs/cloud-sessions.md`.

### Checks

While iterating, run `cargo check` and the tests you touched (`cargo test <filter>`) rather than the full check, which is slow from a cold build.

Before finishing, if the change touches Rust sources (`src/`, `tests/`, `benches/`, `examples/`, `Cargo.toml`) or `hail_df/`, run `cargo fmt --all` and then `scripts/check`, and fix what it reports. It runs the Rust checks, treating warnings as errors, which are what CI runs as `scripts/check --rust`, and then the `hail_df` step: clippy on the bindings crate, the extension built through maturin with `uv run`, and `hail_df`'s ruff and pytest checks. For a Python-only change under `hail_df/`, run `uv run --directory hail_df ruff format` first. If it touches `python/`, run `uv run --directory python ruff format`, `uv run --directory python ruff check --fix`, `uv run --directory python ruff format` again, and `uv run --directory python pytest`, and fix what `ruff check` reports.
