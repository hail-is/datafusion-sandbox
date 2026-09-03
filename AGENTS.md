# datafusion-sandbox

## Agent skills

### Issue tracker

Issues live in GitHub Issues on `hail-is/datafusion-sandbox`, managed via the `gh` CLI. External pull requests are **not** a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles use their default names verbatim (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Tests

Test the library through its public API under `tests/it/`. Mirror the `src/` directory and module structure there, and register each top-level test module in `tests/it/main.rs`. In the library, reserve inline unit tests for behavior the public API cannot exercise. The binary is tested inline, so inline unit tests in the binary do not violate the mirror rule. Shared dataset fixtures are inventoried in the module doc of `tests/it/fixture/mod.rs`; check it before writing a new one. Only two test modules read a filesystem; see `CODING_STANDARDS.md`.
