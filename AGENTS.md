# datafusion-sandbox

## Agent skills

### Issue tracker

Issues live in GitHub Issues on `hail-is/datafusion-sandbox`, managed via the `gh` CLI. External pull requests are **not** a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles use their default names verbatim (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Tests

Library tests are module tests in `#[cfg(test)]` sibling modules declared by the tested module's parent. The tests of module X live under X's parent and name X's items through X's path, never through private items of the parent. A crate test under `tests/` is only for behavior that needs the built binary; today that is `tests/cli.rs`. An inline child test module in the library is the wrong default: use one only when private access is unavoidable, and start it with a module doc explaining why and naming the surface the tests should move behind. Inline tests in the binary are unaffected. Shared dataset fixtures are inventoried in `src/fixture.rs`; check it before writing a new one. Only two test modules read a filesystem; see `CODING_STANDARDS.md`.
