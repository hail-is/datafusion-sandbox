# Coding standards

Rules reviewers enforce on changes to this repo.

## Glossary entries state what a term is

An entry in `CONTEXT.md` is a definition: one or two sentences naming the concept
and its boundary with neighboring terms, followed by an `_Avoid_` line listing
rejected synonyms. Links to ADRs for rationale are fine.

The test: the entry stays true if every type, function, and module in the
codebase is renamed or rewritten. Sentences about how the thing is constructed,
detected, validated, or tested fail that test; that material belongs in code
docs or an ADR, not the glossary.

Target style:

> **Locus**:
> A position in the genome: a contig together with a position within it. The unit
> both combiners order and group by.
> _Avoid_: site, coordinate, variant (a variant is a locus plus alleles)

Violation, for contrast: "A dataset detects the representation from its resolved
schema; detection requires every field the representation names." Both clauses
describe code behavior, not the concept.

## Library tests are sibling module tests

A module test is a `#[cfg(test)]` sibling of the module it tests, declared by the
module's parent. Its location states which module surface it checks. A crate test
lives under `tests/` and is reserved for behavior that needs the built binary.
"Module test" and "crate test" name locations; "unit test" and "integration test"
describe what a test claims.

The test: the tests of module X live in the `tests` subtree under X's parent and
name X's items through X's path. They do not name private items of the parent,
even though Rust permits it. An inline child test module in the library starts
with a module doc stating why it needs private access and which surface it should
eventually move behind. The binary is exempt from this inline-module rule.

See [ADR 0013](docs/adr/0013-test-modules-as-siblings.md) for the decision and
its tradeoffs.

## Only the combiner run and cli tests read a filesystem

`src/tests/combiner_run.rs` and `tests/cli.rs` may read and write disk. Every
other test holds its dataset fixtures in an in-memory object store and creates
no temporary directories. See
[ADR 0010](docs/adr/0010-keep-tests-on-in-memory-object-stores.md) for why.

The test: a test outside those two files that reads or writes the filesystem is
a violation. `tempdir`, `TempDir`, or a filesystem path that is opened, listed,
or written is the sign to look for. A path string that nothing dereferences,
such as one handed to a constructor to check how it is classified, is not a
violation. The fix is to move the test or switch it to an in-memory store, not
to argue that the speed difference per operation is small; the ADR already
weighed that.
