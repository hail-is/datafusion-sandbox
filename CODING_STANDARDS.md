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

## Only the combiner run and cli tests read a filesystem

`tests/it/combiner_run.rs` and `tests/it/cli.rs` may read and write disk. Every
other test holds its dataset fixtures in an in-memory object store and creates
no temporary directories. See
[ADR 0010](docs/adr/0010-keep-tests-on-in-memory-object-stores.md) for why.

The test: `tempdir`, `TempDir`, `CARGO_TARGET_TMPDIR`, or a filesystem path
appearing in a test outside those two files is a violation. The fix is to move
the test or switch it to an in-memory store, not to argue that the speed
difference per operation is small; the ADR already weighed that.
