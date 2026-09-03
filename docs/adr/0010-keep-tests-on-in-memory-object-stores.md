# Keep tests on in-memory object stores

Two test modules read a filesystem, and no other test does. The combiner run tests read disk
dataset fixtures because they are the integration suite: they own the claim that the pipeline works
against stored files, and nothing makes that claim if they hold their data in memory. The cli tests
read disk because they run the built binary as a separate process, which cannot see an object store
living in the test's memory, and they cannot move inline into the binary because Cargo sets
`CARGO_BIN_EXE_<name>` only for integration tests and benches. That is a constraint, not an
oversight. Every other test holds its dataset fixtures in an in-memory object store.

There are two reasons, and the first carries more weight. The layering reason: the dataset, format,
formulation, and pipeline tests assert on schema inference, footer parsing, listing, file
statistics, and plan shapes, and an in-memory store exercises all of those. None of them asserts
anything a filesystem does that the store does not, so a temporary directory in one of them adds a
dependency the test never checks. The speed reason: the same fixture operations measured 9 to 11%
slower on the local filesystem than in memory. Small per operation, but fixture builds run in every
test binary, so the cost lands on every edit-test loop, and a suite fast enough that running it is
not a decision is the point of the fixture work.

## Consequences

A test reaching for a temporary directory outside `tests/it/combiner_run.rs` and `tests/it/cli.rs`
is either in the wrong module or making a claim about real storage it has not argued for, and the
review question is which. The speed difference being small per operation is not an argument for
moving a test to disk; it was small when this decision was made, and the decision weighed it.
