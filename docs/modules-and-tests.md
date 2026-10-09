# Modules and tests

The crate is a set of modules, each with a suite that checks the contract of its interface. This doc
states the rules for both. [ADR 0013](adr/0013-test-modules-as-siblings.md) and
[ADR 0010](adr/0010-keep-tests-on-in-memory-object-stores.md) record why.

## Terms

- **Module**: any Rust module in the library, private children included. A private child's callers
  are its parent's implementation. It has an interface and a suite like any other module, so the
  parent can be understood through its children's interfaces without reading their implementations.
  Aim for deep modules: a lot of behaviour behind a small interface.
- **Interface**: everything a caller must know to use a module: its signatures, and the invariants,
  ordering constraints, error modes, required configuration, and performance characteristics the
  signatures don't show. It covers what the module requires of its callers as well as what it
  provides.
- **Contract** of an interface: the logical properties callers rely on beyond what the compiler
  checks. A suite partly verifies it.
- **Suite**: the module tests of one module.
- **Seam**: a dependency chosen outside the module, so its behaviour can change without editing the
  module. Examples are a trait object, a generic parameter, a callback, or a registry lookup such as
  the session's object store registry. A seam carries behaviour. A configuration value is a
  parameter, though it can select the adapter at a seam elsewhere. A dependency the module
  hard-wires is part of its implementation.
- **Adapter**: a concrete implementation that fills a seam, such as the in-memory object store or a
  test file format.

"Module test" and "crate test" name where a test lives. "Unit test" and "integration test" describe
what a test claims.

## Where suites live

| Suite of | Lives in |
| --- | --- |
| Module X | `tests/X.rs` in the `tests` subtree of X's parent, plus files under `tests/X/`, each named for a concern of the suite: `src/tests/sorted_table.rs` and `src/tests/sorted_table/filtered_scans.rs` |
| X's children | X's own `tests` subtree, one file per child, each continuing in a concern directory the same way: `src/stored/tests/dataset.rs` |

A `tests` subtree root, such as `src/stored/tests.rs`, starts with a module doc stating whose tests it
contains and where the parent's own tests belong. It declares the suites and support modules beneath
it and holds no tests itself. A parent adds its subtree when its first child gains a suite.

A test that needs private access may sit in an inline child test module. That module starts with a
module doc stating why it needs private access and which interface its tests should move behind. The
binary's inline tests are exempt.

A crate test lives under `tests/` and is reserved for behaviour that needs the built binary.

Review check: every library module has a suite, apart from the test support described below. Each
subtree root carries its doc and holds no tests. Each inline child test module carries its doc.

## What a suite checks

Each test makes a claim about the contract of its own module's interface. It may use other modules to
build its inputs and observe its outputs; a claim about another module's contract belongs in that
module's suite. Tests name the module's items through its path. They name no private item of the
parent, even though Rust permits it.

When a test needs to observe something, make it part of the interface, or test the claim through what
the interface already shows. Production code carries no item that exists only for tests to reach:
no `cfg(test)` accessor, test-only feature, or `doc(hidden)` item.

Review check: a test whose claim concerns another module, a test that names a private item of the
parent, and a production item kept for tests are each a violation.

## Seams and adapters

A test exercises every dependency its module hard-wires. At each seam it uses an adapter: a
production one where that works, otherwise a test adapter. `src/fixture.rs` inventories the shared
dataset fixtures and adapters; check it before writing a new one.

Test support lives in a `tests` subtree, beside the suites that use it, and is `cfg(test)` like them.
Examples are plan observation, session builders, and constructors several suites share. It becomes
public library code, as `fixture` and `generated` are, only when a crate test or benchmark needs it.

At the object store, every Rust test uses the in-memory adapter, except `src/tests/combiner_run.rs`
and `tests/cli.rs`, which use local disk. Python tests under `python/tests` check tools that read
files the Rust binary wrote, and may write their fixtures under pytest's `tmp_path`.

Review check: a test outside the two disk files that reads or writes the filesystem is a violation.
`tempdir`, `TempDir`, or a filesystem path that is opened, listed, or written is the sign to look
for. A path string nothing dereferences is not a violation. The fix is to move the test or switch it
to the in-memory adapter, not to argue that the speed difference is small; ADR 0010 weighed that.
