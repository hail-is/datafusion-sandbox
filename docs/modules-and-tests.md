# Modules and tests

The crate is a set of modules, each with a suite that checks the contract of its interface. This doc
states the rules for both. [ADR 0013](adr/0013-test-modules-as-siblings.md),
[ADR 0020](adr/0020-test-every-modules-interface-test-support-included.md), and
[ADR 0010](adr/0010-keep-tests-on-in-memory-object-stores.md) record why.

## Terms

- **Module**: any Rust module in the library, private children and test support included. A private
  child's callers are its parent's implementation. It has an interface and a suite like any other
  module, so the parent can be understood through its children's interfaces without reading their
  implementations. Aim for deep modules: a lot of behaviour behind a small interface.
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
| Module X | `tests/X.rs` in the `tests` subtree of X's parent, plus files under `tests/X/`, each named for a concern of the suite: `src/stored/tests/sorted_table.rs` and `src/stored/tests/sorted_table/filtered_scans.rs` |
| X's children | X's own `tests` subtree, one file per child, each continuing in a concern directory the same way: `src/stored/tests/dataset.rs` |

A `tests` subtree root, such as `src/stored/tests.rs`, starts with a module doc stating whose tests it
contains and where the parent's own tests belong. It declares the suites beneath it and holds no
tests itself. A parent adds its subtree when its first child gains a suite.

Every module has a suite, test support included. Two kinds of file have no interface of their own
and are exempt:

- a module whose only items are child declarations, such as `testing` itself or a `tests` subtree
  root;
- a suite's root file that holds only helpers for its concern files. That file is part of the
  suite, not a module with a contract.

A test that needs private access may sit in an inline child test module. That module starts with a
module doc stating why it needs private access and which interface its tests should move behind. The
binary's inline tests are exempt.

A crate test lives under `tests/` and is reserved for behaviour that needs the built binary.

Review check: every library module has a suite, apart from the two exemptions above. Each subtree
root carries its doc and holds no tests. Each inline child test module carries its doc.

## What a suite checks

Each test makes a claim about the contract of its own module's interface. It may use other modules to
build its inputs and observe its outputs. Tests name the module's items through its path.

A property belongs in the suite of every module whose contract states it. When a module relies on a
property of a child, the child's suite tests it through the child; when the module also promises
that property to its callers, its own suite tests it through the module. Such duplicated tests are
accepted while they are cheap. When a parent's tests grow expensive because of a child, consider a
seam at the child so the parent's suite can use an adapter there.

When a test needs to observe something, make it part of the interface, or test the claim through what
the interface already shows. Production code carries no item that exists only for tests to reach:
no `cfg(test)` accessor, test-only feature, or `doc(hidden)` item.

Review check: a test whose claim is in no contract of its module, and a production item kept for
tests, are each a violation.

## Seams and adapters

A test exercises every dependency its module hard-wires. At each seam it uses an adapter: a
production one where that works, otherwise a test adapter. `src/testing.rs` lists the test-support
modules, and `src/testing/fixture.rs` inventories the shared dataset fixtures; check them before
writing a new one.

All test support lives under `testing`: plan observation, session configs, constructors, adapters,
and fixtures. A support module is `cfg(test)` unless a crate test, benchmark, or example needs it,
since those compile as separate crates; then it is public, as `fixture`, `memory_store`, and
`generated` are. Items are placed in modules by concept, even when only suites use them. Each
support module's suite follows the layout above, in `src/testing/tests/`, as
`src/testing/tests/plan_shape.rs`.

A helper or adapter used by one suite stays in that suite's files. Once a second suite needs it, it
moves into a module under `testing` and gets a suite.

Review check: a production module that imports from `testing`, test support in a `tests` subtree
other than a helper inside one suite's files, and an item one suite's files import from another
suite's files are each a violation.

At the object store, every Rust test uses the in-memory adapter, except `src/tests/combiner_run.rs`
and `tests/cli.rs`, which use local disk ([ADR 0010](adr/0010-keep-tests-on-in-memory-object-stores.md)).
Any other test that touches the filesystem, directly or through a dependency its module hard-wires,
carries a comment saying why that is unavoidable, so a later reader can check whether the reason
still holds. Python tests under `python/tests` check tools that read files the Rust binary wrote,
and may write their fixtures under pytest's `tmp_path`.

Review check: a test outside the two disk files that reads or writes the filesystem without that
comment is a violation. `tempdir`, `TempDir`, or a filesystem path that is opened, listed, or
written is the sign to look for. A path string nothing dereferences is not a violation. The fix is
to move the test or switch it to the in-memory adapter. A comment arguing that the speed difference
is small does not count; ADR 0010 weighed that.

## Python

Each module under `python/src/hailtools/` has a suite, `python/tests/test_<module>.py`. Its tests do
not name the module's `_private` items. Seams are parameters, injected callables, or registry
lookups such as `PATH`. Patching a standard-library attribute through the module under test, such as
`gce.shutil.which` or `gce.subprocess.run`, is not a seam: it fakes a dependency the module
hard-wires.

Two parts of the Python tree are exempt:

- `python/batch/convert_mts.py` is a cloud batch driver. Its conversion code is copied from
  `hailtools/cli.py`, whose suite tests it, so that a job needs nothing but this file. The rest
  submits jobs and moves files in the cloud.
- `scripts/gce/` is infrastructure that provisions and runs GCE machines, and runs only on GCE.

Review check: a `hailtools` module without a suite, a test that names a `_private` item of its
module, and a test that patches a standard-library attribute through its module are each a
violation.
