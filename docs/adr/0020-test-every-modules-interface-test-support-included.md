# Test every module's interface, test support included

> The rules this decision sets are stated in [`docs/modules-and-tests.md`](../modules-and-tests.md).
> This ADR records why.

[ADR 0013](0013-test-modules-as-siblings.md) made library tests module tests, each suite checking
one module's interface. It left test support outside that rule: support modules sat in `tests`
subtrees beside the suites that used them, and had no suites of their own. It also banned naming a
private item of the parent, and sent every claim about another module's contract to that module's
suite. An architecture inventory and a design session on
[issue #269](https://github.com/hail-is/datafusion-sandbox/issues/269) re-checked these rules
against the code and changed four of them. Python, which the rules had not covered, now follows
them too.

## Decision

- **Test support has suites.** A test-support module is a module, with an interface hiding an
  implementation, so it gets a suite. A bug in plan observation or a fixture builder makes every
  suite that uses it pass or fail for the wrong reason, and is hard to find from those suites. A
  suite makes such bugs easier to find, and documents what the support promises. Two kinds of file
  have no interface of their own and are exempt: a module whose only items are child declarations,
  such as `testing` or a `tests` subtree root, and a suite's root file that holds only helpers for
  its concern files, which is part of the suite.
- **All test support lives under `testing`.** Support in a `tests` subtree would put its suite at a
  path like `src/tests/tests/plan_shape.rs`, and split support between two places by who uses it.
  Under `testing`, a support module is `cfg(test)` unless a crate test, benchmark, or example needs
  it, in which case it is public, and its suite follows ADR 0013's layout in `src/testing/tests/`.
  A helper or adapter used by one suite stays in that suite's files; once a second suite needs it,
  it moves into a module under `testing` and gets a suite.
- **A test may name a private item of its parent.** The ban's one argument was that a child's suite
  naming the parent's private items would couple to the parent's implementation. That coupling is
  cheap: a refactor of the parent that breaks it shows up as a compile error the refactor fixes.
  Tests still name the module's items through its path, and production code still carries no item
  kept for tests.
- **A property belongs in the suite of every module whose contract states it.** When a module relies
  on a property of a child, the child's suite tests it through the child; when the module also
  promises that property to its callers, its own suite tests it through the module. The old rule
  sent such a claim only to the child's suite, which left the parent's promise untested and
  invited tests that reach past the parent to check it. Duplicated tests are accepted while they
  are cheap. When a parent's tests grow expensive because of a child, a seam at the child lets the
  parent's suite use an adapter there.
- **Python modules have suites.** Each module under `python/src/hailtools/` has a suite,
  `python/tests/test_<module>.py`, whose tests do not name the module's `_private` items. Seams are
  parameters, injected callables, or registry lookups such as `PATH`. Patching a standard-library
  attribute through the module under test, such as `gce.subprocess.run`, fakes a dependency the
  module hard-wires, as faking one in Rust would. `python/batch/convert_mts.py` is exempt because
  its conversion code is copied from tested CLI code so that a cloud batch job needs only that
  file. `scripts/gce/` is exempt because it is infrastructure that runs only on GCE.

## Considered options

### Keep test support exempt

Support is only reached through suites, so their failures would catch its bugs eventually. We
rejected this because a support bug can as easily make a suite pass wrongly as fail, and the
support's contract is otherwise written down nowhere.

### Keep support beside its users in `tests` subtrees

This keeps support near the suites that use it, but nests suites of support under `tests/tests/`,
and makes support move between places when a crate test starts using it. One place, by concept,
is simpler to search.

## Consequences

`crate::tests::support`, `plan_shape`, and `formulation_plans` move under `crate::testing`, with
`support` split into one module per concept. Their suites, and those of the other support
modules, are [issue #296](https://github.com/hail-is/datafusion-sandbox/issues/296).

Some properties are now tested twice, once through a child and once through its parent. That cost
is the signal for a seam, not a reason to drop the parent's test.
