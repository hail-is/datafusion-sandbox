# Test modules are siblings of the modules they test

A module's visibility states its contract with other code at that point in the module tree. Tests
should check that contract without forcing it wider and without reaching into implementation details.
Cargo crate tests cannot do this for `pub(crate)`, `pub(super)`, or private child modules because they
sit outside the crate. Inline child test modules have the opposite problem: they can see every private
item in the module under test.

## Decision

Library tests are module tests. The tests of module X live in a `#[cfg(test)]` sibling module declared
by X's parent. A `tests` subtree mirrors the parent's children, with one file per tested child. The
test names X's items through X's path and does not name private items of the parent, even though Rust
allows a sibling to do so.

Each `tests` subtree root states whose tests it contains and where the parent's own tests belong. The
crate currently needs only the library root's `src/tests.rs` subtree. Nested parents add their own
subtree when one of their children gains tests.

A `tests` subtree may also hold shared test-support modules that are the tests of no child: plan
observation, session builders, and constructors several test modules use. They are `#[cfg(test)]` like
the tests beside them and keep the same rule, reaching tested modules through their paths and naming
no private item of the parent. Test support becomes public library code, as `fixture` is, only when a
crate test or benchmark needs it. Nothing under `tests/` or `benches/` observes a plan, so plan
observation stays inside the library.

A crate test lives under `tests/` and is reserved for behavior that needs the built binary. The two CLI
process tests in `tests/cli.rs` are the only crate tests. "Module test" and "crate test" describe where
a test lives and what it can see. "Unit test" and "integration test" describe the claim a test makes.
The combiner run module tests remain an integration suite because they exercise the stored-file
pipeline end to end.

Inline child test modules in the library remain permitted when a test must use private state. Such a
module starts with a module doc explaining why private access is needed and naming the module surface
the tests should eventually move behind. This rule does not apply to inline tests in the binary.

Shared dataset fixtures live in the public `fixture` module. Module tests, crate tests, and benchmarks
all use that implementation. The module is part of the public crate API because crate tests and
benchmarks compile as separate crates.

Library modules use self-named files such as `formulation.rs` beside `formulation/`, not
`formulation/mod.rs`. The `clippy::mod_module_files` lint checks this convention.

## Considered options

### Expose internals to crate tests

A test-only feature or `doc(hidden)` public item would let crate tests reach internal modules. We
rejected both because visibility annotations would stop describing the real contract. Tests would be
the reason an internal item remained externally reachable.

### Split the project into a workspace now

A crate boundary would enforce each module's test vantage point in the compiler. The current project
does not need that build-system split yet. A workspace remains the escalation path if review-enforced
sibling boundaries become unreliable.

Two dependencies can make a later split non-mechanical. A module's public signature must not expose a
`pub(crate)` type owned by another module. The fixture module also cannot become its own crate until
the format, locus, and pipeline modules have moved out of the root crate. A fixture crate that depends
back on the root would compile the root twice in tests and produce distinct copies of its types.

### Keep one tests subtree at the crate root

One flat subtree would work for today's top-level modules but would test future nested modules from the
wrong place. It would lose the `pub(super)` view their users have and make nesting a module more than a
file move.

### Forbid inline child test modules

Private access can be useful while a better module boundary is still being designed. We keep the
escape hatch, but require its reason and intended destination to be visible in the module itself.

## Consequences

`cargo test --lib` runs every library test without building the CLI binary. Moving or narrowing a
module no longer requires widening its interface for crate tests.

The compiler does not fully enforce the intended sibling boundary. A sibling can name private items
of its parent and any `pub(crate)` item through `crate::`. Review must check that each module test
reaches the tested module through its path. The old crate-test boundary enforced less useful access
more strictly; this decision trades that enforcement for the visibility each module's real users have.
