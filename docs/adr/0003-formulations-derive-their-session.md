# Formulations derive the session their plan shape needs

A formulation's plan shape depends on session settings: `target_partitions` for the union
formulations, and `target_partitions` together with `optimizer.preserve_file_partitions` for the
one-scan formulation. A plan built under the wrong session returns identical rows through a
substantially slower plan, so the mistake is invisible without a plan-shape assertion. Rather than
have callers pair each plan builder with a matching `SessionConfig` — a pairing that was re-done
independently in `main.rs` and in `tests/plan_shape.rs` — each plan builder derives its own session
from the one it is handed, overriding only the settings its shape depends on and inheriting the
rest.

The derivation clones the caller's *state*, mutates the clone's config in place, and wraps the
result: `SessionContext::state` already returns an owned `SessionState` clone that shares the
caller's `Arc<RuntimeEnv>` and catalog list, so registered object stores and the file-statistics
cache reach the derived session and there is one memory pool rather than two.
`SessionContext::new_with_state` wraps a state without touching it, so nothing re-registers a
catalog over the shared catalog list.

Sharing the state is what must not happen. `SessionContext` is `Clone` over a single
`Arc<RwLock<SessionState>>`, so cloning the context aliases it rather than copying it, and an
override applied through the clone would reach the caller. Cloning the state instead leaves the
caller's `SessionConfig` holding the same `Arc<ConfigOptions>`, so `options_mut` — which is
`Arc::make_mut` — copies rather than mutating in place. `tests/plan_shape.rs` asserts this directly,
because no plan-shape assertion can: a leaked override reshapes whatever is built next, not the plan
that leaked it.

## Considered options

**Building the derived session with `SessionStateBuilder::new_from_existing`,** which the spec for
#48 called for. It reaches the same result but has to work around itself: the builder clears
`create_default_catalog_and_schema`, so re-applying a config copied from the caller restores it and
`build` re-registers a default catalog over the shared catalog list, and the derivation has to clear
the flag again explicitly. Cloning the state skips both the rebuild and the workaround.

**Overriding settings on a clone of the caller's context**, the terse form of this idea. It does not
compile — `state()` hands back an owned clone whose `config()` is immutable — and the form that does
compile mutates the state the clone shares with the caller.

**A shared type pairing each plan builder with its session config.** This makes a wrong pairing
unrepresentable, but the pairing still exists to be reasoned about, and the session has to be built
before the dataset is known — one-scan's `target_partitions` is the sample count — which forces
object-store construction and both runtimes out of `pipeline::run` and into its callers.

**Declaring partitioning explicitly**, via `ListingOptions::with_output_partitioning`. This makes
plan shape genuinely independent of session config rather than insulated from it, and is the
intended end state (#44). Deferred because the declared path applies partition filters after file
grouping instead of at listing time, so the union formulations would give up the list-time pruning
they currently get for free. That trade should be measured rather than assumed.

## Consequences

The settings a formulation overrides are exactly its remaining config dependencies, listed in one
place. #44 removes them one at a time; when the list is empty, the derivation goes with it.
