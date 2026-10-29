# Build Hail plans with datafusion-python and run them in our own backend

A Hail on DataFusion is a Python library, the frontend, over a Rust backend. The frontend lowers
Hail operations to DataFusion logical plans with the stock PyPI `datafusion` wheel
(datafusion-python), and hands them to the backend as unoptimized plan bytes beside an Action. The
backend runs every plan with its own session, optimizers, runtimes and sinks. The wheel's engine
never executes a plan. Charted on the map
[Evaluate datafusion-python as the frontend of Hail-on-DataFusion](https://github.com/hail-is/datafusion-sandbox/issues/311),
with datafusion-python 55.0.0 (built on DataFusion 55.2) and this crate on DataFusion 55.1. Nothing
here is built yet. A spec for an MVP Python frontend starts from this ADR. Each decision below
gists its ticket, which holds the detail and the evidence.

## Decision

### The seam

Decided in
[How do plans get from the frontend to the backend?](https://github.com/hail-is/datafusion-sandbox/issues/313).

- **The frontend lowers directly to DataFusion.** Hail's `Table` and `Expr` methods call
  datafusion-python's `DataFrame` and `Expr` APIs, so a frontend plan is a DataFusion logical plan
  from the start.
- **Hail-owned entry points carry `df.logical_plan().to_bytes(ctx)` and an Action.** The plan
  crosses unoptimized, so the backend's own analyzer and logical rules see the frontend's exact
  shape. A query-planner hook, so that a raw `df.collect()` reaches the backend, may come later as
  a convenience. It is not the main path.
- **Scans travel as descriptions.** Opening a dataset is a backend call. It returns a description
  (dataset path, format, declared row ordering, input table) with its schema and ordering, and the
  frontend wraps that in a schema-only table provider that can be planned against but refuses to
  scan. Our codec writes the description as the provider's payload. The backend decodes each scan
  synchronously into a placeholder holding its description, then reopens it in an async pass that
  re-lists files
  ([#321](https://github.com/hail-is/datafusion-sandbox/issues/321)).
- **Our codec has a stable `__datafusion_codec_id__`.** The backend's codec strips the wheel's
  `DFPYCHN` wrapper and checks that id. Our codec returns an error for providers it doesn't own and
  writes no payload for functions, so they cross by name
  ([#317](https://github.com/hail-is/datafusion-sandbox/issues/317)).
- **Our cdylib ships in the client.** It holds the codec, the schema-only provider, the
  `assert_ordered` UDF and the in-process backend. Our extensions are one crate that builds both as
  an FFI cdylib and as a linkable library.
- **In-process first, remote later.** The in-process call is the remote seam without the transport.
  It keeps nine rules so a remote backend stays open
  ([#320](https://github.com/hail-is/datafusion-sandbox/issues/320)):
  1. Plan bytes are the only plan representation that crosses, in-process too.
  2. Every scan is a description any process can reopen.
  3. The backend owns its session. A call carries the plan bytes, the Action and an explicit
     settings map in our namespace, which rejects unknown keys.
  4. The backend resolves object stores and credentials from URLs.
  5. Functions cross by name, with Python-UDF inlining off.
  6. The Action is a plain serializable value, separate from the plan, with no callbacks.
  7. Outcomes come back as data after the run. The frontend never gets a live plan.
  8. Runs have ids, and cancelling is an explicit backend call.
  9. Every call carries and checks the DataFusion major and the `DFPYCHN` wire version.

### Frontend plans

Decided in
[Which logical nodes may frontend plans use?](https://github.com/hail-is/datafusion-sandbox/issues/314).

- **Stock logical nodes only.** Hail-specific behaviour lives in scans, sinks, Rust UDFs, backend
  logical rules that recognise stock-shaped patterns, and the backend's physical planning.
- **A custom logical node is the escape hatch.** The wheel can't carry one in either direction, so
  the first one forces our own build of datafusion-python, a fork with our extensions linked in, or
  a SedonaDB-style package of our own. To keep that switch cheap, Hail's Python imports DataFusion
  through one internal alias module.
- **Lowering emits semi and anti joins**, never `IN (subquery)` or `EXISTS`, which unoptimized plan
  bytes can't encode ([#317](https://github.com/hail-is/datafusion-sandbox/issues/317)).
- **Versions move in lockstep.** The wheel, the backend crate and later the remote binaries share
  one DataFusion major. A DataFusion upgrade waits for datafusion-python's release, about a month
  at the median, and for `vortex-datafusion` and our fork of it.

### Ordering

Decided in
[How do lowered plans state key order?](https://github.com/hail-is/datafusion-sandbox/issues/315)
and confirmed in
[Can an ordering assertion hold a key order through the backend and fail when it can't?](https://github.com/hail-is/datafusion-sandbox/issues/322).

- **A frontend plan says two things about order.** A sort establishes a key order, and a real sort
  is expected. An ordering assertion states that a key order already holds there.
- **An ordering assertion is a stock-shaped marker,**
  `Filter(assert_ordered(<key columns>, '<direction>', …))`, whose UDF is registered on both sides
  under the same name. A backend pass before the logical optimizer replaces it with a backend-only
  node. That node requires one partition in the asserted order, as a sink's requirement does, so
  the optimizer builds a merge tree beneath it. The one partition holds only until DataFusion gains
  better support for range partitioning (below).
- **A failed assertion is always an error, raised before execution.** The node's
  `check_invariants` runs on the final physical plan. It walks down `maintains_input_order` edges
  and through unions and merges, fails on any sort the backend inserted, and fails closed on any
  non-leaf node that keeps no input's order. Reordering whole files by their statistics is not a
  sort.
- **Every frontend sort is an established order.** The backend wraps it in a node with the same
  requirement as an assertion that accepts the sort beneath it. So a frame may end in a sort and
  keep its merge tree, and only a sort the backend inserted fails.
- **The sink's ordering is an assertion at the root.** The Action carries the ordering as plain
  sort columns, `(column, ascending, nulls_first)`. The backend places an assertion at the root
  from it and hands it to the sink, which writes it as file metadata and keeps no requirement of
  its own.
- **One partition at every assertion, for now.** DataFusion 55 has no range partitioning that
  carries a global order. Per-partition range partitioning, so a mid-plan assertion doesn't force
  a merge, is planned as soon as possible.
- **The backend prefers ordered operators on ordered inputs.** Configuration alone gets streaming
  aggregation, windows over the key order and merges of ordered unions. Our own physical rules are
  needed to choose sort-merge join per join from the input orderings, to run it at one partition,
  to order full outer joins, and for interval joins
  ([#318](https://github.com/hail-is/datafusion-sandbox/issues/318)).
- **No mitigations against DataFusion believing an order that doesn't hold.** We assume the bugs
  are fixed before production and track them in
  [Track the upstream DataFusion bugs the ordering assertion relies on being fixed](https://github.com/hail-is/datafusion-sandbox/issues/325).
  The backend's tests run with the run-time order check (`verify_order`) on, and each bug gets an
  expected-failure test.

### Running a plan and its Outcomes

Decided in
[How do a run's Outcomes come back to the Python frontend?](https://github.com/hail-is/datafusion-sandbox/issues/326).

- **The seam is start, wait and cancel.** `start(plan_bytes, action)` returns a handle, `wait()`
  returns the Outcome, and `cancel(run_id)` cancels. `wait` releases the interpreter and checks for
  signals, and the frontend turns Ctrl-C into a cancel. Dropping a handle doesn't cancel a run.
  Each run has one id, supplied by the caller or generated.
- **Collected rows arrive once the run ends, through the Arrow C stream interface.** Nothing
  streams during a run; large results go through writes.
- **Run records and run metrics are written to the metrics directory, not returned.** A new
  run-record `status` (`completed`, `cancelled`, `failed`) records cancelled and failed runs too.
- **Rendered plans are text**, with the decoded frontend plan as a section beside the logical and
  physical plans.
- **Failures cross as stable error codes,** mapped to `BackendError` and its subclasses
  `OrderingAssertionFailed`, `RunCancelled` and `InvalidAction`. A cancelled or failed run has no
  Outcome, only the exception.
- **No progress reporting in the MVP.** The seam leaves room for a polled status call.

### Python UDFs

Decided in
[May backend plans contain Python UDFs?](https://github.com/hail-is/datafusion-sandbox/issues/316)
and checked in
[What would running datafusion-python's pickled Python UDFs in our backend take?](https://github.com/hail-is/datafusion-sandbox/issues/319).

- **Not in the MVP, and no backend design may rule them out.** Hail expressions lower to DataFusion
  expressions and our Rust UDFs.
- **Four rules keep the option open.** The entry point releases the GIL around `pipeline::run`.
  Our cdylib stays free-threading-safe (pyo3 0.28 or later, `gil_used = false`). The backend
  refuses inlined `DFPYUD*` payloads with a clear error. A remote worker may link libpython or own
  Python processes.
- **When they're built,** UDFs cross by name, with Hail carrying the callables beside the plan
  bytes in a format it owns, or as live callables in-process. They run on their own pool, so a
  worker blocked on Python doesn't stall the merge tree.

## Assumptions and their evidence

**Confirmed by experiment or prototype:**

- **Our codec takes part in `to_bytes(ctx)`, and scans cross as our payload with the wheel never
  scanning them.** Ran from the 55.0.0 wheel to a backend on 55.1
  ([#317](https://github.com/hail-is/datafusion-sandbox/issues/317)).
- **An FFI-imported UDF crosses by name, and the backend resolves it from its own registry.** The
  `assert_ordered` filter decoded on 55.1 and survived the backend's optimizer
  ([#317](https://github.com/hail-is/datafusion-sandbox/issues/317)).
- **The wheel on DataFusion 55.2 and a backend on 55.1 interoperate** over FFI and protobuf, with
  no version bump ([#321](https://github.com/hail-is/datafusion-sandbox/issues/321)). Lockstep is
  by major; minors needn't match.
- **A plan built in Python plans as the Rust plan builder's does.** Grouped-merge matched in 48 of
  48 cases: identical physical plans, no sort, byte-identical files
  ([#321](https://github.com/hail-is/datafusion-sandbox/issues/321)).
- **An ordering assertion shapes the plan as the sink's requirement does, and fails on an inserted
  sort.** 240 of 240 cases matched, including frames ending in a sort; an order the data lacks and
  a computed key both failed at planning
  ([#322](https://github.com/hail-is/datafusion-sandbox/issues/322)).
- **Python UDFs can run in a Rust backend later.** With inlining off they cross by name
  ([#317](https://github.com/hail-is/datafusion-sandbox/issues/317)). Python-bound UDFs scale to
  3.5–3.7× at 4 workers on free-threaded 3.14t, and stay at 1.0× with the GIL
  ([#319](https://github.com/hail-is/datafusion-sandbox/issues/319)).

**Confirmed by reading source only:**

- **A remote backend can adopt Ballista's transport** if the seam keeps its nine rules
  ([#320](https://github.com/hail-is/datafusion-sandbox/issues/320)).

**Known obstacles and their workarounds:**

- **Logical nodes and logical rules can't cross FFI, and physical properties, the CPU/IO runtime
  split and sinks don't survive the wheel's engine**
  ([#312](https://github.com/hail-is/datafusion-sandbox/issues/312)). Workaround: the wheel never
  executes, and the backend applies its rules to decoded stock nodes. At worst, the own-build
  escape hatch.
- **Subquery predicates can't be encoded unoptimized.** Workaround: lower to semi and anti joins.
  Optimizing before encoding is a considered option below.
- **The `DFPYCHN` wrapper is a wire format of the wheel's own**, which changed in 55.0. Workaround:
  the backend unwraps it and checks its version, under lockstep versions.
- **Inlined Python UDFs are opaque to a Rust backend.** Workaround: inlining off, and the backend
  refuses inlined payloads.
- **No stock operator keeps key order through an interval join, a full outer join, or a
  sort-merge join at one partition.** Interval joins are stock-expressible as an equality join on
  contig plus a range filter. Workaround: a backend physical rule and our own operator; at worst,
  the own-build escape hatch
  ([#318](https://github.com/hail-is/datafusion-sandbox/issues/318),
  [#323](https://github.com/hail-is/datafusion-sandbox/issues/323)).
- **DataFusion can claim an order that doesn't hold**
  ([apache/datafusion#25052](https://redirect.github.com/apache/datafusion/issues/25052), reproduced;
  [apache/datafusion#26745](https://redirect.github.com/apache/datafusion/issues/26745)), so an
  assertion passes over misordered rows. Its optimizer rules also guess wrong about custom nodes
  ([apache/datafusion#26697](https://redirect.github.com/apache/datafusion/issues/26697)), and both
  of ours are custom. Workaround: none in production; the run-time check in tests and the tracking
  issue above.
- **Two contradicting stacked assertions fail with DataFusion's own unclear planning error**, from
  a gap in the guard
  [apache/datafusion#11678](https://redirect.github.com/apache/datafusion/pull/11678) added. It
  fails rather than misorders; a report is drafted, and the issue above tracks it.
- **Every assertion forces one partition.** Workaround until range partitioning: a merge at each
  assertion.
- **An ordering on a struct column and orderings on its fields don't imply each other in
  DataFusion 55**, except that fields built by `named_struct` keep their columns' order
  ([#318](https://github.com/hail-is/datafusion-sandbox/issues/318)). Workaround: declare and
  assert orderings on flat columns, as
  [ADR 0009](0009-declare-locus-ordering-independently-of-representation.md) does.

## Considered options

- **Letting the wheel execute, with our extensions over FFI.** A foreign sink's ordering
  requirement is invisible to the host optimizer, so there is no merge tree. The host's runtime
  polls the plan, so host-built operators run off our CPU runtime, and analyzer and logical rules
  can't be installed ([#312](https://github.com/hail-is/datafusion-sandbox/issues/312)).
- **A query-planner hook as the main path**, as Ballista's Python client does. The wheel optimizes
  first, so the backend's logical rules see an optimized plan, and `COPY` plans need a patch that
  mirrors private FFI layouts. Ballista's client also can't carry our scans
  ([#320](https://github.com/hail-is/datafusion-sandbox/issues/320)).
- **Running the wheel's logical optimizer before encoding**, with
  `df.optimized_logical_plan().to_bytes(ctx)`, which would decorrelate `IN` and `EXISTS` into
  joins that encode. Rejected by reasoning, not tested. The backend's ordering pass runs before its
  logical optimizer because it needs the frontend's plan as written: it looks for each
  `assert_ordered` filter where the frontend put it, and wraps each sort the frontend wrote. The
  wheel's filter pushdown merges adjacent filters and moves them through projections and joins,
  which would break a marker or move the assertion, and its rules could remove or move a frontend
  sort. Every plan would also be shaped by two optimizers, the wheel's on its own DataFusion
  version and session. And a Hail lowering has no SQL to decorrelate: its semi joins, anti joins
  and membership filters lower to joins directly. datafusion-python can't run only the
  decorrelation rules, since it can't select or add optimizer rules.
  If DataFusion's optimizer comes to preserve the orderings a plan states, so that the backend no
  longer needs a pass before logical optimization to read them, this is worth reconsidering. Plans
  with subqueries would then encode, which could extend what Hail can express.
- **Our own build of datafusion-python from the start.** It removes every FFI limit, but it is a
  fork whose Python layer hard-codes its module name, clashes with the stock wheel, and leaves us
  the wheel matrix ([#312](https://github.com/hail-is/datafusion-sandbox/issues/312)). Kept as the
  escape hatch.
- **Ordering only at the sink.** It covers only a plan's output; ordered operations inside a plan
  would sort silently ([#315](https://github.com/hail-is/datafusion-sandbox/issues/315)).
- **Frontend frames that never end in a sort**
  ([#321](https://github.com/hail-is/datafusion-sandbox/issues/321)). Established orders made the
  rule unnecessary.
- **Collected rows as a datafusion-python `DataFrame`**, which puts an executing wheel session back
  in the frontend, or as IPC bytes, which copy in-process for no reason
  ([#326](https://github.com/hail-is/datafusion-sandbox/issues/326)).

## Consequences

When the backend is built:
- [ADR 0014](0014-hold-the-merge-tree-with-the-sinks-ordering-requirement.md)'s sink requirement
  becomes an ordering assertion at the root, and sinks take a plain sort list rather than a stored
  ordering.
- `pipeline::run` ([ADR 0006](0006-run-every-plan-through-the-pipeline-runner.md)) gains a
  cancellation token, and its caller releases the GIL.
- The run record ([ADR 0016](0016-record-run-metrics-as-wide-parquet-tables.md)) gains `status`,
  and cancelled and failed runs are recorded before teardown.

Out of scope here, each for a later effort: lowering MatrixTables (to tables, in Python),
implementing Python UDFs, moving runners and the Rust CLI onto the backend, packaging a Hail
frontend, the remote backend's form, building ordered interval joins, and Hail types as Arrow
types.
