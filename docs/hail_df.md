# `hail_df`

`hail_df` is the Python frontend of Hail-on-DataFusion
([ADR 0022](adr/0022-build-hail-plans-with-datafusion-python-and-run-them-in-our-backend.md)). A
frontend builds plans with datafusion-python's own `DataFrame` API, on the stock
`datafusion==55.0.0` wheel, against datasets an in-process Rust backend opens. The backend is
`src/backend.rs` in the root crate; `hail_df/` holds the bindings crate over it, the Python package
and its tests.

A frontend opens a dataset, plans against it, starts a run in the backend, and waits for its Outcome.
The scans in a plan are scan descriptions, which plan like tables but refuse to scan in Python, so
only the backend ever runs a plan.

## Building it

`hail_df` is a maturin mixed project with its own uv project and lock, independent of `python/`
and its `hail` pins. Its wheel is abi3, for Python 3.13 and later.

```sh
uv run --directory hail_df maturin develop --uv   # build the extension into the environment
uv run --directory hail_df pytest
```

`maturin develop` builds the extension with the `release-nonlto` profile, as does a wheel built
through the PEP 517 backend, such as with `pip install ./hail_df`: the fat-LTO `release` profile runs
out of memory linking on 16 GB machines. It installs the package editable, with the extension copied
into `hail_df/python/hail_df/`. `uv sync` installs only the dependencies: the project sets
`package = false`, so uv never builds the extension itself.

`scripts/check` builds the extension the same way and runs `hail_df`'s clippy, ruff and pytest
checks after the Rust checks.

The bindings crate `hail_df` is a member of the root Cargo workspace, so it shares the lock and the
target dir. pyo3 needs a Python 3.13 or later to build against, so run its cargo commands under
`uv run --directory hail_df`, which puts the project's interpreter first.

## Opening a dataset

```python
import hail_df

backend = hail_df.Backend()
dataset = backend.open_dataset("gs://bucket/samples", "vortex", "locus,alleles")
```

`Backend()` takes no arguments. It resolves object stores and credentials from URLs: a local path or
a `file://` URL needs nothing, and a `gs://` URL reaches its bucket with credentials from the
environment, as the CLI does.

`open_dataset(path, input_format, row_ordering)` discovers the dataset as the CLI does (see
[Combiner runs](combiner.md)). `input_format` is `parquet` or `vortex`. `row_ordering` is the
order the rows are declared in: `locus`, `locus,alleles` or `locus,s`. The opened `Dataset` has:

- `input_tables`: an `InputTable` per input table, in name order, with its `name`, its
  `sample_set` and its `scan_description`.
- `ordering`: the stored ordering as plain sort columns, `SortColumn(column, ascending,
  nulls_first)` tuples, every column ascending with nulls last.
- `schema`: the `pyarrow` schema every scan reads, the dataset's rows with their sample `s`.

A `ScanDescription` names the `dataset_path`, `input_format`, `row_ordering` and `input_table` a
scan reads, enough for any backend to reopen it.

A dataset that can't be discovered, an unknown format or row ordering, and rows that lack a column
of the row ordering each raise `hail_df.BackendError`.

Every backend call carries the DataFusion major the wheel builds plans on and the version of the
`DFPYCHN` frame it wraps codec payloads in, `hail_df.datafusion.DATAFUSION_MAJOR` and
`WIRE_VERSION`, and the backend refuses a call whose versions aren't its own with `InvalidAction`, a
`BackendError`. A `datafusion` wheel of another major than the backend's fails every call rather than
misdecode.

## Reading it into a plan

```python
from datafusion import col

ctx = hail_df.context()
frame = dataset.read(ctx)                       # every input table: the union of their scans
group = dataset.read(ctx, ["g0", "s=HG02230"])  # some, in the order named
keys = [col(c).sort(ascending=a, nulls_first=n) for c, a, n in dataset.ordering]
plan_bytes = group.sort(*keys).logical_plan().to_bytes(ctx)
```

`hail_df.context()` returns a fresh datafusion-python `SessionContext` with the backend's codec
installed and the `assert_ordered` function registered. Plans over a dataset's scans encode only in
such a session. Keep the context alive while its frames are in use: the wheel's frames reach their
session through it.

`Dataset.read(ctx, names=None)` returns a `DataFrame` over the named input tables: the union of
their scans, or the single scan of one. With no names it reads every input table, in name order. An
empty list, a repeated name, or a name the dataset doesn't hold raises `ValueError`. Executing the
frame in the wheel, such as with `frame.collect()`, fails: only the backend runs a plan over scan
descriptions.

`backend.scan_descriptions(plan_bytes)` reads the plan bytes back and returns the scan
descriptions the plan reads, each once, in the order the plan first reads them. Bytes that are not
such a plan raise `BackendError`, and a plan carrying a payload the backend refuses raises
`InvalidAction`.

## Asserting an order mid-plan

`hail_df.assert_ordered(frame, ordering)` marks a point in a plan where rows already arrive in an
order, given as plain sort columns. The backend holds the order there with a merge tree, and fails
before execution if only a sort it inserted could meet it:

```python
frame = hail_df.assert_ordered(dataset.read(ctx), dataset.ordering).select("contig", "position")
keys = [hail_df.SortColumn("contig", True, False), hail_df.SortColumn("position", True, False)]

backend.start(ctx, frame, hail_df.Collect(keys)).wait()
```

Here the input tables merge beneath the assertion, and the projection runs above the merge. For now,
an assertion always requires one partition in its order.

It returns the frame with a stock-shaped marker above it:
`Filter(assert_ordered(<key column>, '<direction>', …))`, each direction one of `ASC NULLS LAST`,
`ASC NULLS FIRST`, `DESC NULLS LAST` and `DESC NULLS FIRST`. The function is registered in
`hail_df.context()` and in the backend, and crosses plan bytes by name. The marker shows in an
explain's `frontend_plan` section as written; in the `logical_plan` and `physical_plan` sections it
has become an `OrderingAssertion` and an `OrderingAssertionExec`.

- An order the rows lack raises `OrderingAssertionFailed`, as does a key the plan computes, since
  nothing states the order of a computed column. It raises from `wait`, and no rows are read or
  written.
- A sort the frame wrote is an established order, so an assertion of that sort's order above it
  holds, whether or not the scans have that order.
- An ordering with no keys raises `ValueError`.
- A marker the backend can't read raises `InvalidAction` from `start`: one that is only part of a
  filter's predicate, or that sits anywhere but a filter, or with arguments other than pairs of a
  column and one of the directions above. Only a marker written by hand can be one; `assert_ordered`
  always writes a filter of its own.
- The wheel can't evaluate a marker, and neither can the backend: it marks a plan and computes
  nothing.

An assertion that contradicts the Action's ordering, such as one of `(position, contig, alleles)`
beneath a `Collect` by `(contig, position, alleles)`, fails before execution too. When the two orders
agree on a key at the same place, as these do on `alleles`, it fails with plain `BackendError` rather
than `OrderingAssertionFailed`, because of a DataFusion bug tracked in
[#325](https://github.com/hail-is/datafusion-sandbox/issues/325).

## Formulations

`hail_df.formulations` builds the reference combiner's formulations in the wheel's own `DataFrame`
API. Each returns an `OrderedFrame`: its `frame`, the `ordering` a run holds its rows in, the
`layout` a write gives them, an `OutputLayout`, and its `formulation`, a `FormulationRecord` of the
`name`, `groups` and `split_points` a run record holds.

- `formulations.union(ctx, dataset)` sorts the union of every input table's scan by the dataset's
  ordering, for one file (`OutputLayout.SINGLE_FILE`).
- `formulations.grouped_merge(ctx, dataset, groups)` splits the input tables, in name order, into
  `groups` contiguous, count-balanced input groups, larger groups first and clamped to one input
  table per group. It sorts each group's union and unions the groups with no sort above them, for
  one file (`OutputLayout.SINGLE_FILE`). The run's root assertion merges the groups, and each sort
  is an established order, so the plan is a merge per group beneath a final merge, with no
  `SortExec`. With one group, the plan is the union formulation's. Its formulation record holds the
  group count as given, above the input table count too. A count below 1 raises `ValueError`.

`backend.builtin_plan(dataset, formulation)` returns the plan bytes of the Rust plan builder of the
formulation a `FormulationRecord` names, over the dataset's input tables, with every scan encoded as
the scan description the dataset handed out. Run them through the same `start` as a Python plan, to
check that the frontend loses nothing:

```python
union = hail_df.formulations.union(ctx, dataset)

from_python = backend.start(ctx, union.frame, hail_df.Explain(union.ordering)).wait()
built_in = backend.start(
    ctx, backend.builtin_plan(dataset, union.formulation), hail_df.Explain(union.ordering)
).wait()
```

`FormulationRecord("union")` and `FormulationRecord("grouped-merge", groups=n)`, with a positive
`n`, have built-in plans. Any other record raises `InvalidAction`, including a grouped-merge with no
group count or with split points. The Rust plan builder discovers the dataset again, so a dataset that can no longer
be read with the input tables it was opened with raises `BackendError`.

## Running a plan

```python
union = hail_df.formulations.union(ctx, dataset)

run = backend.start(ctx, union.frame, hail_df.Collect(union.ordering))
run.run_id          # the one given, or a generated one
outcome = run.wait()
outcome.rows        # a pyarrow Table
```

`Backend.start(ctx, frame_or_plan_bytes, action, settings=None, run_id=None)` starts a run in the
background and returns a `Run` with its `run_id`. A frame is encoded unoptimized in `ctx` with
`frame.logical_plan().to_bytes(ctx)`, a session from `hail_df.context()`. Plan bytes built elsewhere
cross as they are, and give the same Outcome. `settings` is the settings map below.

`Run.wait()` blocks until the run ends and returns its Outcome. It releases the interpreter while it
waits, so other Python threads keep running. Waiting again hands back the same result. Dropping a
`Run` leaves the run going.

A run id names one going run of its `Backend`: `start` raises `InvalidAction` for an id whose run is
still going. Once that run has ended the id is free again, apart from a measured write's rule below.

### Cancelling a run

```python
run = backend.start(ctx, union.frame, hail_df.Collect(union.ordering), run_id="run-a")

backend.cancel("run-a")
try:
    outcome = run.wait()    # the Outcome, if the run had already completed
except hail_df.RunCancelled:
    ...                     # a cancelled run has no Outcome
```

`Backend.cancel(run_id)` cancels the going run with that id, whether or not a `Run` for it is left. A
run that already completed keeps its Outcome. Cancelling an id with no run, or one whose run has
ended, is not an error.

Ctrl-C during `Run.wait()` in the main thread cancels the run, waits for it to end, and raises
`KeyboardInterrupt`, so a measured write has recorded itself even if the interrupt ends the script.
The run's `wait` then raises `RunCancelled`. A second Ctrl-C raises `KeyboardInterrupt` at once and
leaves the cancelled run to end in the background. Signals reach only the main thread, so a `wait` in
another thread is not interrupted, and only `Backend.cancel` stops its run. `wait` checks for signals
about every 50 ms. An exception from any signal handler you installed cancels the run the same way; a
handler that doesn't raise leaves it going.

A cancelled run stops its work at once, but `wait` returns only when the run's threads have stopped,
and they stop where the plan yields. Scanning, merging and writing yield often. A large hash join's
fan-out beneath an aggregate doesn't, so such a run can take many seconds to end after its cancel,
though its run record is written at once. Treat a cancelled write's output path as unwritten: what
remains of an interrupted write is up to the store.

### Actions

An Action is a frozen dataclass. Each takes plain sort columns, such as a dataset's or an ordered
frame's `ordering`, and the run holds its rows in that order.

- `Collect(ordering)` collects the rows. Its Outcome is a `Rows`, whose `rows` is a `pyarrow.Table`
  holding the rows in `ordering`. The rows arrive once the run ends, through the Arrow C stream
  interface; nothing streams during a run.
- `Write(output_path, output_format, compression, ordering, layout, annotate_samples=False)` writes
  the rows, below. Its Outcome is a `RowsWritten`, whose `rows_written` counts them.
- `MeasuredWrite(write, metrics_directory, formulation)` performs a `Write` and records the run,
  below. Its Outcome is a `MeasuredWritten`, with `rows_written` and `unrecorded_metrics`.
- `Explain(ordering, write=None)` renders the plan of a run that drains the rows, without running
  it. Its Outcome is a `PlanText`, whose `text` is a table with a section each for the decoded
  `frontend_plan`, as the frontend wrote it, and the `logical_plan` and `physical_plan` the backend
  made of it.
- `ExplainAnalyze(ordering, write=None)` runs the plan, draining the rows, and renders the same
  sections and a `Plan with Metrics` section, the physical plan with its per-operator metrics.

Given a `write`, `Explain` renders the plan of that write and writes nothing, and `ExplainAnalyze`
performs it, as a `Write` would, sample annotation table included, and renders it. The write holds
the rows in its own `ordering`, so an explain's must be the same, or the run raises `InvalidAction`.

### Writing

```python
union = hail_df.formulations.union(ctx, dataset)

write = hail_df.Write(
    "out/combined.parquet", "parquet", "zstd(3)", union.ordering, union.layout, annotate_samples=True
)
backend.start(ctx, union.frame, write).wait().rows_written
```

- `output_path` is a local path or a `gs://` URL naming one file, with the format's extension. A
  path without it, with a trailing slash, or with a hidden file name or one named like a sample
  annotation table raises `InvalidAction`, since a later run could not read the file.
- `output_format` is `parquet` or `vortex`, whatever the dataset is stored in.
- `compression` is `None` for the format's default, or a mode the format takes: for Parquet a codec
  such as `snappy` or `zstd(3)`, for Vortex `standard` or `compact`.
- `ordering` is what the run holds the rows in and what the file's metadata records.
- `layout` is an `OutputLayout`, usually the ordered frame's. `OutputLayout.FILE_PER_PARTITION` raises
  `InvalidAction` until per-partition ordering assertions exist.

With `annotate_samples`, the [sample annotation table](adr/0018-declare-a-multi-sample-input-tables-samples-in-a-sample-annotation-table.md)
is written after the data, beside it, as `<stem>.samples.<ext>`. It covers the union of the sample
sets of the input tables the plan's scans read, whichever others the dataset has, so a plan over
some input tables gets exactly their samples, and the output opens as a dataset's input table. A
write that fails leaves no table. Every write removes the table an earlier write left at that path
first, so one without `annotate_samples` leaves none. A write annotating samples whose plan reads no
input tables raises `InvalidAction`.

### Measured writes

```python
grouped = hail_df.formulations.grouped_merge(ctx, dataset, 2)

run = backend.start(
    ctx,
    grouped.frame,
    hail_df.MeasuredWrite(write, "metrics", grouped.formulation),
)
outcome = run.wait()
outcome.rows_written
outcome.unrecorded_metrics  # what the run metrics table has no column for
```

A measured write records the run as `run.run_id` under the metrics directory, in the tables a CLI
measured write fills, described in [the combiner doc](combiner.md):

- `runs/<run_id>.parquet`, the run record. Its `formulation`, `groups` and `split_points` come from
  the Action's `formulation`. Its `dataset_path`, `input_format`, `row_ordering`, `input_tables` and
  `samples` come from the plan's scan descriptions, so they can't be misstated. `input_tables` counts
  the input tables the plan reads, and `samples` the union of their sample sets.
- `metrics/<run_id>.parquet`, the run metrics, for a completed run.

The plan must read exactly one dataset, one path, input format and row ordering, or the run raises
`InvalidAction` before it starts. A run id that already has a run record there fails the run and
replaces nothing, raising `BackendError` from `wait`.

The record's `status` is `completed`, `failed` or `cancelled`. A failed or cancelled run is recorded
before the exception is raised, and before its runtimes are torn down, however soon after `start` it
was cancelled: its record has the settings and `run_ns`, and `input_tables` and `samples` once its
scans have reopened, but no `rows_written` or `execute_ns`, and it has no run metrics. If that record
can't be written, the run's own failure is still what's raised.
A failure to record a run that did complete raises `BackendError`, and the output stays written.

`peak_rss_bytes` is the whole process's peak so far. A process running several runs reads no one
run's peak from it, unlike the CLI, which runs one per process.

### Settings

`settings` maps these keys to string values, and every one is optional:

| Key | What it sets | Default |
| --- | --- | --- |
| `hail_df.threads` | the threads of each of the run's IO and CPU runtimes | available parallelism |
| `hail_df.target_partitions` | the session's target partitions | the shared session's |
| `hail_df.repartition_file_min_size` | the least file size, in bytes, the optimizer splits a scan of across partitions | the shared session's |

Any other key, inside or outside the namespace, such as a `datafusion.*` key, and a value that isn't
a count, raises `InvalidAction`. The session otherwise keeps the shared defaults of
[ADR 0003](adr/0003-formulations-use-shared-session-defaults.md). A hostile
session, which tempts the optimizer to sort, is:

```python
settings = {"hail_df.target_partitions": "8", "hail_df.repartition_file_min_size": "0"}
```

### Errors

Every failure raises `BackendError`, or one of its subclasses: `InvalidAction` for a call the backend
doesn't take, `OrderingAssertionFailed` for an order the plan could hold only by a sort the backend
inserted, and `RunCancelled` for a cancelled run. `start` raises `InvalidAction` before the run
begins; a run that fails or is cancelled raises from `wait`:

```python
try:
    outcome = backend.start(ctx, frame, hail_df.Collect(dataset.ordering)).wait()
except hail_df.InvalidAction:
    ...  # versions, a setting, an empty ordering, a write it can't perform, a run id still going, a Python UDF in the plan, or a marker it can't read
except hail_df.OrderingAssertionFailed:
    ...  # an order the plan could hold only by an inserted sort, at the root or at an `assert_ordered`
except hail_df.RunCancelled:
    ...  # the run was cancelled
except hail_df.BackendError:
    ...  # anything else
```

The message of each starts with the error code in the table below. `OrderingAssertionFailed` is
raised before the plan executes, so no rows are read or written, and a measured write records it as
`failed`. A Python UDF inlined into a frame raises `InvalidAction`: Python UDFs aren't part of the MVP.

Passing something that is not an Action, or a setting whose name or value is not a string, raises
`TypeError` in Python, before the backend sees the call. A string the backend can't take, an unknown
setting, a value that doesn't parse, an unknown output format or a compression mode the format
doesn't take, raises `InvalidAction`.

## What a run does

This is what the backend does for a run, through its Rust interface in `src/backend.rs`:
`Backend::start(plan_bytes, action, settings, run_id, versions)` starts it, and `Run::wait` returns
its `Outcome`.

The backend decodes the plan with its own session, so functions resolve by name from its registry.
Each scan decodes to a placeholder holding its description, and the run reopens each as the sorted
scan the Rust formulations read, listing the input table's files then: a file added after the plan
was encoded is read. Before the logical optimizer, every `assert_ordered` marker becomes an ordering
assertion where the frontend wrote it, every sort the frontend wrote becomes an established order,
and the Action's ordering becomes an ordering assertion at the root, so the optimizer holds each
with a merge tree.

The backend's Actions:

- `Collect { ordering }` returns `Outcome::Rows`, the rows in `ordering` and their schema.
- `Explain { ordering, write }` returns `Outcome::Plan`, the plan of a run that drains the rows or
  performs the write: the decoded frontend plan as a `frontend_plan` section beside the logical and
  physical plans.
- `ExplainAnalyze { ordering, write }` runs that plan and returns `Outcome::Plan`: those sections and
  a `Plan with Metrics` section.
- `Write(write)` returns `Outcome::RowsWritten`. `Write::new` makes the checks a write can make
  alone, and holds the rows as an explain's write does: the sink writes the ordering as file metadata
  and keeps no requirement of its own, since the root assertion holds it.
- `MeasuredWrite { write, metrics_directory, formulation }` returns `Outcome::Measured`, the rows
  written and the names of the metrics the run metrics table has no column for. It refuses a recorded
  run id first, then writes, then records, all before the pipeline runner tears down the run's
  runtimes.

The run asks the object-store resolver for the paths its plan reads and for those its Action names:
the output path and the metrics directory.

`Backend::builtin_plan(dataset, formulation, versions)` builds a Rust formulation's frame with its
plan builder, swaps each scan of an input table for the scan description the opened dataset handed
out, and encodes the plan in the frame the wheel writes, so that `start` reads it as it reads a
frontend's.

Every failure is a `BackendError` with a stable `ErrorCode`:

| Code | Name | Raised for |
| --- | --- | --- |
| `InvalidAction` | `invalid_action` | versions other than the backend's, an unknown or malformed setting, an empty ordering, an explain whose write holds another ordering, an unknown output format, a compression mode the format doesn't take, an output path that names no readable file, a file-per-partition write, a write annotating samples over no input tables, a measured write not over exactly one dataset, a run id whose run is still going, a formulation with no built-in plan, a payload outside the `DFPYCHN` frame or framed at another version, an inlined Python UDF (`DFPYUD*`), an `assert_ordered` marker that isn't a filter's whole predicate or whose arguments aren't pairs of a key column and a direction |
| `OrderingAssertionFailed` | `ordering_assertion_failed` | a plan an ordering assertion, at the root or at a marker, could hold only by a sort the backend inserted or through a node that keeps no input's order |
| `RunCancelled` | `run_cancelled` | a cancelled run |
| `Backend` | `backend_error` | anything else |

Invalid actions fail `start` before the run begins; an ordering assertion fails `wait`, before the
plan executes. A repeated run id in a measured write has the `backend_error` code and fails `wait`.

## Inside the package

The package's own code imports DataFusion only through `hail_df.datafusion`, so that moving to our
own build of datafusion-python stays a change to one module. Its tests check this.

The codec is installed under the id `hail_df`, its `__datafusion_codec_id__`. It writes each
scan's description as the provider's payload and nothing for functions, which cross by name, as
`assert_ordered` does: the wheel holds it as an FFI function from the extension. The
wheel frames each payload under that id; the backend reads payloads back only from that frame, at
its version 1. The backend refuses a function the wheel inlined as a pickled Python payload:
Python UDFs aren't part of the MVP.
