# `hail_df`

`hail_df` is the Python frontend of Hail-on-DataFusion
([ADR 0022](adr/0022-build-hail-plans-with-datafusion-python-and-run-them-in-our-backend.md)). A
frontend builds plans with datafusion-python's own `DataFrame` API, on the stock
`datafusion==55.0.0` wheel, against datasets an in-process Rust backend opens. The backend is
`src/backend.rs` in the root crate; `hail_df/` holds the bindings crate over it, the Python package
and its tests.

So far a frontend can open a dataset and plan against it. The backend runs plan bytes through its
Rust interface, but Python can't start a run yet: the scans in a plan are scan descriptions, which
plan like tables but refuse to scan in Python.

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
`WIRE_VERSION`, and the backend refuses a call whose versions aren't its own with `BackendError`. A
`datafusion` wheel of another major than the backend's fails every call rather than misdecode.

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
installed. Plans over a dataset's scans encode only in such a session. Keep the context alive while
its frames are in use: the wheel's frames reach their session through it.

`Dataset.read(ctx, names=None)` returns a `DataFrame` over the named input tables: the union of
their scans, or the single scan of one. With no names it reads every input table, in name order. An
empty list, a repeated name, or a name the dataset doesn't hold raises `ValueError`. Executing the
frame in the wheel, such as with `frame.collect()`, fails: only the backend runs a plan over scan
descriptions.

`backend.scan_descriptions(plan_bytes)` reads the plan bytes back and returns the scan
descriptions the plan reads, each once, in the order the plan first reads them. Bytes that are not
such a plan raise `BackendError`.

## Running plan bytes in the backend

The Python binding follows; this is the backend's Rust interface in `src/backend.rs`.
`Backend::start(plan_bytes, action, settings, run_id, versions)` starts a run in the background and
returns a `Run` with its `run_id`, the one given or a generated one; `Run::wait` returns its
`Outcome`. Dropping a `Run` leaves the run going.

The backend decodes the plan with its own session, so functions resolve by name from its registry.
Each scan decodes to a placeholder holding its description, and the run reopens each as the sorted
scan the Rust formulations read, listing the input table's files then: a file added after the plan
was encoded is read. Before the logical optimizer, every sort the frontend wrote becomes an
established order, and the Action's ordering becomes an ordering assertion at the root, so the
optimizer holds it with a merge tree.

The Actions so far:

- `Collect { ordering }` returns `Outcome::Rows`, the rows in `ordering` and their schema.
- `Explain { ordering }` returns `Outcome::Plan`, the plan of a run that drains the rows: the
  decoded frontend plan as a `frontend_plan` section beside the logical and physical plans.

The settings map takes exactly `hail_df.threads` (default: available parallelism),
`hail_df.target_partitions` and `hail_df.repartition_file_min_size` (default: the shared
session's).

Every failure is a `BackendError` with a stable `ErrorCode`:

| Code | Name | Raised for |
| --- | --- | --- |
| `InvalidAction` | `invalid_action` | versions other than the backend's, an unknown or malformed setting, an empty ordering, a payload outside the `DFPYCHN` frame or framed at another version, an inlined Python UDF (`DFPYUD*`) |
| `OrderingAssertionFailed` | `ordering_assertion_failed` | a plan the root assertion could hold only by a sort the backend inserted |
| `RunCancelled` | `run_cancelled` | a cancelled run |
| `Backend` | `backend_error` | anything else |

Invalid actions fail `start` before the run begins; an ordering assertion fails `wait`, before the
plan executes.

## Inside the package

The package's own code imports DataFusion only through `hail_df.datafusion`, so that moving to our
own build of datafusion-python stays a change to one module. Its tests check this.

The codec is installed under the id `hail_df`, its `__datafusion_codec_id__`. It writes each
scan's description as the provider's payload and nothing for functions, which cross by name. The
wheel frames each payload under that id; the backend reads payloads back only from that frame, at
its version 1. The backend refuses a function the wheel inlined as a pickled Python payload:
Python UDFs aren't part of the MVP.
