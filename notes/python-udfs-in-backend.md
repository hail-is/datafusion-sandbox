# Running datafusion-python's pickled Python UDFs in our backend

Research for [#319](https://github.com/hail-is/datafusion-sandbox/issues/319), on the map
[#311](https://github.com/hail-is/datafusion-sandbox/issues/311). Python UDFs are not in the MVP,
but no backend design may rule them out
([#316](https://github.com/hail-is/datafusion-sandbox/issues/316)). These notes check what keeping
that option open takes. They build on
[#317](https://github.com/hail-is/datafusion-sandbox/issues/317), which established the
`DFPYUDF` framing and the `with_python_udf_inlining(enabled=False)` switch.

Every claim carries a tag:

- **read** (or `[read]`): from source, docs, or a first-party issue or PR.
- **ran**: from an experiment run in the sandbox VM on 2026-10-26. The VM is 8 vCPU arm64 Linux,
  native rather than Rosetta, and shared with other sessions.
- **inferred**: my reasoning, not verified.

Unless stated otherwise, `codec.rs`, `udf.rs`, `udaf.rs`, `udwf.rs` and `array.rs` are under
`crates/core/src/` in datafusion-python 55.0.0, and `user_defined.py` is
`python/datafusion/user_defined.py` in the same release. Paths under `src/` are this repository at
`a749b4de`.

The experiments are in [`python-udfs-in-backend-experiment/`](python-udfs-in-backend-experiment/):

- the GIL-scaling benchmark: `src/main.rs`, `run.sh`
- the payload probes: `payload/`
- a Rust binary that reuses datafusion-python's codec: `codec-reuse/`

## Summary

1. **Payload.** Each kind of UDF is a 10-byte header, `DFPYUD{F,A,W} | wire version 1 | Python
   major | minor`, followed by a cloudpickled tuple:
   - scalar: the user's own function, called as `f(*pa.Array) -> pa.Array`
   - aggregate: an `Accumulator` factory
   - window: a `WindowEvaluator` factory

   The tuple also holds the name, volatility, and the signature as arrow-rs IPC schemas. To get a
   callable, a decoder outside datafusion-python needs:
   - an embedded CPython of the same major.minor;
   - `cloudpickle` and `pyarrow`;
   - `datafusion` itself, or a stub of `datafusion.user_defined`, for aggregates and windows,
     because their base classes are pickled by reference;
   - every module the UDF references.

   We can reuse the `datafusion-python` crate from crates.io. Its `PythonLogicalCodec` decoded and
   ran all three kinds in a plain Rust binary (ran), but it is heavy. Our own decoder would be
   small.
2. **In-process.** Our CPU workers would run UDFs one batch at a time under a single GIL, as
   datafusion-python, Polars and DuckDB all do.
   - On a GIL build, Python-bound UDFs do not scale at all: 1.0× at 8 workers (ran).
   - UDFs that spend their time in numpy or Arrow kernels scale nearly linearly at 1M-row batches
     (ran).
   - Free-threaded 3.14t also scales Python-bound UDFs, to 3.5 to 3.7× at 4 workers (ran). It is
     officially supported in 3.14 (PEP 779).
   - datafusion 55.0.0, pyarrow, numpy and cloudpickle all ship working cp314t wheels. There are
     no 3.13t wheels for datafusion. pyo3 defaults modules to `gil_used = false` from 0.28.
3. **Remote.** Two patterns:
   - Separate Python worker processes fed Arrow over local IPC: Spark, Ray, and Daft's process
     mode. This buys isolation, cancellation and per-job environments.
   - An interpreter embedded in the engine process: Sail, Ballista's open PR #2619 (which reuses
     datafusion-python's codecs), Daft's default mode, and Spark's new opt-in `inprocess_udf`.
     This buys zero copy.

   Environments come from images, unpacked archives, or Ray's per-node cached installs. Every
   system enforces a Python-version match.
4. **Consequences for the backend now.** Nothing decided so far rules Python UDFs out. Four cheap
   rules keep it that way:
   - The pyo3 entry point must release the GIL around `pipeline::run`; otherwise the first UDF
     deadlocks.
   - Our cdylib must stay free-threading-safe (`gil_used = false`), or importing it turns the GIL
     back on for the whole process.
   - The backend's codec should refuse a `DFPYUD*` payload by name.
   - The remote backend's worker must be allowed to link libpython or own Python processes.

   When UDFs are built, they want their own thread pool next to the CPU and IO runtimes.

## 1. Payload

**Framing (all three kinds)**
- The layout is `magic(7) | wire_version u8 | py_major u8 | py_minor u8 | cloudpickle.dumps(tuple)`
  (`codec.rs:129-175`). The magics are `DFPYUDF` for scalar, `DFPYUDA` for aggregate and
  `DFPYUDW` for window (`codec.rs:146,152,157`). **read**
- `WIRE_VERSION_CURRENT = 1` and `WIRE_VERSION_MIN_SUPPORTED = 1` (`codec.rs:160,164`). Decode
  rejects any version outside `MIN..=CURRENT`, and any `(major, minor)` that differs from the
  running interpreter's `sys.version_info`, with an actionable error (`codec.rs:190-234`,
  `1362-1367`). The check does not cover the micro version or GIL vs free-threaded. **read**
- From the stock 55.0.0 wheel, on 3.14 and 3.14t alike, I got `wire_version=1`, `py=3.14`,
  pickle protocol 5. The blobs were about 1.1 KB (scalar), 2.8 KB (aggregate) and 1.4 KB (window)
  for trivial UDFs defined in `__main__`. They include each code object's `co_filename`, so the
  size varies with the source path (`notes/python-udfs-in-backend-experiment/payload/probe_payload.py`).
  The 3.14 header and the 3.14t header are identical. **ran**

**The cloudpickled tuples**
- Scalar: `(name: str, func, input_schema_ipc: bytes, return_schema_ipc: bytes, volatility: str)`
  (`codec.rs:1193-1249`).
  - `input_schema_ipc` is an Arrow IPC *stream* holding only a schema message, with one field
    `arg_i` per `Signature::exact` type. Decode keeps only the data types (`codec.rs:1251-1322`).
  - `return_schema_ipc` is a one-field schema; its nullability and metadata are kept.
  - `volatility` is one of `"immutable" | "stable" | "volatile"` (`codec.rs:1347-1353`).
  - **read**
- Aggregate: `(name, accumulator_factory, input_schema_ipc, return_schema_ipc /* field "result" */,
  state_schema_ipc /* all state fields, metadata kept */, volatility)` (`codec.rs:1507-1602`).
  **read**
- Window: `(name, evaluator_factory, input_schema_ipc, return_schema_ipc /* field "result" */,
  volatility)` (`codec.rs:1420-1505`). **read**
- The schemas are Arrow IPC written by arrow-rs; pyarrow is not involved (`codec.rs:1196-1199`).
  So a Rust decoder can read every field except the callable without Python, once
  `cloudpickle.loads` has run. **read**

**What the callable is**
- Scalar: the user's own callable, unwrapped. `ScalarUDF.udf(func, ...)` hands `func` straight to
  `df_internal.ScalarUDF(name, func, ...)`. The decorator form wraps only the *Expr-building*
  function, not the stored callable (`user_defined.py:383-421`, `241-259` in `udf.rs`). It is
  called as `func(*pyarrow_arrays) -> pyarrow.Array`, one `pa.Array` per argument
  (`udf.rs:180-211`). **read**
- Aggregate: the factory is usually the user's `Accumulator` subclass, or a lambda returning an
  instance. `udaf()` insists that `accum()` is an `isinstance` of
  `datafusion.user_defined.Accumulator` (`user_defined.py:729-753`). One instance is called per
  accumulator (`udaf.rs:150-157`), with `update(*arrays)`, `merge(list_of_state_arrays)`,
  `state() -> list[scalar-like]`, `evaluate() -> scalar-like`, and optionally `retract_batch` /
  `supports_retract_batch` (`udaf.rs:52-148`). **read**
- Window: the factory is usually a `WindowEvaluator` subclass, checked with `isinstance`
  (`user_defined.py:1170-1191`). The Rust side calls `memoize`, `get_range`, `is_causal`,
  `evaluate_all`, `evaluate`, `evaluate_all_with_rank`, `supports_bounded_execution`,
  `uses_window_frame` and `include_rank` (`udwf.rs:55-199`). Several of these have their only
  default implementation on the Python base class (`user_defined.py:831-985`). **read**
- What unpickling imports (a recording `pickle.Unpickler.find_class`, run on real plans):
  - scalar: `cloudpickle.cloudpickle._make_function/_function_setstate/_builtin_type/subimport`
    and `pyarrow.lib.Array` (from the function's annotations)
  - aggregate: the same plus `datafusion.user_defined.Accumulator` and `abc.ABCMeta`
  - window: the same plus `datafusion.user_defined.WindowEvaluator`
  - The user classes are defined in `__main__`, so they are pickled by value, but their
    datafusion base classes go *by reference*. **ran**
- What that means for the backend's interpreter (with
  `payload/load_payload.py` in the experiment dir):
  - Without `cloudpickle`, all three kinds fail (`No module named 'cloudpickle'`). Loading needs
    `cloudpickle`, not only `pickle`. **ran**
  - With `cloudpickle` and `pyarrow` but no `datafusion`, the scalar loads and runs, while
    aggregate and window fail with `No module named 'datafusion'`. **ran**
  - Putting a stub `datafusion.user_defined` with empty `Accumulator` and `WindowEvaluator`
    classes into `sys.modules` makes all three load and run. A real stub would also need the
    `WindowEvaluator` defaults (`memoize` no-op, `get_range -> (idx, idx+1)`, the `is_causal`,
    `supports_bounded_execution`, `uses_window_frame` and `include_rank` defaults of `False`). The
    alternative is to install the `datafusion` wheel itself, which imports `datafusion._internal`,
    a second complete DataFusion build, into the process (`user_defined.py:28-32`). **ran** for
    the stub; the rest is **inferred**
- Pickles made on 3.14 loaded and ran on 3.14t, and the reverse also worked, for all three kinds
  (cloudpickle 3.1.2). **ran**

**Version and portability constraints**
- cloudpickle's README: "Cloudpickle can only be used to send objects between the **exact same
  version of Python**", it is not for long-term storage, and only trusted data may be loaded
  (cloudpickle 3.1.2 `README.md:17-25`). datafusion-python checks only `(major, minor)`. Polars
  checks `(minor, micro)` for its cloudpickled UDFs
  (`polars-utils/src/python_function.rs:229-241`, `pl_serialize.rs:262-272`, polars py-2.1.0).
  **read**
- Functions and classes from importable modules are pickled *by reference*. Only `__main__`, and
  modules registered with `cloudpickle.register_pickle_by_value`, are pickled by value
  (`README.md:70-104`; `_lookup_module_and_qualname`, `cloudpickle/cloudpickle.py:273-309`). A UDF defined
  in the user's package, `mylib.f`, therefore needs `mylib` installed in the backend. The same
  goes for modules the function imports (datafusion-python docs,
  `docs/source/user-guide/distributing-work/expressions.md:110-129`). Those docs leave out the
  `datafusion` base-class dependency of UDAFs and UDWFs shown above. **read**, **ran**
- The datafusion-python 55.0.0 wheel requires `cloudpickle>=2.0` and `pyarrow>=16` (`>=22` on
  3.14) (PyPI JSON `requires_dist`). **read**

**How datafusion-python runs them**
- Scalar, `invoke_with_args` (`udf.rs:180-211`): a single `Python::attach` covers the whole
  batch.
  - Each argument goes `ColumnarValue::to_array(num_rows)` → `PyArrowArrayExportable` →
    `py.import("pyarrow").array(exportable)`. pyarrow pulls the data through
    `__arrow_c_array__`, which returns PyCapsules over `FFI_ArrowSchema` and `FFI_ArrowArray`
    (`array.rs:40-82`).
  - Then `func.call(args)`, and the result is imported with
    `ArrayData::from_pyarrow_bound` (`udf.rs:207`). The data is zero-copy, but every batch costs
    pyarrow wrapper objects.
  - **read**
- Aggregate (`udaf.rs:52-148`): every method runs its own `Python::attach`. `update_batch` and
  `merge_batch` export arrays with arrow-rs `ToPyArrow`. `state()` and `evaluate()` go back
  through `PyScalarValue`, which turns a pyarrow scalar into a one-element `pa.array` and imports
  it (`pyarrow_util.rs:66-150`). **read**
- No Python UDAF implements `GroupsAccumulator`, so DataFusion wraps it in
  `GroupsAccumulatorAdapter`, which "creates a new Accumulator for each group"
  (`datafusion-functions-aggregate-common-55.1.0/src/aggregate/groups_accumulator.rs:39-50`,
  `datafusion-physical-plan-55.1.0/src/aggregates/grouped_hash_stream.rs:612-630`). A grouped
  Python UDAF therefore makes one Python object per group and one `update` call per group per
  batch. `DISTINCT` is refused (`udaf.rs:305-317`). **read**; the per-group call count is
  **inferred** from the adapter.
- Window (`udwf.rs:55-199`): one `Python::attach` per method call, including the boolean probes.
  `evaluate` (bounded or framed execution) runs once per row with a fresh pyarrow export of the
  whole partition each time. **read**; the per-row cost is **inferred**
- Nothing in `udf.rs`, `udaf.rs`, `udwf.rs` or `array.rs` releases the GIL. The only `detach` in
  the crates is `wait_for_future`, which releases the GIL while the query runs on its
  `Runtime::new()` multi-thread Tokio runtime (`crates/util/src/lib.rs:47-55, 79-114`).
  datafusion-python is thus the same shape as our backend: N Tokio workers, each calling
  `Python::attach` once per batch. Its docs admit the cost: there is "no cost in using UDFs ...
  apart from having to lock the GIL" (`docs/source/index.md:37`). **read**

**What a Rust backend needs in order to decode and run the payloads**
- The `datafusion-python` crate is on crates.io: version 55.0.0, published 2026-10-12
  (`https://crates.io/api/v1/crates/datafusion-python`), with `crate-type = ["cdylib", "rlib"]`.
  - `pub mod codec` exports `PythonLogicalCodec`, which implements `Default` and
    `LogicalExtensionCodec`, plus `PythonPhysicalCodec` (`codec.rs:588-683, 922`).
  - The modules `udf`, `udaf` and `udwf` are private (`lib.rs:53-59`).
    `PythonFunctionScalarUDF` and `PythonFunctionAggregateUDF` are `pub(crate)`. Only
    `to_rust_accumulator`, `PythonFunctionWindowUDF` and `to_rust_partition_evaluator` are
    re-exported (`lib.rs:60-64`). A backend can still get all three kinds as `Arc<ScalarUDF>`,
    `Arc<AggregateUDF>` or `Arc<WindowUDF>` by decoding the plan with
    `PythonLogicalCodec::default()`.
  - **read**
- Costs of depending on that crate:
  - It forces `pyo3/extension-module`. That feature is deprecated but, per the pyo3 guide, it
    disables linking to libpython, and an embedding binary needs that link (`crates/core/Cargo.toml`
    pyo3 features; pyo3 0.29.3 `guide/src/features.md:317-321`,
    `building-and-distribution.md:225-238`).
  - Its default features install a mimalloc `#[global_allocator]` (`lib.rs:66-67`; use
    `default-features = false`).
  - It drags in datafusion-ffi, datafusion-spark, object_store with aws, gcp and azure, avro and
    pyo3-async-runtimes.
  - It requires `datafusion = "55.1.0"`, which semver-unifies with our 55.1.
  - **read**
- Reuse works, with one workaround. The test is `notes/python-udfs-in-backend-experiment/codec-reuse/`.
  - It is a Rust binary that depends on `datafusion-python = "=55.0.0"` with
    `default-features = false`, plus pyo3 0.29 with `auto-initialize`. It decodes
    `df.logical_plan().to_bytes(ctx)` from the stock wheel using
    `LogicalPlanNode::try_into_logical_plan(&ctx.task_ctx(), &PythonLogicalCodec::default())`,
    then runs the plan on a 4-worker Tokio runtime.
  - The first link failed with 96 undefined `Py*` symbols, because `extension-module` suppresses
    the libpython link. A three-line `build.rs` that emits `rustc-link-lib=python3.14` fixed it.
  - All three plans (scalar, grouped UDAF, UDWF) then produced correct results.
  - With only `cloudpickle` and `pyarrow` on `PYTHONPATH`, the scalar plan ran, and decoding the
    UDAF plan failed with `ModuleNotFoundError: No module named 'datafusion'`.
  - The lockfile resolved to datafusion 55.2.0, arrow 59.3.2 and pyo3 0.29.3. The release binary
    is 155 MB.
  - **ran**
- Reimplementing instead is small (**inferred** from the code sizes above):
  - Strip the 10-byte header and check `(major, minor)`.
  - `cloudpickle.loads`.
  - Read the IPC schemas with arrow-rs `StreamReader`.
  - Wrap the callable in our own `ScalarUDFImpl`, `AggregateUDFImpl` and `WindowUDFImpl`,
    following `udf.rs:180-211`, `udaf.rs:52-157` and `udwf.rs:55-208`. The conversion is arrow's
    own `pyarrow` feature (`ToPyArrow` / `FromPyArrow`), which the experiment below uses.
  - Register them through our own `LogicalExtensionCodec::try_decode_udf/udaf/udwf`.
  - The private wire format becomes our compatibility surface. It is versioned (byte 7), and
    #317 covers how it changes.
- Python packages the backend's interpreter needs:
  - `cloudpickle`
  - `pyarrow`: imported by the conversion and by the payload's annotations
  - `datafusion` (or a stub of `datafusion.user_defined`) for UDAFs and UDWFs
  - whatever the UDF imports or references by module
  - The interpreter's `(major, minor)` must match the client's; the safe choice is to match the
    exact version, as cloudpickle advises.
  - **ran** / **read** as cited above

## 2. In-process: calling Python from the CPU runtime's workers

**The GIL build (CPython 3.12, 3.13, 3.14 default)**
- One thread at a time runs Python bytecode or touches objects. A UDF whose time is spent in
  Python (per-row code, `to_pylist`, building lists) is fully serialized however many Tokio
  workers call it.
  - Experiment, on both 3.13.15 and 3.14.9: `pure_loop` and `rowwise` ran at 0.96 to 1.06× of
    1-worker throughput at 2, 4 and 8 workers. **ran**
- Work inside C kernels that release the GIL (numpy ufunc loops, Arrow C++ compute) runs in
  parallel. Only the conversion and wrapper code around each call holds the GIL, so the speedup
  follows Amdahl's law in the GIL-held share of a call.
  - Experiment: about 12 ms per call over 1M f64, mostly kernel time. On 3.14 at 2, 4 and 8
    workers, `numpy_kernel` reached 2.01×, 3.86× and 6.38×, and `pyarrow_kernel` reached 1.97×,
    3.90× and 6.57×. 3.13 gave 7.45× and 6.97× at 8.
  - A first 3.14 run plateaued at 2.0 to 2.8× with very wide spreads while another session was
    loading the VM. Two reruns did not reproduce it, so I discarded it.
  - **ran**
  - Smaller batches, or more Python glue per batch, raise the GIL-held share and lower the
    ceiling. **inferred**
- A worker blocked waiting for the GIL blocks its Tokio worker thread. The other partitions'
  non-Python operators on that thread stall with it, so a UDF-heavy plan can starve the CPU
  runtime. **inferred**

**Free-threaded CPython (PEP 703 build, "3.13t" / "3.14t")**
- PEP 779 is Final, accepted 2025-06-16. It sets the "phase II" criteria (the free-threaded build
  supported, but optional), with targets of at most 15% single-thread slowdown and at most 20%
  memory overhead (https://peps.python.org/pep-0779/). **read**
- What's New in 3.14: "Free-threaded Python is officially supported". The single-thread penalty
  "is now roughly 5-10%", and the specializing interpreter is enabled in free-threaded mode
  (https://docs.python.org/3/whatsnew/3.14.html). The free-threading HOWTO gives an average
  overhead "from about 1% on macOS aarch64 to 8% on x86-64 Linux" on pyperformance
  (https://docs.python.org/3.14/howto/free-threading-python.html). **read**
- An embedder must link `libpython3.14t` and build pyo3 against it, because 3.14t has no stable
  ABI. pyo3 0.29 adds PEP 803 `abi3t` only for 3.15+ (pyo3 `CHANGELOG.md:103`). **read**
- If any imported extension module does not declare free-threading support, CPython re-enables
  the GIL at import and prints a `RuntimeWarning` (pyo3 guide `free-threading.md:29-32`). The
  backend must check `sys._is_gil_enabled()` after loading the UDFs' imports. **read**
- Experiment, 3.14.9t:
  - Scaling at 2, 4 and 8 workers:
    - `pure_loop`: 1.92×, 3.53×, 4.66×
    - `rowwise`: 1.82×, 3.65×, 6.59×
    - `numpy_kernel`: 1.97×, 3.95×, 7.43×
    - `pyarrow_kernel`: 1.98×, 3.85×, 7.29×
  - At 1 worker, against 3.14 GIL: parity on `pure_loop` and the kernels, and about 15 to 22%
    slower on `rowwise`.
  - **ran**
  - The real gain over the GIL build is for UDFs whose time is spent in Python. For kernel-bound
    UDFs both builds scale similarly at this batch size. **ran** / **inferred**
- Caveats: the 8 vCPUs are shared with other sessions, which caps the 8-worker numbers. Also,
  from pyo3 `free-threading.md:93-108`:
  - GC and other stop-the-world events need every attached thread to reach a safe point.
  - A worker that stays attached while doing long Rust work, or blocks while attached, can hang
    the others.
  - Attach only around the Python call, and `Python::detach` before blocking.
  - **read**
- UDF code that mutates shared state (globals, closures, one accumulator object used from two
  threads) can now race. datafusion-python creates one accumulator or evaluator instance per
  DataFusion accumulator or partition, which limits this to user globals. **inferred**

**pyo3**
- `Python::with_gil` became `Python::attach`, `allow_threads` became `detach`, `GILOnceCell` is
  deprecated for `PyOnceLock`, and `prepare_freethreaded_python` became `Python::initialize`, all
  in 0.26.0 (`CHANGELOG.md:480-512`). **read**
- Free-threaded 3.13t has been supported since 0.23.0, as has `#[pymodule(gil_used = false)]`
  (`CHANGELOG.md:768,787`). In 0.28.0 free-threading became opt-out, so modules default to
  `gil_used = false` (`CHANGELOG.md:273`; guide `free-threading.md:22-39`). 0.29.0 dropped 3.13t
  and keeps 3.14t+ (`CHANGELOG.md:105`). The current release is 0.29.3. **read**
- `PyOnceLock` detaches while it waits on another thread's initialization, which avoids deadlocks
  (`free-threading.md:178-200`). Critical-section wrappers live in `pyo3::sync::critical_section`
  (`CHANGELOG.md:319`). Mutable `#[pyclass]` borrows can raise or panic under concurrent access
  (`free-threading.md:110-167`). **read**

**Free-threaded status of the packages we would load**
- datafusion 55.0.0 on PyPI ships `cp310-abi3` wheels (GIL builds 3.10 to 3.14) and
  `cp314-cp314t` wheels for manylinux aarch64 and x86_64, macOS and Windows. There are no 3.13t
  wheels (https://pypi.org/pypi/datafusion/55.0.0/json). Free-threaded wheels arrived in 54.0.0
  (PR #1553), and 3.13t builds were dropped in #1609 (`dev/changelog/54.0.0.md:37,88`). **read**
- datafusion-python's `#[pymodule]` declares no `gil_used` (`lib.rs:74`), so under pyo3 0.29 it
  defaults to `gil_used = false`. With the cp314t wheel, `import datafusion` left
  `sys._is_gil_enabled()` `False`. CI tests 3.14t (`.github/workflows/test.yml:47`). I found no
  beta or experimental warning in its docs. **read**, **ran**
- pyarrow 26.0.0 ships `cp314t` and `cp315t` wheels, and numpy 2.5.4 ships `cp314t`; neither
  re-enabled the GIL on import (PyPI JSON; ran). cloudpickle 3.1.2 is pure Python
  (`py3-none-any`) and worked on 3.14t (ran). **read**, **ran**

**How other engines handle it**
- datafusion-python attaches per batch on its Tokio workers and holds the GIL for conversion plus
  call, as above. **read**
- Polars py-2.1.0, `map_batches` / `map_elements`: the column UDF hook is
  `Python::attach(|py| call_lambda_with_series(...))`, covering both conversion and call
  (`crates/polars-python/src/on_startup.rs:34-40`). It is called from whatever engine thread runs
  the expression (`crates/polars-plan/src/dsl/python_dsl/python_udf.rs:106-131`).
  - The `map_elements` docs: "strongly discouraged ... effectively running python 'for' loops".
    An unstable `strategy='threading'` "might only speed up your code if ... the python function
    releases the GIL" (`py-polars/src/polars/expr/expr.py:5683-5745`).
  - The user guide recommends ufuncs, numba or plugins over Python functions
    (https://docs.pola.rs/user-guide/expressions/user-defined-python-functions/).
  - **read**
- DuckDB Python (duckdb-python `main` at `4154efc`, `src/python_udf.cpp:237-243, 380-384`):
  `nb::gil_scoped_acquire` for every DataChunk, held across the Arrow conversion and the call, in
  both `native` mode (one Python call per row) and `arrow` mode. Arrow UDFs receive
  `pyarrow.ChunkedArray`s "of at most 2048 rows", and the docs say nothing about threads or the
  GIL (https://duckdb.org/docs/2.0/clients/python/function.html). **read**
- So all three engines run Python UDFs on their native worker pools and take the GIL per batch or
  chunk. None of them uses a separate Python thread or process. Their parallelism comes only from
  GIL-releasing kernels inside the UDF, or from a free-threaded interpreter. **read** /
  **inferred**

### The GIL experiment

The code is in `notes/python-udfs-in-backend-experiment/`; its README has the method, how to run
it, and the full tables. In short: pyo3 0.29 embedded in a Rust binary, with arrow 59
`ToPyArrow` / `FromPyArrow` per call, the same as datafusion-python. N tasks run on an N-worker
Tokio runtime and yield between calls. Each configuration ran 5 × 2 s and the medians are
reported. The VM is 8 vCPU arm64, native rather than Rosetta, but shared and noisy.

| calls/s @8 workers (speedup vs 1) | 3.13 GIL | 3.14 GIL | 3.14t |
|---|---|---|---|
| pure_loop (Python bytecode) | 96.0 (1.06×) | 114.5 (1.02×) | 542.5 (4.66×); rerun 5.10× |
| rowwise (`to_pylist` + listcomp, 100k rows) | 137.5 (0.97×) | 133.0 (0.98×) | 695.0 (6.59×) |
| numpy_kernel (sin·cos, 1M f64) | 652.0 (7.45×) | 532.5 (6.38×) | 620.0 (7.43×) |
| pyarrow_kernel (pc.sin·pc.cos, 1M f64) | 606.5 (6.97×) | 568.5 (6.57×) | 612.5 (7.29×) |

The 4-worker rows are cleaner: pure_loop and rowwise run at 0.97 to 1.03× on the GIL builds and
3.5 to 3.65× on 3.14t, and the kernels run at 3.8 to 3.95× on all three. The VM had a background
load of about 3 to 4 runnable threads, which caps the 8-worker numbers. At 1 worker, 3.14t
matched 3.14 on pure_loop and the kernels, and was 15 to 22% slower on rowwise. **ran**

## 3. Remote: how other systems run user Python on workers

Sources are tarballs of each repo's default branch, fetched 2026-10-26. Citations are `repo@sha path:line`.

| Short name | Repo @ commit | Version at that commit |
|---|---|---|
| spark | apache/spark @ `6c9dcb9b354d378e39b6321af58d64e86f26441e` | 5.0.0-SNAPSHOT (master) |
| ballista | apache/datafusion-ballista @ `98499ebf21192067660ebf0089960a177205f90f` | 55.0.0 |
| ray | ray-project/ray @ `88f1ffefc6b5a88507fbfdb448edecc44664cefa` | 3.0.0.dev0 (master) |
| daft | Eventual-Inc/Daft @ `06c1fa2c7091d8449be8765e84817d45a63f196d` | main |
| sail | lakehq/sail @ `86dea385943c785b38d85479755589d193f989d3` | 0.7.2 |
| comet | apache/datafusion-comet @ `1dec4c058f3f887f6df0bf21181c6450ba6baf05` | main |
| dfpy | datafusion-python 55.0.0 crate source (`~/.cargo/registry`) | 55.0.0 |

### PySpark (classic): a separate Python worker process for each task, forked from a per-executor daemon

- **Process model [read].** For each `pythonExec`, the executor JVM starts one `python -m pyspark.daemon`. Forking from Java is expensive, so the daemon forks the workers (`spark core/.../api/python/PythonWorkerFactory.scala:106-112, 346-347`). For each worker request the JVM connects to the daemon, and the daemon calls `os.fork()` once per connection (`spark python/pyspark/daemon.py:216-222`). The JVM and the worker talk over a local socket, TCP loopback or a Unix domain socket when `spark.python.unix.domain.socket.enabled` is set (`PythonWorkerFactory.scala:188-223`; `Python.scala:97`). There is a no-daemon fallback that runs `ProcessBuilder(pythonExec, "-m", workerModule)` for each worker. It is used on Windows and when `spark.python.use.daemon=false` (`PythonWorkerFactory.scala:110-112, 254-271`; `Python.scala:35-38`).
- **Worker reuse [read].** `spark.python.worker.reuse` is on by default (`Python.scala:25-28`). The JVM exports `SPARK_REUSE_WORKER=1` (`PythonRunner.scala:668-669`), and the forked child then loops over `worker(sock)` for further tasks rather than exiting (`daemon.py:167, 260-267`). Each worker runs one task at a time. Parallelism comes from having many processes.
- **What ships with each UDF [read].** On the driver, `_prepare_for_python_RDD` cloudpickles `command = (func, returnType)` with `CloudPickleSerializer`. If the payload is over the broadcast threshold (1 MiB by default), it is turned into a broadcast variable (`spark python/pyspark/core/rdd.py:5106-5117`; `sql/udf.py:51-72`). The JVM-side `PythonFunction` carries `command: Seq[Byte]`, `envVars`, `pythonIncludes`, `pythonExec`, `pythonVer`, and `broadcastVars` (`core/.../api/python/PythonRDD.scala:84-101`). PySpark vendors its own `pyspark.cloudpickle`.
- **Socket protocol [read].** For each task the runner writes the following, in order: the Python version (`PythonRunner.scala:934`), then the spark-files dir and `pythonIncludes`, then the broadcasts, then the command bytes (`PythonRunner.scala:1022-1028`). On the worker side, `worker.py` checks the version, puts the includes on `sys.path`, sets up the broadcasts, and unpickles the command (`python/pyspark/worker.py:116, 126-127, 143`). Data then streams in the same connection. Batched pickle is used for plain UDFs.
- **Arrow UDFs [read].** pandas, Arrow, and Arrow-optimized UDFs use `ArrowPythonRunner`, which writes an Arrow IPC stream (`ArrowStreamWriter`) onto the worker socket (`sql/core/.../python/PythonArrowInput.scala:44, 103`). The worker reads it with `pa.ipc.open_stream` (`python/pyspark/sql/pandas/serializers.py:125`). Batches are capped by `spark.sql.execution.arrow.maxRecordsPerBatch`, default 10000 (`SQLConf.scala:5536-5543`), and by the internal `maxBytesPerBatch` (`SQLConf.scala:5581`). Regular `udf()` gets Arrow transport through `useArrow=True` or `spark.sql.execution.pythonUDF.arrow.enabled`, available since 3.4 (`SQLConf.scala:5854-5859`). That setting **defaults to true from Spark 4.2** (`python/docs/source/migration_guide/pyspark_upgrade.rst:32`). The rows still cross a process boundary, serialized as Arrow IPC.
- **Python version check [read].** The worker compares the driver's `major.minor` with its own and raises `PYTHON_VERSION_MISMATCH`: "Python in worker has different version … PySpark cannot run with different minor versions" (`python/pyspark/worker_util.py:84-100`; `python/pyspark/errors/error-conditions.json:856-861`). Patch versions may differ.
- **Shipping environments [read].** The "Python Package Management" page is `python/docs/source/tutorial/python_packaging.rst`. It lists these options:
  - native: `--py-files`, `spark.submit.pyFiles`, `addPyFile` (lines 63-71). These are zip, egg, or .py files on `sys.path`.
  - conda-pack: a `.tar.gz` shipped with `--archives` or `spark.archives`, unpacked on executors, plus `PYSPARK_PYTHON=./environment/bin/python` (lines 80-131).
  - venv-pack: the same pattern, but "requires all nodes in a cluster to have the same Python interpreter installed" because the interpreter is a symlink (lines 134-166).
  - PEX: `--files env.pex` with `PYSPARK_PYTHON=./env.pex`. The interpreter is not bundled (lines 191-227).
  - uv: section at line 60.
  `spark.archives` dates from 3.1 (`core/.../config/package.scala:2623`) and `spark.pyspark.python` from 2.1 (`package.scala:1203`). The executor runs the `pythonExec` string the driver chose (`PythonRunner.scala:597`), so a relative `./environment/bin/python` resolves inside each executor's unpacked archive.
- **Spark Connect [read].** The client cloudpickles `(func, output_type)` into `PythonUDF.command` and sends `python_ver = "%d.%d" % sys.version_info[:2]` (`python/pyspark/sql/connect/expressions.py:762-766`; `connect/udf.py:233-240`). The server builds a `SimplePythonFunction` from four inputs:
  - `pythonExec` comes from the **server's** `PYSPARK_PYTHON` env, with `python3` as the default (`SparkConnectPlanner.scala:119-120`).
  - `pythonVer` is the client's.
  - `pythonIncludes` are the client includes plus session artifacts.
  - The env vars and broadcasts are empty.

  This is at `SparkConnectPlanner.scala:2392-2404`. Executors then run the classic worker path, so a client whose minor version differs from the cluster's Python fails with the same `PYTHON_VERSION_MISMATCH`.
- **New on master: an opt-in embedded-interpreter mode [read].** SPARK-59718 ("Add opt-in in-process Python UDF execution via Arrow C Data Interface") was merged on 2026-10-21 (commit `32d190f129`). `@inprocess_udf` runs pyarrow-in, pyarrow-out functions inside the executor JVM through JEP. The design points are in `docs/sql-pyspark-inprocess-udf.md`:
  - **one `SharedInterpreter` on a dedicated thread per executor**, so calls from concurrent tasks queue up. The docs recommend `executor.cores=1` and getting parallelism from more JVMs (lines 27-33, 350-356).
  - zero-copy exchange over the Arrow C Data Interface, with full Arrow validation of results "because the JVM reads result buffers without bounds checks" (lines 66-73, 188-194).
  - **a hung UDF blocks every in-process UDF on that executor, and recovery requires replacing the executor process** (lines 39-44).
  - none of the following work: broadcasts, accumulators, `--py-files`, `addPyFile`, artifacts, per-session `spark.pythonWorkerEnv.*`, or Python memory limits. Spark Connect is rejected. UDFs must not use `multiprocessing` (lines 126-166, 168-170).
  - the driver's major.minor must equal the Python that JEP was built against (`IN_PROCESS_PYTHON_VERSION_MISMATCH`, `error-conditions.json:560-565`; doc lines 177-179, 341).

  The classic worker model stays the default [read, line 80].

### Ballista: Python UDFs not supported on `main`; an open PR embeds them in executors via datafusion-python's codecs

- **On `main` [read].** `python/README.md:38` says Python UDF support is "still being worked on". Line 84 says "No support for `UDF` as DataFusion Python does not serialise them"; that is stale for dfpy 55. The tests mark UDFs `xfail(strict=True)`: "python udfs cannot be serialized (no logical extension codec handles them)" (`python/python/tests/test_context.py:52-55`). `python/src/codec.rs:17-27, 56-62, 117-123` is a pass-through, documented as where Python UDF serde "will live".
- **Tracking [read].** The long-running issue is apache/datafusion-ballista#173 (2022, open). Its discussion covers dependency shipping (per-executor venv built from declared deps, PEX), remote or "HTTP UDF" processes, and a PoC (milenkovicm/ballista_python, zero-copy over Arrow C). The umbrella issue is #2618, which lists "Python UDF support … #2619".
- **PR #2619 (open, head `eff2e350`) [read].** It enables `datafusion-python = "55.0.0"` (`default-features = false`) and wraps its **`PythonLogicalCodec`/`PythonPhysicalCodec`**, with Ballista's codecs as the terminal codec. `BallistaScheduler` and `BallistaExecutor` (pyo3 classes in `python/src/cluster.rs`) install them through `override_logical_codec`/`override_physical_codec`. The FFI planner shim recognises the `DFPYUDF`/`DFPYUDA`/`DFPYUDW` prefixes. The README text it adds says the client, scheduler, and executors "have to run the same Python minor version as the client, and everything the UDF imports has to be installed there". Known gap: Python UDFs used as shuffle-partitioning keys are not inlined.
- **Execution model [read+inferred].** dfpy's `PythonFunctionScalarUDF::invoke_with_args` wraps each batch in `Python::attach`, converts the arguments to pyarrow, and calls the function (`dfpy src/udf.rs:180-200`). Ballista therefore runs Python **in-process inside the executor**, which is itself a Python process that started `BallistaExecutor` [read: cluster.rs pyclass]. It has no worker processes and no environment shipping. Calls are serialized by the GIL across the executor's tokio threads, and a crash in UDF native code takes down the executor [inferred]. The standalone Rust `ballista-executor` binary does not link Python [inferred from Cargo deps: no pyo3 under `ballista/`].

### Ray: worker processes per node, started by the raylet; functions exported via cloudpickle through GCS

- **Processes [read].** "Ray runs arbitrary functions asynchronously on separate worker processes" (`ray doc/source/core/key-concepts.md:17`). Each node's raylet keeps a `WorkerPool` and starts worker processes with `StartWorkerProcess` (`ray src/ray/raylet/worker_pool.cc:495`). A pooled worker is reused only for a lease with the **same job ID and the same runtime_env hash**. Otherwise the result is `ROOT_MISMATCH` or `RUNTIME_ENV_MISMATCH` and a new worker starts (`worker_pool.cc:1373-1396`). Idle workers are killed after a timeout (`kill_idle_workers_interval_ms`, `worker_pool.cc:216-220`).
- **Function shipping [read].** The driver cloudpickles the remote function using the vendored `ray.cloudpickle`. `FunctionActorManager.export` puts `{job_id, function_id, module, function: pickled, …}` into the GCS internal KV, keyed by job and function ID (`ray python/ray/_private/function_manager.py:219-266`). Workers fetch and unpickle it lazily (`function_manager.py:289-314`). Code is resolved by pickle-by-reference against the worker's `sys.path` unless it is dynamic.
- **Arguments and data [read].** Data goes through a pickle-5 backport plus cloudpickle, using the shared-memory Plasma object store. NumPy buffers are zero-copy for readers on the same node (`doc/source/core/objects/serialization.md:11-31`).
- **Environments, `runtime_env` [read].** Fields are `working_dir`, `py_modules`, `py_executable`, `pip`, `uv`, `conda`, `env_vars`, `image_uri` (container), and `config` (`doc/source/core/handling-dependencies.md:428-606`). They can be set per job or per task/actor. Envs are installed eagerly at `ray.init` by default and cached per node, with a 10 GB default cache for each field (`:604, 613, 706-722`). "Ray starts a new worker for every Ray actor or task that requires a new runtime environment" (`:722`). pip envs are virtualenvs with `--system-site-packages` over the base Python (`python/ray/_private/runtime_env/virtualenv_utils.py:101`), so the **Python version is fixed by the cluster**. "`conda` environments must have the same Python version as the Ray cluster" (`handling-dependencies.md:274`).
- **Version matching [read].** `check_version_info` raises "Version mismatch" if the Ray versions differ, or if the Python version differs at `RAY_DEFAULT_PYTHON_VERSION_MATCH_LEVEL`, which defaults to **`patch`**. At `minor` level, a patch mismatch only logs a warning (`python/ray/_private/utils.py:1366-1440`). The uv docs repeat that the Ray and Python versions must match the cluster's (`handling-dependencies.md:334`).

### Daft: a Rust engine embedded in Python; UDFs run in-process by default, with an optional subprocess per UDF instance

- **Local [read].** The UDF operator selects `use_process = (is_actor_pool_udf || use_process.unwrap_or(false)) && all input and output dtypes are Arrow` (`daft src/daft-local-execution/src/intermediate_ops/udf.rs:375-389`).
  - The default is `UdfHandle::Thread`: the UDF runs in the host interpreter on a swordfish thread, under the GIL (`udf.rs:163-166, 540-546`).
  - The docs say `use_process=None` "lets Daft pick at runtime based on observed performance" (`docs/custom-code/func.md:440-442`; `daft/udf/__init__.py:69`). At this commit the operator code treats `None` as `false`, so I found no runtime switch [read].
  - `is_actor_pool_udf` means `concurrency` is set and the UDF is not async (`src/daft-dsl/src/functions/python/mod.rs:225-227`).
- **Subprocess protocol [read].** `UdfHandle` (`daft/execution/udf.py:58-113`) works like this:
  - It starts `sys.executable -m daft.execution.udf_worker` with a copy of the parent's environment. That is the same interpreter and venv, so no version check is needed.
  - Control goes over `multiprocessing.connection` on a Unix socket with a random authkey.
  - It sends the cloudpickled `ExpressionsProjection` once (`_ENTER`).
  - For each batch, the input `RecordBatch` is written as an **Arrow IPC stream into POSIX shared memory** (`SharedMemoryTransport`, `udf.py:33-55, 134-139`). The worker evaluates it and writes the result back the same way (`daft/execution/udf_worker.py:26-60`). Each direction copies once.
  - The worker caps its own compute runtime at one thread (`udf_worker.py:41`). Stdout is relayed with divider lines.
  The docs give the motivation: functions that are "not thread-safe or hold the GIL heavily" (`func.md:442`; `docs/custom-code/cls.md:266`).
- **Serialization [read].** Python objects inside Rust plans are serde'd with `daft.pickle`, which is vendored cloudpickle, via `serialize_py_object`/`deserialize_py_object` (`src/common/py-serde/src/python.rs:25-60`; `daft/pickle/pickle.py:5-14`).
- **Distributed on Ray, "Flotilla" [read].**
  - Each node gets one `RaySwordfishActor` (`@ray.remote`, async). It is a Ray worker process that hosts the native executor, with `num_cpus` threads set from the node's Ray CPUs (`daft/runners/flotilla.py:139-185, 477`).
  - Local physical plans go to it as Ray actor-method arguments (`flotilla.py:212-214`).
  - UDFs therefore run in that actor's interpreter, or in a `use_process` subprocess of it. Class or concurrency UDFs get dedicated Ray `UDFActor`s, each its own process, restartable (`daft/execution/ray_actor_pool_udf.py:28-55`; `src/daft-distributed/src/pipeline_node/actor_udf.rs:64-71`).
  - The environment is whatever Ray's `runtime_env` supplies. The docs example is `ray.init(..., runtime_env={"pip": ["daft"]})` (`docs/distributed/ray.md:81-82`). Daft only adds `env_vars` (`flotilla.py:461-475`).
  - The Python-version and Ray-version rules are Ray's [inferred].

### Sail (Rust, DataFusion-based Spark Connect server): an embedded interpreter via pyo3 in every process, including cluster workers

- **Process [read].** The `sail` command is a Python console script, `pysail.cli:main` (`sail pyproject.toml:77-78`). The Rust engine is a Python extension module running in that interpreter. In local mode it is one process (`docs/concepts/architecture/index.md`). In cluster mode the driver launches **worker containers** with `sail worker` from the same image (`crates/sail-execution/src/worker_manager/kubernetes.rs:402-407`). The local worker manager spawns worker actors in-process (`worker_manager/local.rs:31`).
- **Execution [read].** "The Python interpreter running the UDF is in the same process as the Rust-based execution engine". Arrow UDFs share buffers "directly via pointers" (`docs/guide/udf/performance.md`). `PySparkUDF::invoke_with_args` calls `Python::attach` on the polling thread (`crates/sail-python-udf/src/udf/pyspark_udf.rs:134-155`). The streaming UDFs drop the GIL while waiting for input (`stream.rs:73, 171`).
- **Serialization and PySpark reuse [read].** Sail keeps PySpark's cloudpickled `command` bytes. `PySparkUdfPayload::build` prepends PySpark's worker preamble (eval type, conf, arg offsets) for the detected PySpark version (3.x/4.0/4.1/4.2). `load` then calls **`pyspark.worker.read_udfs`** in the embedded interpreter, so PySpark's wrappers apply, and the `pyspark` package must be installed next to Sail (`crates/sail-python-udf/src/cereal/pyspark_udf.rs:18-120`). In cluster mode the payload bytes travel inside the protobuf physical plan (`UdfKind::PySpark{payload, …}`, `crates/sail-execution/src/proto/codec.rs:3335-3366`) and are decoded on the worker.
- **Version check [read].** `check_python_udf_version` requires `Python::version_str()` to start with the client's `python_ver`, which is effectively major.minor (`crates/sail-python-udf/src/cereal/mod.rs:45-54`). The docs ask users to choose a Python compatible with their PySpark version (`docs/guide/deployment/_common/python-version.md`).
- **Environment [read+inferred].** Sail has no per-job environment shipping and does not implement `addArtifacts` for Python [inferred: none found]. Dependencies come from the server or worker image [read: k8s `image` option].
- **Costs of embedding [read], all in open issues from 2026-08:**
  - lakehq/sail#2472: UDFs run inline on the primary tokio runtime, so a saturating UDF batch starves the Spark Connect gRPC server. The cause is thread occupancy, not the GIL.
  - #2456: a per-call `Python::attach` destroys the `PyThreadState` after each call, which breaks `threading.local`-tied native state and gave a use-after-free SIGSEGV in pyproj.
  - #2471: SIGABRT when a session is reaped while a UDF is in flight.
  - #2462: free-threaded (3.14t) support, to get rid of GIL serialization.

### Comet (one line) [read]

Comet does not run Python itself. Python UDFs use Spark's worker processes. The opt-in `spark.comet.exec.pyarrowUDF.enabled` path (`mapInArrow`/`mapInPandas`, and in open PR apache/datafusion-comet#5744 `CometArrowEvalPythonExec`) feeds Comet's native Arrow batches straight into Spark's Arrow Python runner, skipping the ColumnarToRow step (`spark/src/main/spark-4.1+/org/apache/spark/sql/comet/CometArrowEvalPythonExec.scala`; issue #5386). Polars Cloud: not checked.

### Synthesis

- **Two patterns.**
  1. **Separate Python worker processes** fed over a local IPC channel. Spark, Ray, and Daft's `use_process`/actors do this, and Comet does by reuse. The channels are Spark's socket stream (pickle or Arrow IPC), Ray's Plasma shared memory, and Daft's Unix socket plus shm with Arrow IPC.
  2. **An embedded interpreter in the engine process**, passing Arrow through the C Data Interface. Sail, Daft's default local mode, Ballista PR #2619 (via dfpy), and Spark's new opt-in `inprocess_udf` do this. [read]
- **Why processes [read+inferred].** Every embedded design has to manage the GIL. Spark's in-process mode serializes all of an executor's calls onto one thread. Sail and Daft serialize on the GIL and are moving to no-GIL or opting into processes. Processes are also how these systems get:
  - crash containment: in-process, a hang or segfault takes down the executor (Spark in-process docs lines 39-44; Sail #2456/#2471).
  - cancellation, which you get by killing the process.
  - per-job or per-task environments: Ray's pool is keyed by runtime_env hash, and Spark's `pythonExec` and archives are per application.
  - memory limits: `spark.executor.pyspark.memory` cannot apply in-process.
  - freedom from `multiprocessing`/fork hazards in a multithreaded host.

  Embedding buys zero-copy and no IPC. That is why Sail, Spark's new mode, and Ballista's PR all advertise it.
- **Environments [read].** No system ships an interpreter to the worker per query. The options are:
  - pre-baked images: Sail, Ballista, Ray's `image_uri`.
  - archives unpacked on executors with `pythonExec` pointing into them: Spark's conda-pack, venv-pack, and PEX.
  - per-node, cached, on-demand venv or conda installs keyed by hash: Ray's `runtime_env`, which Daft inherits.

  Shipping a pip or conda env still requires that the base Python version matches. Embedded designs can't switch environment per job, because the interpreter is fixed when the process starts and `sys.modules` is shared (Spark in-process docs lines 117-124, 152-155).
- **Python version constraint [read].** Every system checks it, because cloudpickle bytecode is not portable across minor versions.
  - Spark checks major.minor in both worker and in-process mode, and through Connect.
  - Sail checks major.minor.
  - Ray checks Ray version plus Python **patch** by default.
  - dfpy 55 already frames its payloads with Python major.minor (`dfpy src/codec.rs:56-64`).
  - Spark, Ray, and Daft also vendor cloudpickle, which pins the pickler version to the framework version [read: `pyspark.cloudpickle`, `ray.cloudpickle`, `daft.pickle.cloudpickle`].
- **Implications for our backend [inferred].**
  - (a) The cheapest remote form follows Ballista PR #2619 and Sail: embed the interpreter in the worker process, decode `DFPYUDF` payloads with dfpy's codec, and require an image whose Python major.minor and packages match the client.
  - (b) If we need isolation, cancellation, or per-job envs, the proven form is a worker-side Python process pool speaking Arrow IPC over a socket or shm, as Spark and Daft do. The pool would be keyed by env, as Ray's is. Either way, a worker-side version check against the payload header is mandatory.
  - (c) Run UDF calls off the async runtime's core threads (`spawn_blocking` or a dedicated pool), per Sail #2472.

| System | Where UDF Python runs | Data channel | UDF serialization | Env shipping | Version rule |
|---|---|---|---|---|---|
| PySpark (classic) | Worker processes forked by a per-executor `pyspark.daemon`; reused | Local TCP/UDS socket: pickle batches or Arrow IPC (10k rows/batch) | cloudpickle `command` bytes (broadcast if >1 MiB) + includes + broadcasts | `--py-files`, `spark.archives` (conda-pack/venv-pack), PEX, uv; `spark.pyspark.python` | Driver = worker major.minor |
| Spark Connect | Same as classic, on the server's executors | Same | Client cloudpickles; `python_ver` in proto | Server `PYSPARK_PYTHON`; `addArtifacts` | Client = worker major.minor |
| Spark `inprocess_udf` (master, opt-in) | JEP `SharedInterpreter`, one thread per executor JVM | Arrow C Data Interface, zero-copy | cloudpickle, registered once per task | Preinstalled + `sitePackages`; no py-files or artifacts | Driver = JEP Python major.minor |
| Ballista | Not supported on main; PR #2619: in-process in a Python-hosted executor (dfpy `Python::attach`) | In-process pyarrow (C Data Interface) | dfpy `DFPYUDF` cloudpickle via `PythonLogical/PhysicalCodec` | None (preinstall) | Same major.minor on client, scheduler, executors |
| Ray | Raylet-managed worker processes per node; pool keyed by job + runtime_env | Plasma shared memory, pickle-5 out-of-band | cloudpickle into GCS KV, fetched lazily | `runtime_env`: pip, uv, conda, working_dir, py_modules, py_executable, image_uri; cached per node | Ray version + Python patch (configurable to minor) |
| Daft local | Default: host interpreter thread (GIL). `use_process=True`/actor UDF: `sys.executable` subprocess | Unix socket control + POSIX shm with Arrow IPC | cloudpickled `ExpressionsProjection` | Same venv as parent | N/A (same interpreter) |
| Daft on Ray (Flotilla) | One `RaySwordfishActor` per node (in-process or subprocess); `UDFActor`s for class UDFs | Ray object store / actor calls | cloudpickle (`daft.pickle`) inside plan serde | Ray `runtime_env` | Ray's rules |
| Sail | Embedded pyo3 interpreter in server and every worker process | In-process Arrow pointers | PySpark cloudpickle command, loaded via `pyspark.worker.read_udfs` | Worker image | Runtime Python startswith client major.minor |
| Comet | Spark's Python workers | Native Arrow batches into Spark's Arrow runner | Spark's | Spark's | Spark's |

## 4. Consequences for the backend's shape now

The backend as it stands, for reference (**read**):

- `pipeline::run` builds an IO runtime and a CPU runtime with `threads` workers each, both
  multi-thread (ADR 0001).
- It spawns the pipeline onto the CPU runtime and **blocks the calling thread** in
  `io_runtime.block_on` until the pipeline finishes (`src/pipeline.rs:136-176`).
- The CPU runtime lives on its own thread and drops pending tasks at their next yield
  (`src/pipeline/cpu_runtime.rs:15-19, 71`).
- The throughput probe's sampler is spawned on the IO runtime (`src/sink.rs:205-231`).
- No pyo3 code exists yet. Per #313, our cdylib will ship in the client and hold the in-process
  backend.

What would make Python UDFs harder later, and the cheap rule that avoids each:

- **Deadlock at the in-process entry point.**
  - The frontend calls `backend.execute(bytes, action)` holding the GIL (or, free-threaded,
    attached). `pipeline::run` then blocks that thread. The first CPU worker to call
    `Python::attach` for a UDF waits forever for a GIL the blocked caller holds. **inferred**
    from `src/pipeline.rs:165` and pyo3's attach semantics.
  - Even with no UDFs, holding the GIL there freezes every other Python thread for the whole run.
    Free-threaded, a thread that stays attached while blocking can hang the others at the next
    stop-the-world (pyo3 `free-threading.md:93-108`, **read**).
  - **Rule:** the pyo3 entry point wraps `pipeline::run` in `py.detach(..)`, and checks signals
    periodically so Ctrl-C works. datafusion-python's `wait_for_future` is the model
    (`crates/util/src/lib.rs:79-114`, **read**). Cheap now; it belongs in the first in-process
    prototype.
- **Our cdylib re-enabling the GIL.**
  - If any extension module imported into a free-threaded interpreter does not declare
    `gil_used = false`, CPython turns the GIL back on for the whole process (pyo3 guide
    `free-threading.md:29-32`, **read**).
  - pyo3 ≥ 0.28 defaults to `gil_used = false` (**read**), so our module just has to be
    thread-safe: no `GILOnceCell`-style assumptions, and `PyOnceLock`/`Mutex` for shared state.
  - Free-threaded builds have no stable ABI before 3.15's `abi3t` (pyo3 `CHANGELOG.md:103`,
    **read**). Supporting 3.14t therefore means shipping a `cp314t` wheel next to an abi3 one,
    as datafusion-python already does (**read**).
  - **Rule:** build our cdylib with pyo3 ≥ 0.28, keep `gil_used = false`, and keep a 3.14t CI
    leg once there is a wheel.
- **Tokio workers blocked on Python.**
  - A worker waiting for the GIL, or running a long Python call, blocks its whole Tokio thread.
    Other operators scheduled on that worker stall with it: the merge tree's buffered
    `spawn_buffered` tasks, sinks, and the scans' decode (**inferred**). Sail hit the same thing:
    UDFs on its main runtime starved its gRPC server (lakehq/sail#2472, **read**).
  - The fit with ADR 0006 is a **third pool owned by the runner**, carried as a `SessionConfig`
    extension like `IoRuntime` (`src/pipeline.rs:179-201`). A Python UDF would `await` its call
    on that pool rather than run inline on a CPU worker. Its size would follow the GIL: 1 to a
    few threads on a GIL build, up to the core count free-threaded. **inferred**
  - Nothing in today's runner blocks adding this. Ordering-related physical properties are
    unaffected, because a UDF is just a scalar, aggregate or window function in a stock node.
    **inferred**
  - The probe sampler already sits on the IO runtime, so a Python-saturated CPU runtime does not
    skew samples (**read**, `src/sink.rs:205-231`).
- **Cancellation and shutdown.**
  - A probe stops by dropping the stream, and `CpuRuntime` drops tasks at their next yield. A
    Python call in progress can't be interrupted; it finishes first (**inferred**). That costs
    one batch of latency, not correctness.
  - In-process, a UDF that hangs or segfaults takes the user's interpreter with it. That is the
    same as datafusion-python today. **inferred**
- **Decoding: what the backend codec does with `DFPYUD*`.**
  - **Rule now:** recognize the three prefixes and fail with a clear "Python UDFs are not
    supported by this backend" error (#317 already recommends recognizing and refusing).
  - **Later, choose between:**
    - (a) Decode datafusion-python's private wire format, ourselves or through the
      `datafusion-python` crate (§1). Version lockstep with the wheel, per #314.
    - (b) Hail-owned UDF shipping. The frontend turns inlining off, so UDFs cross by name (#317),
      and the Hail entry point carries the callables next to the plan bytes in a format Hail
      owns.

    (b) is what PySpark does: `PythonFunction.command` travels beside the plan (§3). It decouples
    us from a private format that changed in 55.0. **inferred**
  - **In-process needs no pickling.** The backend runs in the frontend's own interpreter, so with
    (b) the entry point can pass the live Python callables directly. Version and environment
    match trivially. **inferred**
- **The remote seam.** #313 already treats the plan bytes and the Action as opaque payloads,
  which leaves room for UDF payloads (**read**). What the remote form must allow:
  - **Workers that link libpython, or own Python processes.** Embedding means a backend binary
    built per Python minor (or per `t` build), linked against `libpython3.X`. The experiment's
    `codec-reuse/build.rs` had to add that link, because datafusion-python forces pyo3's
    `extension-module` feature (**ran**). A process pool means an Arrow IPC protocol, as in Spark
    and Daft (§3). Either way the worker can't be "a static Rust binary, full stop", which #316
    already says.
  - **An environment contract.** The worker image or archive provides a Python matching the
    payload header's major.minor (better, the exact version, per cloudpickle's README), plus
    `cloudpickle`, `pyarrow` and the user's packages.
  - **A security boundary.** Unpickling executes arbitrary code (cloudpickle `README.md:17-25`,
    **read**). A shared remote backend must run each user's UDFs where that user's code is
    allowed to run, which favours per-job worker processes (Ray keys its worker pool by
    job and runtime_env) over one embedded interpreter per shared worker. **inferred**
  - Scans as descriptions, the Action as an argument, and backend-side opening of datasets are
    all independent of UDFs. **inferred**
- **Sinks.** Sinks, `PartitionedSinkExec` and the merge tree beneath them are Rust and see only
  Arrow batches. A UDF upstream changes nothing about them. **inferred**
- **Python UDAFs are slow by construction.** datafusion-python's Python accumulator has no
  `GroupsAccumulator`, so a grouped Python UDAF gets one Python object per group and one call per
  group per batch (§1, **read** / **inferred**). That is a performance limit for whoever builds
  UDAFs, not a constraint on the backend's shape.
