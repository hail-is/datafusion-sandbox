# Python UDF GIL-contention experiment (issue #319)

A throwaway Rust binary that embeds CPython through pyo3 0.29 (`auto-initialize`), the pyo3 and
arrow (59) majors that datafusion-python 55 uses. It asks: if the backend ran a Python scalar UDF
on every partition of a plan, from the worker threads of a multi-thread Tokio runtime, how would
throughput scale with the number of workers, on a GIL build and on a free-threaded build?

Each of N tasks on an N-worker Tokio runtime loops for a fixed time, doing per call what
datafusion-python's `PythonFunctionScalarUDF::invoke_with_args` does per batch: inside one
`Python::attach`, export a Rust Arrow array to pyarrow over the C data interface
(`arrow::pyarrow::ToPyArrow`), call the Python function, and import the result
(`FromPyArrow`). The task yields to Tokio between calls, as an operator does between batches.
Its own `[workspace]` keeps it out of the repo's workspace.

Workloads (`src/main.rs`, `UDFS`):

| name | Python body | input | holds the GIL for |
|---|---|---|---|
| `pure_loop` | 200k-iteration integer loop, ignores the input | 1 row | everything |
| `rowwise` | `pa.array([x * x % 7 for x in arr.to_pylist()])` | 100k int64 | everything |
| `numpy_kernel` | `pa.array(np.sin(a) * np.cos(a))`, `a = arr.to_numpy()` | 1M float64 | conversions only; ufunc loops release it |
| `pyarrow_kernel` | `pc.multiply(pc.sin(arr), pc.cos(arr))` | 1M float64 | wrapper code only; Arrow C++ kernels release it |

## Running

```sh
uv python install 3.14 3.14t
for v in 3.14 3.14t; do
  uv venv -p $v /tmp/pyudf-venv-$v
  uv pip install --python /tmp/pyudf-venv-$v/bin/python numpy pyarrow
done
./run.sh 3.14  2 1,2,4,8 5   # SECONDS THREADS REPS [WORKLOADS]
./run.sh 3.14t 2 1,2,4,8 5
```

`run.sh` builds with `PYO3_PYTHON` pointed at the uv-managed interpreter (one target dir per
interpreter through `SANDBOX_TARGET_SUFFIX`), then runs the binary with `LD_LIBRARY_PATH` set to
the interpreter's `lib/`, so the binary finds `libpython3.14[t].so`, and with `PYTHONPATH` set to
the venv's site-packages. Each configuration runs REPS times, with thread counts interleaved, and
the table reports the median rate.

## Results (2026-10-26)

Machine: the Lima sandbox VM, 8 vCPU arm64 Linux on an Apple-silicon host. The timings are
native, not Rosetta, but the VM is shared with other agent sessions. Load average was about 2.5 to
4.5 throughout, so on top of the experiment about 3 to 4 other threads were runnable. 8-worker
runs therefore cannot reach 8×; trust the 2- and 4-worker rows more.

Versions: CPython 3.14.9, 3.14.9t and 3.13.15 from uv; numpy 2.5.4; pyarrow 26.0.0. numpy and
pyarrow both ship cp314t wheels and keep the GIL disabled on import (`sys._is_gil_enabled()` stays
`False`).

Each configuration ran 5 × 2 s. The tables show the median calls/s, with the speedup relative to 1
worker.

CPython 3.14.9 (GIL):

| workload | 1 | 2 | 4 | 8 workers |
|---|---|---|---|---|
| pure_loop | 112.5 (1.00×) | 113.5 (1.01×) | 110.0 (0.98×) | 114.5 (1.02×) |
| rowwise | 135.5 (1.00×) | 133.0 (0.98×) | 131.0 (0.97×) | 133.0 (0.98×) |
| numpy_kernel | 83.5 (1.00×) | 168.0 (2.01×) | 322.0 (3.86×) | 532.5 (6.38×) |
| pyarrow_kernel | 86.5 (1.00×) | 170.0 (1.97×) | 337.0 (3.90×) | 568.5 (6.57×) |

CPython 3.13.15 (GIL):

| workload | 1 | 2 | 4 | 8 workers |
|---|---|---|---|---|
| pure_loop | 90.5 (1.00×) | 91.5 (1.01×) | 93.5 (1.03×) | 96.0 (1.06×) |
| rowwise | 142.0 (1.00×) | 140.5 (0.99×) | 136.0 (0.96×) | 137.5 (0.97×) |
| numpy_kernel | 87.5 (1.00×) | 172.5 (1.97×) | 335.5 (3.83×) | 652.0 (7.45×) |
| pyarrow_kernel | 87.0 (1.00×) | 170.0 (1.95×) | 337.0 (3.87×) | 606.5 (6.97×) |

CPython 3.14.9t (free-threaded, GIL disabled):

| workload | 1 | 2 | 4 | 8 workers |
|---|---|---|---|---|
| pure_loop | 116.5 (1.00×) | 224.0 (1.92×) | 411.0 (3.53×) | 542.5 (4.66×) |
| rowwise | 105.5 (1.00×) | 191.5 (1.82×) | 385.0 (3.65×) | 695.0 (6.59×) |
| numpy_kernel | 83.5 (1.00×) | 164.5 (1.97×) | 329.5 (3.95×) | 620.0 (7.43×) |
| pyarrow_kernel | 84.0 (1.00×) | 166.0 (1.98×) | 323.0 (3.85×) | 612.5 (7.29×) |

A second 3.14t run of `pure_loop` and `rowwise` gave 5.10× and 6.56× at 8 workers. A second 3.14
GIL run of the two kernels gave 6.36× and 6.99× at 8 workers.

A discarded run: the first 3.14 GIL run had `numpy_kernel` and `pyarrow_kernel` plateauing at
2.0 to 2.8× from 4 workers up, with min-to-max spreads of up to 3× within a configuration. It
overlapped heavy load from another session. Two later runs, and the 3.13 run, all showed the
near-linear scaling above, with tight spreads, so I treat the plateau as interference rather than
a GIL effect.

## Payload probes (`payload/`) and codec reuse (`codec-reuse/`)

These are separate from the scaling experiment. Both need a venv with the stock wheel
(`uv venv -p 3.14 /tmp/v && uv pip install --python /tmp/v/bin/python datafusion==55.0.0`). The
`.bin` files go to `$PYUDF_OUT` (default `/tmp/pyudf-research`).

- `payload/probe_payload.py` (run with `python -I`) builds plans that carry a Python scalar UDF,
  UDAF and UDWF. For each `DFPYUD*` payload it prints the header and the shape of the unpickled
  tuple, and records every `module.name` that unpickling imports. `payload/write_plans.py` writes
  the full plan bytes.
- `payload/load_payload.py [--stub]` loads the payloads in an interpreter without `datafusion`.
  On 3.14 with `cloudpickle` and `pyarrow` only, the scalar loads and runs, and the UDAF and UDWF
  fail with `No module named 'datafusion'`, because their base classes
  `datafusion.user_defined.Accumulator` and `WindowEvaluator` are pickled by reference. With
  `--stub`, which puts a fake `datafusion.user_defined` into `sys.modules`, all three run.
  Payloads pickled on 3.14 load on 3.14t, and the reverse also works.
- `codec-reuse/` is a Rust binary that depends on the `datafusion-python` crate (55.0.0, from
  crates.io). It decodes the plan files with `PythonLogicalCodec::default()` and executes them.
  That crate turns on pyo3's `extension-module` feature, which stops pyo3 linking libpython, so
  `build.rs` links it explicitly. Build and run:

  ```sh
  lib=$(dirname $(uv python find 3.14))/../lib
  PY_LIBDIR=$lib PYO3_PYTHON=$(uv python find 3.14) cargo build --release
  LD_LIBRARY_PATH=$lib PYTHONPATH=/tmp/v/lib/python3.14/site-packages \
    <binary> $PYUDF_OUT/plan-{scalar,aggregate,window}.bin
  ```

  All three plans ran correctly. With only `cloudpickle` and `pyarrow` importable, the scalar plan
  ran and the UDAF plan failed to decode (`No module named 'datafusion'`).

Takeaways:

- On the GIL build, Python code that holds the GIL does not scale at all. With per-row Python or
  plain bytecode, 8 Tokio workers run at the speed of 1 on both 3.13 and 3.14.
- A UDF that spends most of each call inside GIL-releasing kernels scales nearly as well on the
  GIL build as on the free-threaded one: 3.9× at 4 workers on both builds, and 6.4 to 7.5× at 8.
  Here a call takes about 12 ms, mostly numpy or Arrow C++ work on 1M doubles, and only the small
  conversion and wrapper part of it holds the GIL. The GIL bites in proportion to the share of
  each call spent in Python (Amdahl's law), so smaller batches or more Python glue per call would
  scale worse.
- The free-threaded build also scales Python-heavy UDFs: `rowwise` reached 3.65× at 4 workers
  and 6.6× at 8. `pure_loop` reached 4.7 to 5.1× at 8; it may be held back by the VM's
  background load, but I have not shown that.
- At 1 worker, against the 3.14 GIL build, the free-threaded build was at parity on `pure_loop`
  and the kernels, and about 15 to 22% slower on `rowwise`, which allocates many objects (105 vs
  124 to 135 calls/s). CPython 3.14 documents roughly 5 to 10% on average.
