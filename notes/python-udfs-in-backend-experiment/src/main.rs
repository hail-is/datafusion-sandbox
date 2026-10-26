//! Throwaway experiment for notes/python-udfs-in-backend.md (issue #319): how does calling a
//! Python UDF from N Tokio worker threads scale, on a GIL build and on a free-threaded build?
//!
//! Each of N tasks (one per "partition") on an N-worker multi-thread Tokio runtime repeatedly
//! does what a backend's Python ScalarUDF would do per batch: export a Rust Arrow array to pyarrow
//! over the C data interface, call a Python function on it, and import the result back.
//!
//! Usage: pyudf-gil-experiment [SECONDS] [THREADS,...] [REPS] [WORKLOAD,...]

use std::collections::HashSet;
use std::ffi::CString;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayData, ArrayRef, Float64Array, Int64Array};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::prelude::*;
use pyo3::types::PyModule;

const UDFS: &str = r#"
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

def pure_loop(arr):
    # Pure-Python bytecode, touches no shared objects: the best case for free-threading.
    s = 0
    for i in range(200_000):
        s += i * i % 7
    return pa.array([s], type=pa.int64())

def rowwise(arr):
    # Per-row Python over the batch, like a row-at-a-time UDF (Polars map_elements style).
    return pa.array([x * x % 7 for x in arr.to_pylist()], type=pa.int64())

def numpy_kernel(arr):
    # Vectorized numpy ufuncs; numpy releases the GIL inside the loops.
    a = arr.to_numpy()
    return pa.array(np.sin(a) * np.cos(a))

def pyarrow_kernel(arr):
    # Vectorized Arrow C++ compute kernels; pyarrow releases the GIL inside them.
    return pc.multiply(pc.sin(arr), pc.cos(arr))
"#;

struct Workload {
    name: &'static str,
    input: ArrayRef,
}

fn workloads() -> Vec<Workload> {
    let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(0..100_000));
    let floats: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..1_000_000).map(|i| i as f64 * 1e-3),
    ));
    let one: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1));
    vec![
        Workload { name: "pure_loop", input: one },
        Workload { name: "rowwise", input: ints },
        Workload { name: "numpy_kernel", input: floats.clone() },
        Workload { name: "pyarrow_kernel", input: floats },
    ]
}

/// One UDF invocation: Rust Arrow -> pyarrow, call, pyarrow -> Rust Arrow.
fn call_udf(py: Python<'_>, f: &Py<PyAny>, input: &ArrayData) -> PyResult<usize> {
    let py_in = input.to_pyarrow(py)?;
    let py_out = f.call1(py, (py_in,))?;
    let out = ArrayData::from_pyarrow_bound(py_out.bind(py))?;
    Ok(out.len())
}

/// Runs `threads` tasks on a `threads`-worker runtime for `secs`; returns (calls, distinct OS threads).
fn run(f: &Arc<Py<PyAny>>, input: &ArrayData, threads: usize, secs: f64) -> (u64, usize) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .build()
        .unwrap();
    rt.block_on(async {
        // Warm up each worker's thread state and caches.
        let deadline = Instant::now() + Duration::from_millis(300);
        let warm: Vec<_> = (0..threads)
            .map(|_| {
                let (f, input) = (f.clone(), input.clone());
                tokio::spawn(async move {
                    while Instant::now() < deadline {
                        Python::attach(|py| call_udf(py, &f, &input)).unwrap();
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        for h in warm {
            h.await.unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs_f64(secs);
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (f, input) = (f.clone(), input.clone());
                tokio::spawn(async move {
                    let mut calls = 0u64;
                    let mut seen = HashSet::new();
                    while Instant::now() < deadline {
                        Python::attach(|py| call_udf(py, &f, &input)).unwrap();
                        calls += 1;
                        seen.insert(std::thread::current().id());
                        // DataFusion operators return to the scheduler between batches.
                        tokio::task::yield_now().await;
                    }
                    (calls, seen)
                })
            })
            .collect();
        let mut total = 0;
        let mut seen = HashSet::new();
        for h in handles {
            let (c, s) = h.await.unwrap();
            total += c;
            seen.extend(s);
        }
        (total, seen.len())
    })
}

fn main() -> PyResult<()> {
    let args: Vec<String> = std::env::args().collect();
    let secs: f64 = args.get(1).map_or(3.0, |s| s.parse().unwrap());
    let threads: Vec<usize> = args
        .get(2)
        .map_or("1,2,4,8".into(), |s| s.clone())
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    let reps: usize = args.get(3).map_or(5, |s| s.parse().unwrap());
    let only: Option<Vec<String>> =
        args.get(4).map(|s| s.split(',').map(String::from).collect());

    let funcs: Vec<(Workload, Arc<Py<PyAny>>)> = Python::attach(|py| -> PyResult<_> {
        let sys = py.import("sys")?;
        let version: String = sys.getattr("version")?.extract()?;
        let gil: bool = match sys.getattr("_is_gil_enabled") {
            Ok(f) => f.call0()?.extract()?,
            Err(_) => true,
        };
        println!("python {} | GIL enabled at runtime: {gil}", version.replace('\n', " "));
        let module = PyModule::from_code(
            py,
            &CString::new(UDFS).unwrap(),
            c"udfs.py",
            c"udfs",
        )?;
        let gil_after: bool = match sys.getattr("_is_gil_enabled") {
            Ok(f) => f.call0()?.extract()?,
            Err(_) => true,
        };
        println!("GIL enabled after importing numpy+pyarrow: {gil_after}");
        workloads()
            .into_iter()
            .filter(|w| only.as_ref().is_none_or(|o| o.iter().any(|n| n == w.name)))
            .map(|w| Ok((Arc::new(module.getattr(w.name)?.unbind()), w)))
            .map(|r: PyResult<_>| r.map(|(f, w)| (w, f)))
            .collect()
    })?;

    // The sandbox VM is shared and noisy: repeat each configuration, interleaving thread counts,
    // and report the median rate.
    println!("reps={reps} x {secs}s per configuration; rate is the median over reps");
    println!("workload        threads  calls/s  (min-max)        speedup  efficiency  os_threads");
    for (w, f) in &funcs {
        let input = w.input.to_data();
        let mut rates = vec![Vec::new(); threads.len()];
        let mut os_threads = vec![0; threads.len()];
        for _ in 0..reps {
            for (i, &n) in threads.iter().enumerate() {
                let (calls, seen) = run(f, &input, n, secs);
                rates[i].push(calls as f64 / secs);
                os_threads[i] = os_threads[i].max(seen);
            }
        }
        let median = |v: &mut Vec<f64>| {
            v.sort_by(f64::total_cmp);
            v[v.len() / 2]
        };
        let base = median(&mut rates[0]) / threads[0] as f64;
        for (i, &n) in threads.iter().enumerate() {
            let rate = median(&mut rates[i]);
            let speedup = rate / base;
            println!(
                "{:<15} {:>7}  {:>7.1}  ({:>6.1}-{:>6.1})  {:>6.2}x  {:>9.0}%  {:>10}",
                w.name,
                n,
                rate,
                rates[i][0],
                rates[i][rates[i].len() - 1],
                speedup,
                100.0 * speedup / n as f64,
                os_threads[i]
            );
        }
    }
    Ok(())
}
