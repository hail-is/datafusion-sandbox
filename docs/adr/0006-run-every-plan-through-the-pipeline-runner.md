# Run every DataFusion plan through the pipeline runner

Everything in this crate that executes a DataFusion plan goes through `pipeline::run`, including
test fixtures that write a few rows to a temporary directory. A caller may only build its own
runtime and execute a plan outside the runner for a measured performance win or a large reduction
in complexity. Neither has appeared so far, and the two candidates below were rejected.

The runner exists because a pipeline needs two Tokio runtimes rather than one. CPU-bound plan
execution and object store requests sharing a runtime delays the requests, and delayed requests
cause network flow control to throttle the available bandwidth in response, which compounds when
queries run concurrently. DataFusion documents this directly under [optimizing latency: throttled
CPU / IO under highly concurrent
load](https://docs.rs/datafusion/latest/datafusion/#optimizing-latency-throttled-cpu--io-under-highly-concurrent-load).
The symptom to watch for is a run that saturates neither the CPU nor the available bandwidth, or
high tail latency on object store requests.

The split is only real because `register_object_store` gives each store a
`SpawnedReqwestConnector` holding the IO runtime's handle. Without that, TLS decryption and HTTP
framing run on whichever runtime issued the request, and the pipeline is spawned onto the CPU
runtime, so all of it would land there. A caller cannot reproduce the arrangement by building two
runtimes of its own: the second half of it arrives with the object store registration.

Going around the runner also loses the guarantee that the CPU runtime is multi-thread, which
DataFusion's `spawn_buffered` treats as semantic rather than as a performance setting. See
[ADR 0001](0001-always-use-multi-thread-tokio-runtimes.md).

## What was rejected

Letting a caller that performs no object store IO skip the runner. The candidate was the test
fixture, which writes small local tables and needs neither runtime for throughput. A plain
`SessionContext` and a `block_on` on a current-thread runtime would be a few lines shorter and
would drop a pair of runtimes per fixture. Rejected because the fixture executes DataFusion plans,
and on a current-thread runtime `spawn_buffered` returns its streams unbuffered, so `SortExec`'s
in-memory merge and spill reads behave differently there than they do in a combiner run. Nothing
the fixture writes is sorted today. The rule costs a few lines; the exception costs the first
fixture that does sort executing a different plan from the code it stands in for, with nothing in
the file to say so.

Reading the rule backwards, as "the fixture uses the runner only because it is the only writing
path available," also gets rejected here. It is the rule, not an accident of what happens to be
reachable, and a plain writing helper that bypasses the runner is not the fix.

## Consequences

A test or fixture that executes a plan pays for a pair of runtimes. Measured at one thread that is
roughly 0.8 ms per run, so runtime construction is not a reason to deviate and should not be cited
as one. A caller that believes it has a case for deviating should first check whether its plan can
contain a `SortExec`, a `SortPreservingMergeExec`, or a spill, because those are what the flavor
guarantee protects.
