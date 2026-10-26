# Ballista's remote seam, and what ours must keep

This note answers [#320](https://github.com/hail-is/datafusion-sandbox/issues/320): what does Ballista's remote seam look like, and what must the frontend-to-backend seam of [#313](https://github.com/hail-is/datafusion-sandbox/issues/313) keep so that a remote backend can be adopted later without redoing the in-process one? Checked on 2026-10-26.

Each claim is marked:

- **read**: read in source or docs, cited as `path:line` from the source root;
- **inferred**: follows from what was read, but nothing checked it directly.

Nothing was run. Every question could be settled by reading. Building Ballista's Python crate to test cancel-on-drop was not worth a full DataFusion build, since the source leaves no doubt.

Sources:

- `bal`: apache/datafusion-ballista `main` at `12a24f00203b09ea55b24f8c4d558708b0e93dcd` (2026-10-26), downloaded as a tarball. Its workspace is at version 55.0.0 on DataFusion 55. `python/` is the `pyballista` crate, also 55.0.0.
- `dfpy`: datafusion-python 55.0.0, the source unpacked for #317.
- `ffi`: datafusion-ffi 55.2.0. `proto`: datafusion-proto 55.2.0. `df`: datafusion 55.2.0 (the `datafusion` crate).
- Ballista issues and PRs, cited by number on apache/datafusion-ballista.

**Which Python client this describes.** Ballista's Python client on `main` is new and unreleased. It installs an FFI query planner on a stock `datafusion.SessionContext` (issue #2618, PR #2588). Its upgrade notes are filed under 56.0.0 and marked "awaiting release" (`bal docs/source/upgrading/56.0.0.md:26`, `:92-120`). The `ballista` package on PyPI is still 54.1.0, which requires `datafusion==54` (PyPI JSON API). That release wraps `SessionContext` and `DataFrame` in subclasses. It serializes the logical plan of each executing call and resubmits it through a separate session (#2618). This note describes `main`, since it is the design Ballista is moving to.

## Answers

1. **Python client.** `BallistaSessionContext` is a factory. It calls `ctx.with_extensions(BallistaExtension)` on a plain `SessionContext`, which installs Ballista's logical and physical codecs and an FFI query planner. The wheel runs its own analyzer and optimizer, then hands the *optimized* logical plan to that planner. The planner returns one leaf, `DistributedQueryExec`, which holds the plan. Executing the leaf encodes the logical plan as datafusion-proto `LogicalPlanNode` bytes with Ballista's codec. It sends them over gRPC (`ExecuteQueryParams.logical_plan`), along with every session config entry as string key-value pairs and a session id. The scheduler decodes the plan, optimizes it again and splits it into stages. Results come back only after the whole job succeeds: the client fetches each final-stage partition over Arrow Flight. Executors (or a Flight proxy) stream those partitions from Arrow IPC shuffle files on disk.
2. **Session state.** None of it persists across the wire. The scheduler builds a new `SessionContext` for every query from its own config producer plus the submitted key-value pairs, and drops keys it doesn't know without an error.
   - **Tables** travel inside the plan. Only datafusion-proto's built-ins cross: listing tables, views and empty tables. In-memory tables fail (a strict xfail in Ballista's suite), and so do `information_schema` scans.
   - **UDFs** travel by name and must be registered on the scheduler's session builder and every executor's function registry. Python UDFs don't work; PR #2619 is open.
   - **Object stores and credentials** never travel. Each process builds stores from its own registry. The stock binaries support only `file`, `http(s)` and `s3`, with credentials from the environment or from `s3.*` config keys sent in plain text. The client must also register its own store, because it lists files and infers schemas locally.
3. **Version coupling.** Ballista's major version follows DataFusion's.
   - **Client and scheduler** must share a major version. A `ballista-version` gRPC header, checked by both sides since 55.0.0, enforces it, because plan bytes carry no version.
   - **Scheduler and executors** must match a protocol integer exactly; they are upgraded together.
   - **The Python crate** pins `datafusion`, `datafusion-ffi` and `datafusion-proto` to `=55.2.0`. Its query planner rewrites two entries of datafusion-ffi's `FFI_QueryPlanner`, mirroring the private layouts of `FFI_QueryPlanner` and `FFI_SessionRef`. This works around the FFI logical codec's `not_impl_err!` for file formats, which otherwise breaks `DataFrame.write_*`. The mirror is valid only for that exact datafusion-ffi version. No upstream DataFusion issue for the gap turned up.
4. **Long-running and early-stopped work.**
   - **Cancel.** `CancelJob` (gRPC, or `PATCH /api/job/{id}`) marks the job failed with "Cancelled". It aborts each running task's future on the executors and drops pending tasks. The client calls it only after a version mismatch; dropping the result stream does not cancel the job (*inferred*: there is no `Drop` guard). Cancelling when a `LIMIT` is reached is an open issue (#628).
   - **Progress and metrics.** No progress or metrics stream back while a job runs. Job status says queued or running, with timestamps (`// TODO: add progress report`). Executors report operator metrics only when a task ends. `GetJobMetrics` returns per-stage, per-operator, per-partition metrics after the fact, and only for stages that succeeded, so a cancelled job loses the metrics of its unfinished stages.
   - **Consequence for us.** Neither our throughput probe nor our measured write can be built on Ballista's client API. Both have to live where the sink runs.
5. **What our seam must keep**: see [the closing list](#what-our-seam-must-keep).

## 1. Ballista's Python client

### How it installs its planner and codecs

**The entry point is a factory over a stock context** (read).

- `BallistaSessionContext(address, config, runtime, cluster_config)` builds a `SessionConfig` from Ballista's defaults (`bal python/python/ballista/extension.py:281-290`). Some DataFusion options are forced to Ballista's values whatever the user sets, for example `prefer_hash_join`, `enable_round_robin_repartition` and `enable_dynamic_filter_pushdown` (`:301-311`, `:347-356`).
- It attaches the `ballista.*` settings as an FFI config extension, `BallistaConfigExtension` (`extension.py:360`; `bal python/src/config.rs:62-80`).
- It then calls `with_ballista_query_planner(ctx, address, cluster_config)` (`extension.py:285-289`).

**The planner goes in through `with_extensions`** (read).

- `with_ballista_query_planner` applies `datafusion.*` overrides to the local session by running SQL `SET`, and keeps `ballista.*` overrides for the planner (`bal python/src/query_planner.rs:399-420`, `:445-465`). It then calls `session_ctx.with_extensions(BallistaExtension)` (`:422-428`).
- `BallistaExtension` implements the two-phase protocol. Phase one, `__datafusion_session_components__`, contributes a logical and a physical codec (`:67-75`, `:361-385`). Phase two, `__datafusion_session_planner__`, returns an `FFI_QueryPlanner` capsule. It replaces the fallback planner rather than delegating to it (`:77-94`, `:387-397`).
- The capsule wraps `LazyBallistaQueryPlanner`, which parses the scheduler URL lazily and builds `ballista_core::planner::BallistaQueryPlanner` per query (`:250-275`, `:329-346`).
- The cdylib keeps its own process-wide Tokio runtime for the FFI codecs (`:477-483`).

**Ballista's logical codec ignores the session's own codecs** (read).

- `PyBallistaLogicalExtensionCodec::new` imports the session's logical codec and then drops it (`_user_logical_codec`, `query_planner.rs:114-115`). It installs `codec::PyBallistaLogicalExtensionCodec::default()`, a pass-through to `BallistaLogicalExtensionCodec` (`:116-117`; `bal python/src/codec.rs:56-115`).
- That codec handles Ballista's cache node and the five stock file formats, and passes everything else to `DefaultLogicalExtensionCodec` (`bal ballista/core/src/serde/mod.rs:193-356`). Its doc comment names Python UDFs and Python table providers as future work (`codec.rs:24-28`, `:56-60`).
- *Inferred:* a table provider from another cdylib, such as our schema-only provider, reaches Ballista as a `ForeignTableProvider`. The wheel's codec chain decodes our payload by calling our codec, and the provider handle that comes back belongs to another library, so it isn't unwrapped (`ffi src/proto/logical_extension_codec.rs:147-172`, library marker per #317). Ballista's codec then fails to encode it with datafusion-proto's "Error serializing custom table" (`proto src/logical_plan/mod.rs:1514-1535`). So our scans cannot travel through Ballista's Python client as it stands.

### What it sends to the scheduler

**An optimized logical plan, once more as protobuf** (read).

- `SessionState::create_physical_plan` runs the analyzer and the logical optimizer before calling the query planner (`df src/execution/session_state.rs:777-785`). The plan reaching Ballista's planner is already optimized by the wheel.
- The FFI wrapper on Ballista's side decodes the bytes the wheel encoded. It calls `BallistaQueryPlanner` and re-encodes the resulting physical plan with the physical codec (`ffi src/query_planner.rs:135-176`).
- `BallistaQueryPlanner` plans `information_schema`-only queries and `DESCRIBE` locally and an empty relation as `EmptyExec` (`bal ballista/core/src/planner.rs:116-135`). It refuses a query mixing `information_schema` with other tables (`:181-209`). `EXPLAIN ANALYZE` becomes `DistributedExplainAnalyzeExec` over a `DistributedQueryExec` (`:136-156`). Everything else becomes a single `DistributedQueryExec` leaf holding the logical plan (`:157-166`).
- The leaf crosses back to the wheel through Ballista's physical codec as a `BallistaPhysicalPlanNode`. That node carries the logical plan bytes, the `ballista.*` settings, the scheduler URL and the session id (`codec.rs:180-217`, decode `:144-178`).

**What `DistributedQueryExec::execute` sends** (read, `bal ballista/core/src/execution_plans/distributed_query.rs`).

- The plan is `T::try_from_logical_plan(&self.plan, codec)` encoded to bytes (`:294-304`). `T` is datafusion-proto's `LogicalPlanNode` (`query_planner.rs:339`).
- The settings are every entry of the *task context's* session config, as `KeyValuePair { key, value }` (`:306-317`). That includes `datafusion.*` and any registered extension's keys.
- The request is `ExecuteQueryParams { query: LogicalPlan(bytes), settings, session_id, operation_id }` (`:323-328`). The proto also offers `substrait_plan` and `physical_plan` (`bal ballista/core/proto/ballista.proto:1128-1150`). A physical plan is "serialized with the scheduler's physical extension codec" and skips adaptive planning (`ballista.proto:1133-1137`; `bal ballista/scheduler/src/state/task_manager.rs:315-333`).
- The client attaches its version as a gRPC header (`:512-520`).
- There are two modes. Pull: submit, then poll `GetJobStatus` every 50 ms (`:556-733`, sleep at `:646`). Push: `ExecuteQueryPush` streams status changes (`:738-915`). Both wait for `Successful` before reading any rows.
- The upgrade notes say "the planner sends the physical plan to the scheduler" (`56.0.0.md:117-119`). The code sends the logical plan, and the physical plan is only the `DistributedQueryExec` leaf handed back to the wheel.

### How results come back

**Flight, from Arrow IPC files on the executors** (read).

- On `Successful`, the job status lists `partition_location`s: executor host and port plus partition id (`ballista.proto:1226-1231`, `PartitionLocation` at `:603-616`). The client opens one Flight stream per location and concatenates them (`distributed_query.rs:713-729`, `fetch_partition` `:961-1012`).
- Depending on the scheduler's config, the client fetches from the executor directly, from a Flight proxy embedded in the scheduler, or from an advertised external endpoint (`:917-960`; `bal ballista/scheduler/src/scheduler_server/grpc.rs:909-924`).
- The executor's Flight service reads the stage's output from shuffle files on local disk, written as Arrow IPC or as sort-shuffle files with an index. It streams them as Flight data (`bal ballista/executor/src/flight_service.rs:103-210`).
- The client's `DistributedQueryExec.metrics()` has only its own counters: `output_rows`, `transferred_bytes`, `job_execution_time_ms`, `job_scheduling_in_ms` and `total_query_time_ms` (`distributed_query.rs:86-93`, `:330-332`, `:697-707`).
- A Ballista design issue states it directly: under today's blocking shuffle, time to first row comes "after the whole job completes" (#2652, comparison table).

**Writes run on the executors** (read). `write_parquet` and `INSERT` are `COPY`/`Dml` nodes in the plan, so the sink runs where the last stage runs. The `COPY` plan's file format is what needs the FFI workaround (see [section 3](#3-version-coupling)). Two known gaps: `COPY ... OPTIONS` are dropped by datafusion-proto, and the CSV writer quotes every field (strict xfails, `bal python/python/tests/test_context.py:470-476`, `:627-630`).

### What the scheduler does with it

- **It builds a new session per query** (read). `create_context` builds a session from the scheduler's config producer plus the submitted settings, then calls `create_or_update_session` (`grpc.rs:834-849`). The in-memory backend builds a fresh `SessionContext` every time (`bal ballista/scheduler/src/cluster/memory.rs:454-467`; `bal ballista/scheduler/src/state/session_manager.rs:70-89`). A doc comment says so: "The scheduler builds a new session, with its own runtime, for every query" (`session_manager.rs:95`). The session id is a label for caches and events, not a store of tables.
- **It decodes the plan with its own codec** (read). It uses `self.state.codec.logical_extension_codec()` (`grpc.rs:851-865`), which is set by `SchedulerConfig`'s `override_logical_codec` (`bal docs/source/user-guide/extending-components.md:24-36`).
- **It optimizes again, then splits into stages** (read). `ctx.state().create_physical_plan(logical_plan)` re-runs the analyzer and optimizer (`task_manager.rs:294-295`). With adaptive planning on (the default), `AdaptiveExecutionGraph` plans from the logical plan instead (`:277-289`).
  - The distributed planner cuts a stage at every `CoalescePartitionsExec`, every `SortPreservingMergeExec` and every hash `RepartitionExec`. Each cut writes the child stage's partitions to shuffle files. Non-hash repartitions are removed (`bal ballista/scheduler/src/planner.rs:245-300`).

## 2. Session state across the wire

| State | How it reaches the cluster | What breaks |
| --- | --- | --- |
| DataFusion config | Every entry, as string pairs, with each job (`distributed_query.rs:306-317`). Applied with `options_mut().set` (`bal ballista/core/src/extension.rs:655-680`). | Unknown keys are dropped with a `debug!` log, not an error (`extension.rs:663-671`). Some options are forced to Ballista's values (`extension.py:301-311`). Config set locally and config sent split across `config` and `cluster_config` (#2618, PR #2637 open). |
| Ballista config | As `ballista.*` entries in the same pairs. The Python codec also carries them inside `DistributedQueryExecNode.settings` (`codec.rs:193-201`). | When the task context lacks the extension, "as seen with Ballista Python", the plan's own copy is used (`distributed_query.rs:336-344`). |
| Extension config | As pairs, if the scheduler's and executors' config producers register the extension. The stock binaries register only `S3Options` (`bal ballista/scheduler/src/bin/main.rs:90-92`; `bal ballista/executor/src/bin/main.rs:58-59`). | Our own `hail.*` keys would be dropped silently by a stock cluster (*inferred* from the row above). |
| Registered tables | Inside the plan, as the `TableScan`'s provider, encoded by the codec. datafusion-proto encodes `ListingTable`, `ViewTable`, `CteWorkTable` and `EmptyTable` natively, and anything else through `try_encode_table_provider` (`proto src/logical_plan/mod.rs:1362-1535`). There is no catalog on the cluster. | In-memory tables: `from_pydict`, `from_arrow` and `register_record_batches` are strict xfails, "in-memory table providers cannot be serialized" (`test_context.py:47-51`, `:404-421`). The quickstart still shows `create_dataframe` working (`bal docs/source/user-guide/python/quickstart.md:149-174`); the docs are behind. `information_schema` "cannot be serialized across the FFI planner boundary" (`test_context.py:650-654`). |
| File listing and schema | The client infers the schema itself, so it needs its own object store registered (`bal docs/source/user-guide/python/cloud-storage.md`, "Registering an S3 Object Store" and its note under SQL configuration). The scheduler lists files and reads every footer again for statistics, cached across sessions (`session_manager.rs:91-121`). | Compressed CSV and JSON: `file_compression_type` is not applied on executors (`test_context.py:42-46`). |
| Scalar, aggregate and window UDFs | By name. Executors resolve them in their `BallistaFunctionRegistry` when decoding a task (`bal ballista/executor/src/executor_server.rs:890-922`, `:432-443`). The scheduler resolves them through its session builder. | Python UDFs: strict xfail, "no logical extension codec handles them" (`test_context.py:52-55`, `:439-453`); the docs say "not yet implemented" (`quickstart.md:176-180`). PR #2619 (open) inlines them as datafusion-python's cloudpickle payloads and runs them on executors that are Python processes. Rust UDFs need custom scheduler and executor binaries (`extending-components.md:24-36`). |
| Object stores | Never sent. Every process resolves a URL through its own `ObjectStoreRegistry`. The stock `CustomObjectStoreRegistry` handles `file`, `http`, `https` and `s3`, and errors on anything else (`bal ballista/core/src/object_store.rs:145-185`). | No `gs://` in the stock binaries: no GCS code appears anywhere in `bal ballista/` (grep). Adding a store means a custom runtime producer and custom binaries (`extending-components.md:37-40`). |
| Credentials | Either ambient (`AmazonS3Builder::from_env()`, one identity per process, `object_store.rs:202`) or `SET s3.access_key_id/secret_access_key/session_token`, which travel in the session config to scheduler and executors (`cloud-storage.md`, "Configuring S3 via SQL"). | Secrets ride in plain config and task properties; they were logged until #2535. Executors keep a session's first `s3.*` values after a `SET` changes them, so a refreshed token never arrives (#2533, open). No pluggable credential provider exists (#2532, open). |
| Runtime and threads | Executors run tasks on a dedicated "task_runner" Tokio runtime, separate from the runtime serving gRPC (`executor_server.rs:843-875`). They build or reuse a `RuntimeEnv` per session (`:423-427`). | There is no IO runtime: object store requests run on whichever runtime polls the scan (*inferred*: no `SpawnedReqwestConnector` or IO-runtime handle anywhere in `bal`, by grep). Our CPU/IO split (ADR 0006) would need a custom runtime producer. |

## 3. Version coupling

**Client and scheduler: same Ballista major, which tracks the DataFusion major** (read).

- `version.rs` says it: "Each Ballista major version moves to a new DataFusion major version, and DataFusion's plan encoding can change between majors in ways that still decode but mean something different" (`bal ballista/core/src/version.rs:18-24`).
- The client sends `ballista-version` on job submission, and the scheduler rejects a different major with `failed_precondition` (`grpc.rs:87-95`, `version.rs:61-84`). The scheduler returns its own version in the response. A client that gets none back is talking to a scheduler from before 55.0.0 that has already queued the job, so it cancels the job and errors (`distributed_query.rs:603-623`, `:784-803`). A different minor or patch version only logs a warning (`:525-537`).
- The rule binds the Python package too: "the `ballista` package on PyPI must have the same major version as the cluster" (`bal docs/source/upgrading/55.0.0.md:158-191`).
- The plan bytes themselves carry no version. That matches #317's finding for datafusion-python's bytes.

**Scheduler and executors: strict protocol equality** (read). `BALLISTA_PROTOCOL_VERSION` is 3 (`bal ballista/core/src/lib.rs:47`). Executors must match it exactly at registration; it "can change in any release", because "schedulers and executors are upgraded together" (`version.rs:39-41`; `55.0.0.md:120-153`).

**The Python crate and the wheel** (read).

- `pyballista` requires `datafusion==55`, meaning any 55.x wheel (`bal python/pyproject.toml:43`). The PyPI 55.0.0 wheel is built on DataFusion 55.2 (#317).
- The Rust crate pins `datafusion`, `datafusion-ffi` and `datafusion-proto` to `=55.2.0` (`bal python/Cargo.toml:46-59`). The cluster workspace asks for `datafusion = "55"` and locks 55.2.0 (`bal Cargo.toml` workspace dependencies; `Cargo.lock`).

**The `=55.2.0` FFI workaround** (read, `bal python/src/ffi_planner_workaround.rs`).

- **The problem.** datafusion-python replaces the codecs of an installed `FFI_QueryPlanner` with its own, so Ballista decodes every logical plan through a `ForeignLogicalExtensionCodec`. That codec's `try_decode_file_format` is a hard-coded `not_impl_err!`, which breaks any plan with a `FileFormatFactory`, that is `COPY`, as in `DataFrame.write_*` (`:18-24`; `ffi src/proto/logical_extension_codec.rs:430-444`).
- **The fix.** Ballista overwrites the `create_physical_plan` and `clone` entries of its own `FFI_QueryPlanner`. The replacement wraps the session codec in a `FileFormatFallbackCodec`, which falls back to `DefaultLogicalExtensionCodec` for file formats (`:121-183`, `:185-246`). The terminal default codec wrote those formats without a `DFPYCHN` envelope, so they decode locally (`:26-28`).
- **Why the exact pin.** The fix mirrors the private layouts of `FFI_QueryPlanner` and `FFI_SessionRef`, which is `pub(crate)`. It checks only size and alignment at compile time (`:64-104`). In its own words: "It is only valid for the pinned `datafusion-ffi = "=55.2.0"`" (`:35-37`; `Cargo.toml:46-55`).
- **Status.** A TODO says to remove it once datafusion-ffi delegates file formats (`:39-40`), and #2618 lists it as a known limitation. Searching apache/datafusion issues and PRs for the FFI file-format gap found nothing that tracks it.
- **Relevance to us.** This is the query-planner path only. On the `to_bytes` path the terminal codec encodes `COPY` formats (#317), and our Action is an argument anyway (#313).

## 4. Long-running and early-stopped work

**Cancel is a scheduler operation, and the client never sends it on its own** (read).

- **Entry points.** `CancelJob(job_id)` over gRPC (`ballista.proto:1374`; `grpc.rs:785-799`) and `PATCH /api/job/{job_id}` over REST (`bal ballista/scheduler/src/api/handlers.rs:384-398`). Both post a `JobCancel` event.
- **Scheduler side.** The event aborts the job with the reason "Cancelled" and cleans it up as a failed job (`bal ballista/scheduler/src/scheduler_server/query_stage_scheduler.rs:468-476`). `abort_job` marks the graph failed, collects running tasks and frees pending ones. It sends the cancellation before persisting the terminal state (`task_manager.rs:643-700`). The executor manager sends `CancelTasks` to each executor from a spawned task, so the cancel call returns before executors act (`bal ballista/scheduler/src/state/executor_manager.rs:152-190`).
- **Executor side.** Every task runs under `futures::future::abortable`. `cancel_task` calls `AbortHandle::abort()`, the task ends with `BallistaError::Cancelled`, and its metrics are still recorded locally (`bal ballista/executor/src/executor.rs:259-318`). Abort takes effect at the task's next `.await`.
- **Client side.** `DistributedQueryExec` calls `cancel_job` only when the version check fails (`distributed_query.rs:539-550`, `:618-623`, `:788-803`). *Inferred:* dropping the result stream, or interrupting `collect()` in Python, leaves the job running. Nothing implements `Drop`. While the client is still polling, nothing has been fetched yet; once fetching starts, the job has already finished.
- **No early termination.** Stopping a stage once a `LIMIT` is met is an open issue (#628).

**No progress streams back during a job** (read).

- `RunningJob` holds `queued_at`, `started_at` and the scheduler id, under `// TODO: add progress report` (`ballista.proto:1237-1242`). Push mode streams state changes only: queued, running, failed, successful (`distributed_query.rs:805-913`).
- The REST job view reports `percent_complete` as stages completed over stages (`bal ballista/scheduler/src/api/dto_build.rs:289-296`). The Python `collect_with_progress` runs `collect()` on a thread and reports elapsed time; it reads no job state (`bal python/python/ballista/jupyter.py:566-626`).
- Executors attach operator metrics to a task's terminal `TaskStatus` (`ballista.proto:1003-1021`), collected after the task finishes (`executor_server.rs:461-468`). No running-task status with metrics is sent; no code builds one.

**Metrics after the fact, successful stages only** (read).

- The scheduler merges each task's operator metrics into its stage, keyed by global partition id (`bal ballista/scheduler/src/state/execution_stage.rs:934-981`).
- `GetJobMetrics` walks each successful stage's plan and returns `(depth, operator_type, operator_desc, metrics)` per operator (`grpc.rs:627-753`). Stages that are not `Successful` are skipped (`:653-657`), so a cancelled or failed job reports only the stages that finished.
- `DistributedQueryExec::explain_executed_plan` renders this after a run (`distributed_query.rs:171-208`), and `EXPLAIN ANALYZE` uses it through `DistributedExplainAnalyzeExec`.

**What this means for our probe and measured write** (inferred).

- **The probe** (ADR 0017) samples the rows its sink has received while the plan runs and stops once the estimate settles.
  - On Ballista the sink is in the last stage on executors, and the client sees nothing until the job succeeds. So the sampling and the stopping rule have to run beside the sink, in backend code on the executor.
  - Stopping means cancelling the job. That fails it and drops the metrics of every stage that hadn't finished, so the probe would have to record its samples and any run metrics before it cancels.
  - "Stop at the first finished partition" needs an executor-side signal too, since the scheduler learns of a finished task only from its terminal status.
- **The measured write** (ADR 0016) records per-operator, per-partition metrics. `GetJobMetrics` has the same shape after the run, aggregated per stage. Ballista adds stage boundaries of its own: shuffle writers and readers at every merge (section 1). The plan measured would then not be the plan a plain in-process write runs.

## What our seam must keep

These are the properties that let a remote backend replace the in-process one behind the same call. Each one is a place Ballista's seam already works, or a place it breaks.

1. **Plan bytes are the only plan representation that crosses.** The in-process backend decodes the same `to_bytes` output a remote one would. It must not take a shortcut through a shared `LogicalPlan`, `Arc` or PyCapsule. Ballista's own seam is `ExecuteQueryParams.logical_plan` bytes.
2. **Every scan is a description any process can reopen.** That means dataset URL, format, declared ordering and input-table selection. It never means in-memory batches, a registry key, a client-side handle or `information_schema`. This is exactly where Ballista breaks (in-memory providers, `information_schema`, and the foreign provider its codec can't re-encode). It is also why opening a dataset must stay a backend call: Ballista's client needs its own object store just to infer schemas.
3. **The backend owns its session; the call carries everything else.** Ballista rebuilds a session per query from flat string pairs and drops unknown keys silently. Our call should carry the plan bytes, the Action and an explicit settings map in our own namespace. The backend should reject keys it doesn't know, and nothing may depend on state registered earlier on the frontend's context.
4. **Object stores and credentials are resolved by the backend from URLs.** The frontend never builds a store the backend needs. If credentials ever travel, they travel as explicit inputs with a refresh story, not as config snapshots; Ballista's `s3.*` keys go stale (#2533) and were logged (#2535). Ballista's stock binaries have no GCS, so a remote backend means our own binaries in any case.
5. **Functions cross by name and resolve in the backend's registry.** This covers `assert_ordered` and any Rust UDF. Keep Python-UDF inlining off, as #317 found and #316 allows: Ballista executors resolve UDFs by name, and #2619's inlined cloudpickle payloads need executors running Python.
6. **The Action is a plain serializable value, separate from the plan.** Its parameters are data: output URL and format, metrics directory, run id, probe settings. There are no callbacks or Python objects in it. Ballista has no Action field (its sink is a node in the plan), so a remote form needs its own request field, or a settings entry its scheduler understands. Either way the value has to encode to bytes.
7. **Outcomes come back as data after the run, gathered where the plan ran.** Collected rows come back as an Arrow stream, which is Ballista's Flight-over-IPC shape. The run record, run metrics and probe samples are produced by backend code next to the sink and returned or written as tables. The frontend must never get a live plan to poll or walk, because a remote client sees only the final stream.
8. **The run has an id, and stopping and cancelling are backend operations.** The probe's stopping rule runs inside the backend; the frontend can only ask to cancel a run, by id. The in-process API needs an explicit cancel, since Ballista shows that dropping a stream cancels nothing remotely. The backend must record what a stopped or failed run measured *before* it tears the run down, since Ballista loses unfinished stages' metrics on cancel.
9. **The call checks versions.** It carries the frontend's DataFusion major (and the wheel's `DFPYCHN` wire version from #317), and the backend refuses a mismatch before decoding. Plan bytes carry no version, which is why Ballista added its header check.
