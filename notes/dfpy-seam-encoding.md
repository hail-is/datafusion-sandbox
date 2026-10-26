# What datafusion-python 55 puts in `LogicalPlan.to_bytes(ctx)`

This note answers [#317](https://github.com/hail-is/datafusion-sandbox/issues/317): does the stock PyPI `datafusion` 55.0.0 wheel encode everything the frontend-to-backend seam of [#313](https://github.com/hail-is/datafusion-sandbox/issues/313) needs, when a frontend plan goes through `df.logical_plan().to_bytes(ctx)`? Checked on 2026-10-26.

Each claim is marked:

- **read**: read in source, cited as `path:line` from the crate root;
- **ran**: seen in the experiment under `notes/dfpy-seam-encoding-experiment/` (see [The experiment](#the-experiment));
- **inferred**: follows from what was read or run, but nothing checked it directly.

Sources:

- `dfpy`: datafusion-python 55.0.0 (Rust in `crates/`, Python in `python/`). Its `Cargo.lock` resolves datafusion, datafusion-proto and datafusion-ffi to **55.2.0**.
- `ffi`: datafusion-ffi 55.2.0.
- `proto`: datafusion-proto 55.2.0. Its `src/` is byte-identical to 55.1.0, and so is datafusion-proto-common's (`diff -rq`, **ran**), so the 55.1 crate this repo uses parses the same wire format.

## Answers

1. **Codecs take part, through the context you pass.** `with_logical_extension_codec` and `with_extensions` both append to the `PythonLogicalCodec` chain of the handle they return, and `to_bytes(ctx)` encodes with that handle's chain. Order: Python-UDF inlining first (UDFs only), then installed codecs in install order, then `DefaultLogicalExtensionCodec`. The first codec to write bytes wins. **dfpy wraps every payload an installed codec writes in a `DFPYCHN` envelope that names the codec's id.**
2. **By name.** An FFI-imported UDF reaches our codec as our own type. If our codec writes nothing, `fun_definition` stays unset, and a backend on its own DataFusion copy resolves `assert_ordered` from its own registry. The ran experiment did exactly that.
3. **Yes.** dfpy hands a provider registered from our cdylib back to our codec as the original Rust type, so a downcast works. The backend sees our bytes inside the `DFPYCHN` envelope, so its codec has to strip that header (about 20 lines). A 55.1 backend in the experiment decoded the scan description and optimized the plan.
4. **Python UDFs go inline as `DFPYUDF` + 3 header bytes + a cloudpickle blob.** The blob is a pickled tuple `(name, callable, input-schema IPC, return-field IPC, volatility)`. Reading it takes CPython of the same minor version, cloudpickle, and the callable's imports. A Rust decoder can only recognize the prefix and refuse. With inlining off, Python UDFs travel by name.
5. **Nothing blocks a same-major backend, given four conditions.** The backend needs a codec that strips `DFPYCHN` (and refuses or ignores `DFPYUDF`). Every UDF name has to be registered on the backend. Every table has to come from our provider or a stock listing/view/empty table. The plan can't contain subquery expressions, extension nodes or DDL/statements. There is no proto extension of dfpy's own and no version stamp in the bytes beyond the envelope's and `DFPYUDF`'s version bytes.

## 1. Which codec chain `to_bytes(ctx)` runs

**The codec comes from the context argument** (read).

- `PyLogicalPlan::to_bytes` uses `ctx.logical_codec()` when given a context, otherwise a fresh `PythonLogicalCodec::default()`. It then calls `LogicalPlanNode::try_from_logical_plan` (`dfpy crates/core/src/sql/logical.rs:201-219`; codec choice at :208).
- `from_bytes` decodes with the same session codec (`:222-238`, :234). `Expr.to_bytes` does the same (`crates/core/src/expr.rs:723-757`).
- The Python wrapper passes `ctx.ctx` through (`python/datafusion/plan.py:110-124`).

**Every session handle owns a `PythonLogicalCodec`** (read).

- The handle's field is `logical_codec: Arc<PythonLogicalCodec>` (`crates/core/src/context.rs:417`). A new session starts with `PythonLogicalCodec::default()` (:449), whose terminal codec is `DefaultLogicalExtensionCodec` (`crates/core/src/codec.rs:679-683`).
- The codec chain lives on the handle, not on `SessionState`. Installing a codec returns a new handle on the same session, and you must pass *that* handle to `to_bytes`.

**`with_logical_extension_codec(codec, codec_id=None)`** (read). It imports the capsule from `__datafusion_logical_extension_codec__(session)`; the import checks the FFI major version (`crates/util/src/lib.rs:368-383`). It then appends the imported codec under an id (`crates/core/src/context.rs:1522-1551`). The id comes from `codec_id=`, else the object's `__datafusion_codec_id__`, else its class's `module.QualName`. A bare capsule gets `anon:<uuid>` (`derive_codec_id`, :2007-2034). A duplicate id is rejected (:1950-1967).

**`with_extensions(*bundles)`** (read).

- It calls `__datafusion_session_components__(ctx)` on each bundle and collects `SessionExtensionComponents.logical_extension_codecs` in argument order (`python/datafusion/context.py:1996-2016`).
- It hands them to `_install_extension_codecs` (`:2020`), which appends each one the same way (`crates/core/src/context.rs:1674-1720`).
- A bare capsule is refused here; it must be an object with the getter (`resolve_bundle_codec_id`, :1988-2005).

So both routes feed the same chain. **Ran:** both produced identical 257-byte plans, and our codec logged the same calls.

**Encoding order inside `PythonLogicalCodec`** (read).

- UDF, UDAF and UDWF calls first try inline Python encoding when inlining is on. That handles only dfpy's own `PythonFunction*UDF` types (`codec.rs:770-781`, `:1163-1173`).
- Everything else goes through `chain_encode` (`codec.rs:524-557`). It offers the object to each installed codec in install order, each writing into a scratch buffer.
  - The first codec that returns `Ok` *with bytes* wins, and its bytes are wrapped in the envelope.
  - `Ok` with no bytes means "no opinion", and the walk continues. An `Err` means "not mine".
  - The terminal `DefaultLogicalExtensionCodec` goes last. It writes unframed.
  - If nothing wrote bytes but anything returned `Ok`, the result is `Ok` with nothing written. That is the by-name signal for UDFs.
- The same walk applies to table providers (`:727-740`), file formats (`:756-768`) and extension nodes (`:701-709`).

**The envelope** (read, ran).

- Layout: `DFPYCHN | version u8 (=1) | id_len u32 LE | id | blob` (`codec.rs:236-250`, `write_chained_payload` :320-326).
- Decoding reads the id and hands the blob to exactly that codec. A missing id fails with "encoded by extension codec '…', which is not installed on this session" (`chain_decode`, :404-443).
- An empty buffer is offered to every codec in turn (`chain_resolve_by_name`, :460-480).
- The upgrade guide documents this as a 55.0 wire change: "Serialized plans change shape once an extension codec is installed" (`docs/source/user-guide/upgrade-guides.md:142-184`).
- Ran: `DFPYCHN` 1 9 `hail.seam` `HAILSCANgs://bucket/table.vortex`. The wheel's own `from_bytes` on a session without our codec failed with "a table provider was encoded by extension codec 'hail.seam', which is not installed on this session".

**What an installed FFI codec can claim** (read). dfpy sees our codec as `ForeignLogicalExtensionCodec` (`ffi src/proto/logical_extension_codec.rs:359-367`). That wrapper:

- forwards table providers, UDFs, UDAFs and UDWFs to us (`:411-500`);
- returns `not_impl_err!` for extension nodes (`:375-387`) and file formats (`:430-444`).

So for `CopyTo` the file format falls through to the terminal codec, which encodes stock formats (`proto src/logical_plan/mod.rs:262-297` for decode; encode follows). An `Extension` node fails to encode in every chain: the default codec also returns `not_impl_err!` (`proto src/logical_plan/mod.rs:239-241`).

## 2. How a call to an FFI-imported scalar UDF is encoded

**The proto side** (read).

- Encoding an `Expr::ScalarFunction` calls `codec.try_encode_udf(func, &mut buf)` and ignores its result. It writes `fun_name: func.name()` and sets `fun_definition` only if `buf` is non-empty (`proto src/logical_plan/to_proto.rs:261-271`).
- Decoding with `fun_definition` present calls `codec.try_decode_udf(name, buf)`. With it absent, the decoder tries `ctx.udf(name)` (the decoder's `FunctionRegistry`) first and the codec with `&[]` second (`from_proto.rs:487-502`).
- `None` is the only absent case; an empty payload is never written as `Some(empty)`.

**The imported UDF on the wheel's side** (read).

- `udf(obj)` with `__datafusion_scalar_udf__` builds `ScalarUDF::new_from_shared_impl((&ffi_udf).into())` (`dfpy crates/core/src/udf.rs:213-222`). Our library's marker differs from the wheel's, so the result is a `ForeignScalarUDF` (`ffi src/udf/mod.rs:407-415`).
- Its `name()` is the name our side put in `FFI_ScalarUDF.name` (`:351-363`, `:417-420`), which is our impl's `name()` (`:296-302`). Aliases cross too (:303).

**The encode path for that call** (read, ran).

- `PythonLogicalCodec::try_encode_udf` first checks inline Python encoding. It doesn't apply (not a `PythonFunctionScalarUDF`), so the call goes to the chain.
- In the chain, `ForeignLogicalExtensionCodec::try_encode_udf` converts the UDF with `FFI_ScalarUDF::from(Arc<ScalarUDF>)`. That re-exports the original handle of a `ForeignScalarUDF` (`ffi src/udf/mod.rs:296-300`) and calls our codec (`ffi src/proto/logical_extension_codec.rs:455-462`).
- On our side, `(&node).into()` sees our own library marker and unwraps to our original `Arc<dyn ScalarUDFImpl>` (`:206-218`, `udf/mod.rs:407-410`). Our codec can downcast it.
- If our codec writes nothing, the terminal `DefaultLogicalExtensionCodec` also writes nothing: it inherits the trait's default `try_encode_udf` (`proto src/logical_plan/mod.rs:185-187`, not overridden in `:229-340`), and `chain_encode` returns `Ok` empty. The call goes on the wire **by name only, with no envelope**.
- dfpy pins that behaviour with a test: an empty result is deliberately left unframed so the registry lookup still runs (`codec.rs:514-520`; `examples/datafusion-ffi-example/python/tests/_test_logical_extension_codec.py:481-518`).
- Ran: our codec logged `encode_udf(assert_ordered): own AssertOrderedUdf=true, writing nothing`. Built-ins pass through our codec too: `encode_udf(abs): own AssertOrderedUdf=false`. That costs one FFI hop per function call in the plan and nothing more.

**The backend side** (ran). The backend is a separate DataFusion 55.1 binary with its own `assert_ordered` registered. It decoded `Filter: assert_ordered(t.contig, t.position)` from the registry; its codec's `try_decode_udf` was never called for it. Its optimizer ran over the plan and kept the filter.

So the marker of [#315](https://github.com/hail-is/datafusion-sandbox/issues/315) crosses as planned. If our codec ever *did* write a payload for a UDF, `fun_definition` would carry it inside the envelope (inferred from `chain_encode`). The backend would then have to decode it through its codec, because the registry lookup happens only when `fun_definition` is absent.

## 3. A foreign schema-only table provider

**Registration** (read).

- `ctx.register_table(name, obj)` calls `obj.__datafusion_table_provider__(session)` (`dfpy crates/util/src/lib.rs:236-254`) and checks the FFI major.
- It stores `provider.into()`, a `ForeignTableProvider`, since the library markers differ (`ffi src/table_provider.rs:458-466`). It goes into the catalog as is (`dfpy crates/core/src/table.rs:93-100`, `context.rs:751-757`).

**Encode** (read, ran).

- For a `TableScan`, datafusion-proto first recognizes `ListingTable`, `ViewTable`, `CteWorkTable` and `EmptyTable`. Anything else becomes a `CustomScan` carrying the table name, projection, filters and the Arrow schema, plus `custom_table_data`, which comes from `codec.try_encode_table_provider` (`proto src/logical_plan/mod.rs:1336-1547`; custom branch :1529-1547).
- `ForeignLogicalExtensionCodec::try_encode_table_provider` builds an `FFI_TableProvider` with `new_with_ffi_codec`, which re-exports the original handle of a `ForeignTableProvider` (`ffi src/table_provider.rs:415-425`; `src/proto/logical_extension_codec.rs:411-428`).
- Our side converts it back with `(&node).into()`, which unwraps to our original `Arc<dyn TableProvider>` because the marker is our own (`:174-191`, `table_provider.rs:458-461`).
- Ran: our codec logged `encode_table_provider(t): own ScanDescription path=gs://bucket/table.vortex` and wrote `HAILSCAN<path>`. The provider's `scan` always errors, and nothing called it.

**Decode** (read, ran).

- `CustomScan` decoding calls `codec.try_decode_table_provider(custom_table_data, table_name, schema, ctx)` and rebuilds the scan with the projection and filters (`proto src/logical_plan/mod.rs:705-737`).
- The backend codec gets our bytes inside the envelope. It stripped the envelope, checked the id `hail.seam`, read `HAILSCAN<path>`, and returned its own provider with the schema from the proto. Ran on 55.1: the plan decoded and the backend's optimizer pushed a projection into it.

**Failure cases** (ran).

- Without our codec, `to_bytes(None)` and `to_bytes(plain_ctx)` fail at the terminal codec: "Error serializing custom table … LogicalExtensionCodec is not provided" (`proto mod.rs:253-260`, :1534).
- An in-memory table (`from_pydict`) fails the same way after our codec declines it. So does any provider the backend can't reopen, which is what [#313](https://github.com/hail-is/datafusion-sandbox/issues/313) wants.

**For our codec** (inferred).

- Our provider encode should return `Err` for providers that aren't ours. `Ok` with no bytes would leave `custom_table_data` empty and pass the "not mine" decision to whichever codec decodes an empty buffer.
- Pick a stable `__datafusion_codec_id__`. It goes on the wire, and the backend has to match it.

## 4. Python UDFs

**Encode** (read, ran).

- With inlining on (the default), `try_encode_python_scalar_udf` downcasts to `PythonFunctionScalarUDF` (`dfpy crates/core/src/codec.rs:1163-1173`). It writes the payload straight into `fun_definition`, with no `DFPYCHN` envelope and before any installed codec runs (`:770-773`).
- Ran: our codec was not called for `py_double` with inlining on.

**Framing** (read). `<family 7 bytes> <wire version u8> <py_major u8> <py_minor u8> <cloudpickle blob>` (`codec.rs:129-175`).

- Families are `DFPYUDF` (scalar), `DFPYUDA` (aggregate) and `DFPYUDW` (window) (:146-157). The wire version is 1, and versions `1..=1` are accepted (:160-164).
- The decoder rejects a version outside that range or a Python minor mismatch (`strip_wire_header`, :190-234).
- Ran: header `DFPYUDF [1, 3, 13]`, then pickle protocol 5 (`\x80\x05`). The payload was 1,018 bytes for a one-line lambda.

**What the blob holds** (read).

- Scalar: `cloudpickle.dumps((name, func, input_schema_bytes, return_schema_bytes, volatility))` (`:1193-1221`). The schemas are arrow-rs IPC streams holding only a schema message (`schema_to_ipc_bytes`, :1251-1258). Input fields are synthetic `arg_i`; only the data types count (:1288-1295). Volatility is one of `immutable`/`stable`/`volatile` (:1347-1353).
- Window: `(name, evaluator_factory, input, return, volatility)` (:1452-1474).
- Aggregate: `(name, accumulator_factory, input, return, state_schema, volatility)` (:1537-1566).

**What a foreign decoder needs** (inferred).

- The name, the types and the callable all sit inside the pickle, so even the signature can't be read without unpickling.
- Unpickling means a CPython of the same minor version (enforced by the header), `cloudpickle`, and whatever modules the callable imports. Executing it means arrow arrays crossing into pyarrow, as dfpy's `PythonFunctionScalarUDF` does.
- A Rust-only backend can recognize `DFPY*` and refuse with a clear error, as the experiment's backend did ("'py_double' is an inline Python UDF (wire v1, Python 3.13)"). It can't do more.
- Unpickling is arbitrary code execution; dfpy's own docs say so (`docs/source/user-guide/distributing-work/expressions.md:237-245`).

**With inlining off** (read, ran).

- `ctx.with_python_udf_inlining(enabled=False)` sends Python UDFs through the chain like any other UDF, so they go by name (`codec.rs:654-672`).
- Ran: our codec was called for `py_double` and the plan carried the bare name. The backend then tried its registry, then its codec with an empty buffer, and failed because nothing named `py_double` exists there.
- For [#316](https://github.com/hail-is/datafusion-sandbox/issues/316) this is the simpler hook (inferred): a backend that one day runs Python UDFs can register them by name, or decode `DFPYUDF` itself with an embedded interpreter.

## 5. What could stop a same-major backend in another DataFusion copy

**Codecs the backend must have.**

- A logical codec that understands the `DFPYCHN` envelope for our id. This applies to table providers and to any UDF our codec writes bytes for (read, ran).
- Nothing else. A session with no installed codec writes plain datafusion-proto bytes (`codec.rs:328-333`; dfpy test `test_default_only_session_writes_no_envelope`, `_test_logical_extension_codec.py:521-528`) (read).
- dfpy adds no protobuf messages or fields of its own. Its only additions are the byte formats inside `bytes` fields: the `DFPYCHN` envelope and `DFPY*` UDF payloads (read; `codec.rs` is the only encoder, `LogicalPlanNode` is upstream's).

**Function names the backend must register** (read, inferred).

- Every UDF, UDAF and UDWF without a payload is resolved by name in the backend registry (`from_proto.rs:487-502`).
- That covers our UDFs and every DataFusion built-in, which share names at the same version.
- It also covers anything the frontend pulls from `datafusion-spark`, opt-in through `SessionContext.enable_spark_functions` (`dfpy crates/core/src/context.rs:1098-1109`). A backend without datafusion-spark registered would fail on those names.

**Plans datafusion-proto can't encode**, so `to_bytes` fails before anything crosses (read).

- Expressions: `InSubquery`, `Exists`, `OuterReferenceColumn` and `SetComparison` are "not yet supported" (`proto src/logical_plan/to_proto.rs:465-472`). `ScalarVariable` is refused too (:256-259). `ScalarSubquery` is supported (:458).
  - The frontend sends *unoptimized* plans, so a lowering that uses `IN (subquery)`/`EXISTS` fails. The optimizer would have decorrelated them into joins, but it hasn't run yet.
  - Lowering must emit semi/anti joins directly.
- Plan nodes: DDL other than `CreateExternalTable`/`CreateView`/`CreateCatalog*`/`DropView`, `Statement`, `DescribeTable` (`mod.rs:2102-2135`, `:2212`), and `Repartition` by `DistributeBy` (`:1818`).
- Extension nodes, with every codec in the chain (§1). Stock nodes only, as [#314](https://github.com/hail-is/datafusion-sandbox/issues/314) already decided.

**`not_impl_err!` paths that don't block the seam** (read).

- The FFI codec's file-format and extension methods (§1).
- The default codec's table-provider methods, which our codec covers.
- `COPY`'s file format is encoded by the terminal default codec, and a backend decodes it with its own default codec (`proto mod.rs:262-297`).
  - So the `COPY` failure noted in [#312](https://github.com/hail-is/datafusion-sandbox/issues/312) belongs to the foreign-query-planner path, where the FFI codec does the decoding, not to this one (inferred).
  - The seam carries the Action as an argument anyway.

**Version coupling** (read, ran).

- The bytes carry no DataFusion version.
- The `DFPYCHN` envelope and `DFPY*` payloads each carry their own version byte, so a future dfpy release can change their framing. That release would also have to change the envelope's version byte, which a backend can check (inferred).
- Under the lockstep rule of [#314](https://github.com/hail-is/datafusion-sandbox/issues/314), keep the backend's envelope parser in step with the wheel's `codec.rs`.
- 55.1 and 55.2 proto are identical (see Sources), and a 55.1 backend decoded 55.2 bytes in the experiment.

## The experiment

Under `notes/dfpy-seam-encoding-experiment/`, throwaway, each crate its own workspace:

- `frontend/`: the `seam_probe` pyo3 cdylib on datafusion and datafusion-ffi `=55.2.0`, the wheel's versions. It exports `SchemaOnlyTable(path)`, a provider whose `scan` always errors, and `AssertOrdered()`. It also exports `SeamCodec()`, with id `hail.seam`, which writes `HAILSCAN<path>` for its own provider, encodes UDFs by name, and logs what it was handed.
- `backend/`: a binary on datafusion and datafusion-proto 55.1.0, locked with this repo's `Cargo.lock`. It decodes plan files with a codec that strips `DFPYCHN`, reopens `HAILSCAN`, and refuses `DFPYUDF`. It registers its own `assert_ordered` and runs its optimizer over each decoded plan.
- `run.py`: builds plans with PyPI `datafusion` 55.0.0 and writes `to_bytes(ctx)` output, for both install routes, two controls, a MemTable and Python UDFs.

Run (Python 3.13.15, arm64):

```sh
cargo build --release --manifest-path notes/dfpy-seam-encoding-experiment/frontend/Cargo.toml
cp <target>/release/libseam_probe.so /tmp/seam-so/seam_probe.abi3.so
uv run --no-project --with datafusion==55.0.0 --with pyarrow \
  python -I notes/dfpy-seam-encoding-experiment/run.py /tmp/seam-so /tmp/seam-out
cargo run --release --manifest-path notes/dfpy-seam-encoding-experiment/backend/Cargo.toml -- \
  /tmp/seam-out/extension_codec.bin /tmp/seam-out/with_extensions.bin \
  /tmp/seam-out/python_udf_inline.bin /tmp/seam-out/python_udf_by_name.bin
```

Results are quoted in the sections above. The plan the backend decoded:

```
Projection: t.contig, t.position, abs(t.value) AS abs_value
  Filter: assert_ordered(t.contig, t.position)
    TableScan: t
```
