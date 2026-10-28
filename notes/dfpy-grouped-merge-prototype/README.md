# PROTOTYPE: a Python-built grouped-merge plan through the backend

Throwaway. It answers [Can a Python-built grouped-merge plan reach the backend with the Rust plan
builder's plan shape?](https://github.com/hail-is/datafusion-sandbox/issues/321) and nothing it
holds is the MVP.

- `src/lib.rs` is one pyo3 cdylib, `hail_proto`, on this crate's DataFusion 55.1. It has the
  frontend pieces the stock `datafusion` 55.0.0 wheel (DataFusion 55.2) loads, `SeamCodec` and a
  schema-only `ScanTable` per input table, and an in-process `Backend` over this crate:
  `open_dataset`, `execute(plan_bytes, **action)`, and the oracle `rust_grouped_merge`.
- `run.py` opens a disk fixture through the backend and lowers grouped-merge with the wheel's
  `DataFrame` API: a union per input group, a sort per group, then an outer union. It sends
  `to_bytes(ctx)` and an Action to the backend, then compares the physical plan, rows and output
  file with the Rust plan builder's. The cases are both formats, 1, 2, 3 and 8 groups, three
  settings maps, and `explain` and `write`. Negative controls follow.

## Run

```sh
cd notes/dfpy-grouped-merge-prototype
# mold asserts on arm64 linking this cdylib (create_range_extension_thunks); link with cc.
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=cc cargo build --message-format=json \
  | jq -r 'select(.target.name == "hail_proto") | .filenames[0]'
mkdir -p /tmp/hail-proto-so && cp <that path> /tmp/hail-proto-so/hail_proto.abi3.so
uv run --no-project --with datafusion==55.0.0 --with pyarrow python -I run.py /tmp/hail-proto-so
```

Writes go under `/tmp/PROTOTYPE-wipe-me-*`.
