# PROTOTYPE: an ordering assertion through the backend

Throwaway. It answers [Can an ordering assertion hold a key order through the backend and fail
when it can't?](https://github.com/hail-is/datafusion-sandbox/issues/322) and nothing it holds is
the MVP. It is the [#321 prototype](../dfpy-grouped-merge-prototype/) plus an assertion.

- `src/assertion.rs` holds the assertion:
  - `assert_ordered`, the marker UDF, exported to the wheel as `AssertOrdered`. The frontend
    lowers an assertion to `Filter(assert_ordered(key, 'ASC NULLS LAST', ...))`.
  - `lift_assertions`, the backend's replacement pass. It runs with the scan reopen, before the
    logical optimizer, and turns each marker filter into the backend-only `OrderingAssertion` node.
    A marker anywhere else is an error.
  - `AssertionPlanner`, the extension planner on the backend's query planner, which plans that
    node to `AssertExec` (displayed as `OrderingAssertionExec`). Like `DataSinkExec`, it requires
    a single partition in the asserted order.
  - The check is `AssertExec::check_invariants(Executable)`. `SanityCheckPlan` calls it on the
    final plan. It walks down `maintains_input_order` edges, through every `UnionExec` and
    `SortPreservingMergeExec` child, and fails on `SortExec`, `PartialSortExec` or
    `PartitionedTopKExec`. It also fails on any non-leaf node that keeps no input's order.
  - The replacement pass also wraps every frontend `Sort` in an established order. That node plans
    to `EstablishedOrderExec`, which requires the same as an assertion but accepts the sort beneath
    it. An assertion's check stops at it: a sort the frontend wrote is expected, while a sort the
    backend inserted fails.
  - `verify_order` additionally checks the row order at run time.
- `src/lib.rs` is #321's cdylib. `Backend.execute` gains two Action fields:
  - `sink_ordering`: `require` is #321's sink requirement. `assert` places an assertion of the
    Action's ordering at the root and gives the sink no ordering.
  - `verify_order`.
- `run.py` has three sections, and `output.txt` holds a run of all three:
  1. **Shape**: grouped-merge against the Rust plan builder, with an assertion per group in place
     of a sort per group, the sink's ordering as an assertion, and #321's final logical sort.
  2. **Cases**: where an assertion must hold, and where it must fail.
  3. **A lie**: apache/datafusion#25052.
- `upstream-repro/` is a standalone DataFusion 55.1 reproduction of the sort-pushdown guard gap
  that the "two contradicting assertions" case hit. `upstream-report-draft.md` is the report, to
  be filed by hand.

## Run

```sh
cd notes/dfpy-ordering-assertion-prototype
# mold asserts on arm64 linking this cdylib (create_range_extension_thunks); link with cc.
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=cc cargo build --message-format=json \
  | jq -r 'select(.target.name == "hail_proto") | .filenames[0]'
mkdir -p /tmp/hail-assert-so && cp <that path> /tmp/hail-assert-so/hail_proto.abi3.so
uv run --no-project --with datafusion==55.0.0 --with pyarrow python -I run.py /tmp/hail-assert-so [shape|cases|lie]...
```

Writes go under `/tmp/PROTOTYPE-wipe-me-*`.
