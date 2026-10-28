"""PROTOTYPE, throwaway, for hail-is/datafusion-sandbox#321.

Builds the reference combiner's grouped-merge plan in Python with the stock PyPI
`datafusion` 55.0.0 wheel, sends `to_bytes(ctx)` and a write Action to the
in-process backend in `hail_proto`, and compares the physical plan it ran with
the one the Rust grouped-merge plan builder gets for the same dataset and Action.

Usage: see README.md; `python -I run.py <dir holding hail_proto.abi3.so>`.
"""

import sys
import tempfile
from functools import reduce
from pathlib import Path

sys.path.insert(0, sys.argv[1])

import datafusion  # noqa: E402
import hail_proto  # noqa: E402
from datafusion import SessionContext, col  # noqa: E402

THREADS = 4
ROW_ORDERING = "locus_then_alleles"
# The Action's settings map. "hostile" is the plan-shape suites' hostile_config(8): any file scan
# may be split across 8 partitions wherever the optimizer is allowed to.
SESSIONS = {
    "default": [],
    "one-partition": [("datafusion.execution.target_partitions", "1")],
    "hostile": [
        ("datafusion.execution.target_partitions", "8"),
        ("datafusion.optimizer.repartition_file_min_size", "0"),
    ],
}


def input_groups(names, groups):
    """A port of combine_refs_grouped_merge::input_groups: contiguous, count-balanced, larger first."""
    groups = min(groups, len(names))
    quotient, remainder = divmod(len(names), groups)
    out, start = [], 0
    for index in range(groups):
        size = quotient + (index < remainder)
        out.append(names[start : start + size])
        start += size
    return out


def union_or_single(frames):
    return reduce(lambda left, right: left.union(right), frames)


def grouped_merge(ctx, tables, ordering, groups):
    """The frontend's lowering: a union per input group, a sort per group, an outer union."""
    frames = {name: ctx.read_table(table) for name, table in tables}
    keys = [col(c).sort(ascending=asc, nulls_first=nf) for c, asc, nf in ordering]
    return union_or_single(
        [union_or_single([frames[n] for n in group]).sort(*keys) for group in input_groups(list(frames), groups)]
    )


def main():
    print("datafusion wheel", datafusion.__version__)
    backend = hail_proto.Backend(THREADS)
    out = Path(tempfile.mkdtemp(prefix="PROTOTYPE-wipe-me-"))
    all_match = True
    for fmt in ["parquet", "vortex"]:
        fixture = hail_proto.make_fixture(fmt)
        tables, ordering = backend.open_dataset(fixture.path, fmt, ROW_ORDERING)
        print(f"\n##### {fmt}: {fixture.path}")
        print("input tables:", [name for name, _ in tables])
        print("ordering:", ordering)
        ctx = SessionContext().with_logical_extension_codec(hail_proto.SeamCodec())
        for session, settings in SESSIONS.items():
            for groups in [1, 2, 3, 8]:
                for kind in ["explain", "write"]:
                    df = grouped_merge(ctx, tables, ordering, groups)
                    plan_bytes = df.logical_plan().to_bytes(ctx)
                    py_out = out / f"py-{fmt}-{session}-{groups}-{kind}.{fmt}"
                    rust_out = out / f"rust-{fmt}-{session}-{groups}-{kind}.{fmt}"
                    action = dict(
                        kind=kind,
                        output_path=str(py_out),
                        output_format=fmt,
                        ordering=ordering,
                        layout="single_file",
                        settings=settings,
                    )
                    py_plan, py_rows, decoded = backend.execute(plan_bytes, **action)
                    rust_plan, rust_rows = backend.rust_grouped_merge(
                        fixture.path,
                        fmt,
                        ROW_ORDERING,
                        groups,
                        kind=kind,
                        output_path=str(rust_out),
                        output_format=fmt,
                        settings=settings,
                    )
                    # Plans name their output path; compare with it masked.
                    same = py_plan.replace(str(py_out), "<out>") == rust_plan.replace(str(rust_out), "<out>")
                    same_bytes = kind != "write" or py_out.read_bytes() == rust_out.read_bytes()
                    all_match &= same and same_bytes and py_rows == rust_rows
                    sorts = py_plan.count("SortExec:")
                    merges = py_plan.count("SortPreservingMergeExec:")
                    print(
                        f"--- {session} groups={groups} {kind}: {len(plan_bytes)} plan bytes; plans identical: {same}; "
                        f"rows py={py_rows} rust={rust_rows}; output files identical: {same_bytes}; "
                        f"SortExec={sorts} SortPreservingMergeExec={merges}"
                    )
                    if kind == "explain" and groups == 2 and session == "default":
                        print("decoded logical plan (backend side, before reopening scans):")
                        print(decoded)
                        print("physical plan from the Python-built frame:")
                        print(py_plan)
                    if not same:
                        print("PYTHON:\n" + py_plan + "\nRUST:\n" + rust_plan)
    print("\nALL MATCH" if all_match else "\nMISMATCH")


def controls():
    """Negative controls: the comparison can fail, and the backend refuses what it should."""
    print("\n##### controls")
    backend = hail_proto.Backend(THREADS)
    fixture = hail_proto.make_fixture("parquet")
    tables, ordering = backend.open_dataset(fixture.path, "parquet", ROW_ORDERING)
    ctx = SessionContext().with_logical_extension_codec(hail_proto.SeamCodec())
    out = tempfile.mkdtemp(prefix="PROTOTYPE-wipe-me-")
    action = dict(
        kind="explain",
        output_path=f"{out}/x.parquet",
        output_format="parquet",
        ordering=ordering,
        layout="single_file",
        settings=[],
    )
    keys = [col(c).sort(ascending=asc, nulls_first=nf) for c, asc, nf in ordering]

    # 1. A final logical sort over the outer union, the shape ADR 0014 warns about.
    plan, _, _ = backend.execute(
        grouped_merge(ctx, tables, ordering, 2).sort(*keys).logical_plan().to_bytes(ctx), **action
    )
    print(
        "final sort above the union of groups -> SortPreservingMergeExec count:", plan.count("SortPreservingMergeExec:")
    )
    print(plan)

    # 2. An Action ordering that is not the datasets'.
    try:
        backend.execute(
            grouped_merge(ctx, tables, ordering, 2).logical_plan().to_bytes(ctx), **{**action, "ordering": ordering[:2]}
        )
    except RuntimeError as e:
        print("short Action ordering:", e)

    # 3. An unknown setting.
    try:
        backend.execute(
            grouped_merge(ctx, tables, ordering, 2).logical_plan().to_bytes(ctx),
            **{**action, "settings": [("datafusion.no_such_key", "1")]},
        )
    except RuntimeError as e:
        print("unknown setting:", e)

    # 4. Scanning a description in the wheel's own engine.
    try:
        grouped_merge(ctx, tables, ordering, 2).collect()
    except Exception as e:  # noqa: BLE001
        print("wheel collect:", type(e).__name__, e)

    # 5. A session without our codec.
    plain = SessionContext()
    try:
        grouped_merge(plain, tables, ordering, 2).logical_plan().to_bytes(plain)
    except Exception as e:  # noqa: BLE001
        print("to_bytes without the codec:", type(e).__name__, e)


main()
controls()
