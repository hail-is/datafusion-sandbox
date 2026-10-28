"""PROTOTYPE, throwaway, for hail-is/datafusion-sandbox#322.

Can an ordering assertion hold a key order through the backend, and fail before
execution when the plan can only meet it by sorting? Builds on the #321
prototype: frames are lowered in Python with the stock `datafusion` 55.0.0
wheel, sent as `to_bytes(ctx)` plus an Action to the in-process backend in
`hail_proto`, which lifts `Filter(assert_ordered(...))` into its own node.

1. Shape: grouped-merge with an assertion per group instead of a sort per group,
   with the sink's ordering as a requirement or as an assertion, against the
   Rust plan builder's plan.
2. Cases: where an assertion holds, and where it must fail with a clear error.
3. A lie: a plan DataFusion wrongly believes ordered (#25052), which only a
   run-time order check catches.

Usage: see README.md; `python -I run.py <dir holding hail_proto.abi3.so>`.
"""

import sys
import tempfile
from functools import reduce
from pathlib import Path

sys.path.insert(0, sys.argv[1])

import datafusion  # noqa: E402
import hail_proto  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from datafusion import SessionContext, col, lit, udf  # noqa: E402

THREADS = 4
ROW_ORDERING = "locus_then_alleles"
SESSIONS = {
    "default": [],
    "one-partition": [("datafusion.execution.target_partitions", "1")],
    "hostile": [
        ("datafusion.execution.target_partitions", "8"),
        ("datafusion.optimizer.repartition_file_min_size", "0"),
    ],
}
SMJ = [
    ("datafusion.execution.target_partitions", "4"),
    ("datafusion.optimizer.prefer_hash_join", "false"),
]

assert_ordered = udf(hail_proto.AssertOrdered())
OUT = Path(tempfile.mkdtemp(prefix="PROTOTYPE-wipe-me-"))


def direction(asc, nulls_first):
    return f"{'ASC' if asc else 'DESC'} NULLS {'FIRST' if nulls_first else 'LAST'}"


def assert_order(df, ordering):
    """The frontend's lowering of an ordering assertion: Filter(assert_ordered(key, dir, ...))."""
    args = []
    for column, asc, nulls_first in ordering:
        args += [col(column), lit(direction(asc, nulls_first))]
    return df.filter(assert_ordered(*args))


def input_groups(names, groups):
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


def grouped_merge(ctx, tables, ordering, groups, per_group):
    """per_group is "sort" (#321's lowering), "assert" (an ordering assertion per group), or
    "sort+final" (#321's lowering plus the final logical sort ADR 0014 warns about)."""
    frames = {name: ctx.read_table(table) for name, table in tables}
    keys = [col(c).sort(ascending=asc, nulls_first=nf) for c, asc, nf in ordering]

    def close(frame):
        return frame.sort(*keys) if per_group == "sort" else assert_order(frame, ordering)

    merged = union_or_single(
        [close(union_or_single([frames[n] for n in g])) for g in input_groups(list(frames), groups)]
    )
    return merged.sort(*keys) if per_group == "sort+final" else merged


def shape(plan):
    """Plan lines without assertion nodes or indentation, the output path masked."""
    skip = ("OrderingAssertionExec", "EstablishedOrderExec", "DataSinkExec")
    return [line.strip() for line in plan.splitlines() if not line.strip().startswith(skip)]


def same_parquet_rows(left, right):
    """Same rows in the same order. With the sink's ordering only asserted, the sink is given no
    ordering, so its Parquet footer has no sorting_columns: the files differ in metadata alone."""
    if left.suffix != ".parquet":
        return False
    return pq.read_table(left).to_pylist() == pq.read_table(right).to_pylist()


def section_shape(backend):
    print("\n########## 1. shape: assertions in place of sorts, against the Rust plan builder")
    all_match = True
    for fmt in ["parquet", "vortex"]:
        fixture = hail_proto.make_fixture(fmt)
        tables, ordering = backend.open_dataset(fixture.path, fmt, ROW_ORDERING)
        ctx = SessionContext().with_logical_extension_codec(hail_proto.SeamCodec())
        print(f"\n##### {fmt}: ordering {ordering}")
        variants = [
            ("assert", "require"),
            ("assert", "assert"),
            ("sort", "assert"),
            ("sort+final", "require"),
            ("sort+final", "assert"),
        ]
        for per_group, sink_ordering in variants:
            results, byte_identical, writes = [], 0, 0
            for session, settings in SESSIONS.items():
                for groups in [1, 2, 3, 8]:
                    for kind in ["explain", "write"]:
                        df = grouped_merge(ctx, tables, ordering, groups, per_group)
                        tag = f"{fmt}-{per_group}-{sink_ordering}-{session}-{groups}-{kind}"
                        py_out, rust_out = OUT / f"py-{tag}.{fmt}", OUT / f"rust-{tag}.{fmt}"
                        py_plan, py_rows, _ = backend.execute(
                            df.logical_plan().to_bytes(ctx),
                            kind=kind,
                            output_path=str(py_out),
                            output_format=fmt,
                            ordering=ordering,
                            layout="single_file",
                            settings=settings,
                            sink_ordering=sink_ordering,
                        )
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
                        same = shape(py_plan.replace(str(py_out), "<out>")) == shape(
                            rust_plan.replace(str(rust_out), "<out>")
                        )
                        same_bytes = kind != "write" or py_out.read_bytes() == rust_out.read_bytes()
                        same_rows = kind != "write" or same_bytes or same_parquet_rows(py_out, rust_out)
                        ok = same and same_rows and py_rows == rust_rows and "SortExec" not in py_plan
                        all_match &= ok
                        results.append(ok)
                        byte_identical += kind == "write" and same_bytes
                        writes += kind == "write"
                        if not ok:
                            print(f"MISMATCH {tag}\nPYTHON:\n{py_plan}\nRUST:\n{rust_plan}")
                        if fmt == "parquet" and session == "default" and groups == 2 and kind == "explain":
                            print(f"--- per-group {per_group}, sink {sink_ordering}, 2 groups, physical plan:")
                            print(py_plan)
            print(
                f"per-group {per_group}, sink {sink_ordering}: {sum(results)}/{len(results)} cases match "
                "the Rust plan builder (assertion and sink nodes aside), no SortExec, same rows; "
                f"{byte_identical}/{writes} written files byte-identical"
            )
    print("\nALL MATCH" if all_match else "\nMISMATCH")


def run_case(backend, ctx, title, df, ordering, settings=(), sink_ordering="assert", kind="explain", **extra):
    print(f"\n--- {title}")
    out = OUT / f"case-{abs(hash(title))}.parquet"
    try:
        plan, rows, decoded = backend.execute(
            df.logical_plan().to_bytes(ctx),
            kind=kind,
            output_path=str(out),
            output_format="parquet",
            ordering=ordering,
            layout="single_file",
            settings=list(settings),
            sink_ordering=sink_ordering,
            **extra,
        )
    except Exception as e:  # noqa: BLE001
        print(f"FAILED, {type(e).__name__}:\n{e}")
        return None
    print(f"HELD: SortExec={plan.count('SortExec')} rows={rows}")
    print(plan)
    return out


def section_cases(backend):
    print("\n########## 2. cases")
    fixture = hail_proto.make_fixture("parquet")
    tables, ordering = backend.open_dataset(fixture.path, "parquet", ROW_ORDERING)
    ctx = SessionContext().with_logical_extension_codec(hail_proto.SeamCodec())
    one = ctx.read_table(tables[0][1])
    every = union_or_single([ctx.read_table(t) for _, t in tables])
    keys = [col(c).sort(ascending=asc, nulls_first=nf) for c, asc, nf in ordering]
    position_first = [ordering[1], ordering[0], ordering[2]]

    run_case(
        backend,
        ctx,
        "an order the data lacks: (position, contig, alleles)",
        assert_order(one, position_first),
        position_first,
    )
    run_case(
        backend,
        ctx,
        "two contradicting assertions: (position, contig, alleles) inside, the Action's key order at the root",
        assert_order(one, position_first),
        ordering,
    )
    computed = one.select(col("contig"), (col("position") + lit(1)).alias("position"), col("alleles"), col("s"))
    run_case(backend, ctx, "a computed key: position + 1, asserted in key order", assert_order(computed, ordering), ordering)
    renamed = one.select(col("contig"), col("position").alias("pos"), col("alleles"), col("s"))
    run_case(
        backend,
        ctx,
        "a renamed key: position AS pos",
        assert_order(renamed, [ordering[0], ("pos", True, False), ordering[2]]),
        [ordering[0], ("pos", True, False), ordering[2]],
    )
    for session, settings in SESSIONS.items():
        run_case(
            backend,
            ctx,
            f"a hash repartition on contig (4 partitions) under the assertion, {session} settings",
            assert_order(every.repartition_by_hash(col("contig"), num=4), ordering),
            ordering,
            settings,
        )
    run_case(
        backend,
        ctx,
        "a frontend sort by the key order beneath the assertion (data already ordered)",
        assert_order(every.sort(*keys), ordering),
        ordering,
    )
    by_sample = [("s", True, False), *ordering]
    run_case(
        backend,
        ctx,
        "a frontend sort by (s, key) beneath an assertion of (s, key): a sort the frontend asked for, accepted",
        assert_order(every.sort(*[col(c).sort(ascending=a, nulls_first=n) for c, a, n in by_sample]), by_sample),
        by_sample,
    )
    sorted_by_sample = every.sort(*[col(c).sort(ascending=a, nulls_first=n) for c, a, n in by_sample])
    run_case(
        backend,
        ctx,
        "a frontend sort by (s, key), then a computed key, then an assertion: a sort the backend inserted",
        assert_order(
            sorted_by_sample.select(
                col("s"), col("contig"), (col("position") + lit(1)).alias("position"), col("alleles")
            ),
            by_sample,
        ),
        by_sample,
    )
    final_sort = grouped_merge(ctx, tables, ordering, 2, "sort").sort(*keys)
    run_case(
        backend, ctx, "#321's final logical sort, sink ordering as a requirement", final_sort, ordering, sink_ordering="require"
    )
    run_case(backend, ctx, "#321's final logical sort, sink ordering as an assertion", final_sort, ordering, sink_ordering="assert")
    run_case(
        backend,
        ctx,
        "the marker inside a larger predicate",
        every.filter(assert_ordered(col("contig"), lit("ASC NULLS LAST")) & (col("position") > lit(0))),
        ordering,
    )
    run_case(
        backend,
        ctx,
        "a filter above the assertion",
        assert_order(grouped_merge(ctx, tables, ordering, 2, "assert"), ordering).filter(col("position") > lit(5)),
        ordering,
    )
    run_case(
        backend,
        ctx,
        "a projection above the assertion that drops alleles and s",
        assert_order(every, ordering).select(col("contig"), col("position")),
        ordering[:2],
    )


def is_sorted(path, columns):
    rows = list(zip(*(pq.read_table(path).column(c).to_pylist() for c in columns), strict=True))
    return rows == sorted(rows)


def section_lie(backend):
    print("\n########## 3. a lie: sort-merge join with a repeated left key (apache/datafusion#25052)")
    fixture = hail_proto.make_fixture("parquet")
    tables, _ = backend.open_dataset(fixture.path, "parquet", ROW_ORDERING)
    ctx = SessionContext().with_logical_extension_codec(hail_proto.SeamCodec())
    left = ctx.read_table(tables[0][1]).select(col("contig"), col("s"))
    right = ctx.read_table(tables[1][1]).select(
        col("contig").alias("r_contig"), col("position").alias("r_position"), col("alleles").alias("r_alleles")
    )
    joined = left.join(right, left_on="contig", right_on="r_contig", how="inner")
    claimed = [("contig", True, False), ("r_position", True, False), ("r_alleles", True, False)]
    for nulls_first in [False, True]:
        asserted = [(c, a, nulls_first) for c, a, _ in claimed]
        out = run_case(
            backend,
            ctx,
            f"assert (contig, r_position, r_alleles) NULLS {'FIRST' if nulls_first else 'LAST'} over the join, write",
            assert_order(joined, asserted),
            asserted,
            SMJ,
            kind="write",
        )
        if out is not None:
            print("rows actually in that order:", is_sorted(out, ["contig", "r_position", "r_alleles"]))
            run_case(
                backend,
                ctx,
                "the same, with verify_order=True",
                assert_order(joined, asserted),
                asserted,
                SMJ,
                kind="write",
                verify_order=True,
            )


def main():
    print("datafusion wheel", datafusion.__version__)
    backend = hail_proto.Backend(THREADS)
    which = sys.argv[2:] or ["shape", "cases", "lie"]
    if "shape" in which:
        section_shape(backend)
    if "cases" in which:
        section_cases(backend)
    if "lie" in which:
        section_lie(backend)


main()
