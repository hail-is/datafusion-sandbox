"""Throwaway driver for notes/dfpy-seam-encoding.md.

Builds plans with the stock PyPI `datafusion` 55.0.0 wheel against the
seam_probe cdylib, writes `LogicalPlan.to_bytes(ctx)` output to OUT_DIR, and
reports what the probe's codec was handed.

Usage: python -I run.py <dir holding seam_probe.abi3.so> <out dir>
"""

import sys
from pathlib import Path

so_dir, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
sys.path.insert(0, str(so_dir))

import datafusion  # noqa: E402
import pyarrow as pa  # noqa: E402
import seam_probe  # noqa: E402
from datafusion import SessionContext, SessionExtensionComponents, col, lit, udf  # noqa: E402
from datafusion import functions as f  # noqa: E402

out_dir.mkdir(parents=True, exist_ok=True)
print("datafusion", datafusion.__version__)

ASSERT_ORDERED = udf(seam_probe.AssertOrdered())


def build(ctx: SessionContext):
    ctx.register_table("t", seam_probe.SchemaOnlyTable("gs://bucket/table.vortex"))
    ctx.register_udf(ASSERT_ORDERED)
    return (
        ctx.table("t")
        .filter(ASSERT_ORDERED(col("contig"), col("position")))
        .select(col("contig"), col("position"), f.abs(col("value")).alias("abs_value"))
    )


def dump(name: str, ctx, df) -> bytes | None:
    try:
        blob = df.logical_plan().to_bytes(ctx)
    except Exception as e:  # noqa: BLE001
        print(f"[{name}] to_bytes failed: {e}")
        print(f"[{name}] codec saw: {seam_probe.seen()}")
        return None
    (out_dir / f"{name}.bin").write_bytes(blob)
    print(f"[{name}] {len(blob)} bytes; DFPYCHN at {blob.find(b'DFPYCHN')}; "
          f"DFPYUDF at {blob.find(b'DFPYUDF')}; 'assert_ordered' at {blob.find(b'assert_ordered')}")
    print(f"[{name}] codec saw: {seam_probe.seen()}")
    return blob


# 1. Codec installed with with_logical_extension_codec.
ctx_a = SessionContext().with_logical_extension_codec(seam_probe.SeamCodec())
print("ids:", ctx_a.logical_extension_codec_ids())
df_a = build(ctx_a)
blob = dump("extension_codec", ctx_a, df_a)
if blob:
    i = blob.find(b"DFPYCHN")
    print("  envelope:", blob[i : i + 7], blob[i + 7], int.from_bytes(blob[i + 8 : i + 12], "little"),
          blob[i + 12 : i + 12 + 9], blob[i + 21 : i + 21 + 40])


# 2. Codec installed through a with_extensions bundle.
class Bundle:
    def __datafusion_session_components__(self, ctx):
        return SessionExtensionComponents(logical_extension_codecs=(seam_probe.SeamCodec(),))


ctx_b = SessionContext().with_extensions(Bundle())
print("ids:", ctx_b.logical_extension_codec_ids())
dump("with_extensions", ctx_b, build(ctx_b))

# 3. Controls: no ctx (default PythonLogicalCodec), and a session without our codec.
dump("no_ctx", None, df_a)
ctx_plain = SessionContext()
dump("plain_ctx", ctx_plain, build(ctx_plain))

# 4. An in-memory table, which no codec in the chain owns.
dump("memtable", ctx_a, ctx_a.from_pydict({"a": [1, 2]}).select(col("a")))

# 5. Python UDFs, inline (default) and by name.
py_double = udf(lambda a: pa.compute.multiply(a, 2), [pa.float64()], pa.float64(), "immutable",
                name="py_double")
ctx_a.register_udf(py_double)
blob = dump("python_udf_inline", ctx_a, ctx_a.table("t").select(py_double(col("value"))))
if blob:
    i = blob.find(b"DFPYUDF")
    print("  header:", blob[i : i + 7], list(blob[i + 7 : i + 10]), "pickle starts", blob[i + 10 : i + 12])
strict = ctx_a.with_python_udf_inlining(enabled=False)
dump("python_udf_by_name", strict, strict.table("t").select(py_double(col("value"))))

# 6. Decoding in the wheel itself, for comparison. The probe's codec refuses
# to decode, so the first is expected to reach it and fail there.
try:
    datafusion.LogicalPlan.from_bytes(ctx_a, (out_dir / "extension_codec.bin").read_bytes())
except Exception as e:  # noqa: BLE001
    print("wheel from_bytes on ctx_a:", e)
try:
    datafusion.LogicalPlan.from_bytes(SessionContext(), (out_dir / "extension_codec.bin").read_bytes())
except Exception as e:  # noqa: BLE001
    print("wheel from_bytes on a session without the codec:", e)
