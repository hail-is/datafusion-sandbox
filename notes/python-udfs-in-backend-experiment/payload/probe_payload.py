"""Build plans with Python scalar/aggregate/window UDFs, dump their DFPYUD* payloads."""
import os
OUT = os.environ.get("PYUDF_OUT", "/tmp/pyudf-research")  # where .bin files go
import io, pickle, sys
import pyarrow as pa
import pyarrow.compute as pc
from datafusion import SessionContext, udf, udaf, udwf, Accumulator, col
from datafusion.user_defined import WindowEvaluator

def double(arr: pa.Array) -> pa.Array:
    return pc.multiply(arr, 2)

class MySum(Accumulator):
    def __init__(self): self._s = 0
    def update(self, values): self._s += pc.sum(values).as_py() or 0
    def merge(self, states): self._s += pc.sum(states[0]).as_py() or 0
    def state(self): return [pa.scalar(self._s)]
    def evaluate(self): return pa.scalar(self._s)

class RowNum(WindowEvaluator):
    def evaluate_all(self, values, num_rows): return pa.array(range(num_rows), pa.int64())

ctx = SessionContext()
df = ctx.sql("SELECT column1 AS a, column2 AS g FROM (VALUES (1::bigint, 1::bigint), (2, 1), (3, 2))")
u = udf(double, pa.int64(), pa.int64(), "immutable", name="double")
a = udaf(MySum, pa.int64(), pa.int64(), [pa.int64()], "immutable", name="mysum")
w = udwf(RowNum, pa.int64(), pa.int64(), "immutable", name="rownum")
plans = {
    "scalar": df.select(u(col("a"))),
    "aggregate": df.aggregate([col("g")], [a(col("a"))]),
    "window": df.select(w(col("a"))),
}

class Recorder(pickle.Unpickler):
    def __init__(self, f):
        super().__init__(f); self.found = []
    def find_class(self, module, name):
        self.found.append(f"{module}.{name}")
        return super().find_class(module, name)

for kind, d in plans.items():
    buf = d.logical_plan().to_bytes(ctx)
    off = buf.find(b"DFPYUD")
    hdr = buf[off:off + 10]
    blob = io.BytesIO(buf[off + 10:])
    r = Recorder(blob)
    tup = r.load()
    print(f"== {kind}: plan {len(buf)} B, payload at {off}, header={hdr[:7]!r} "
          f"wire_version={hdr[7]} py={hdr[8]}.{hdr[9]} pickle_len={blob.tell()} "
          f"pickle_protocol={buf[off+11]}")
    print("   tuple types:", [type(x).__name__ for x in tup])
    print("   tuple[0], tuple[-1]:", tup[0], tup[-1])
    print("   callable:", tup[1], "module:", getattr(tup[1], "__module__", None),
          "mro:", [c.__module__ + "." + c.__qualname__ for c in getattr(tup[1], "__mro__", [])])
    print("   find_class:", sorted(set(r.found)))
    with open(f"{OUT}/payload-{kind}.bin", "wb") as f:
        f.write(buf[off + 10: off + 10 + blob.tell()])
print("ran on", sys.version)
