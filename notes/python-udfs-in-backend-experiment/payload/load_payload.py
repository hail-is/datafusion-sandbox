"""Load DFPYUD* cloudpickle blobs in an interpreter without the datafusion package."""
import os
OUT = os.environ.get("PYUDF_OUT", "/tmp/pyudf-research")  # where .bin files go
import pickle, sys, types
stub = "--stub" in sys.argv
if stub:
    # Minimal stand-in for the two base classes the payloads reference by name.
    import abc
    m = types.ModuleType("datafusion.user_defined")
    class Accumulator(metaclass=abc.ABCMeta): pass
    class WindowEvaluator: pass
    m.Accumulator, m.WindowEvaluator = Accumulator, WindowEvaluator
    pkg = types.ModuleType("datafusion"); pkg.user_defined = m
    sys.modules.update({"datafusion": pkg, "datafusion.user_defined": m})
import pyarrow as pa
for kind in ["scalar", "aggregate", "window"]:
    try:
        t = pickle.loads(open(f"{OUT}/payload-{kind}.bin", "rb").read())
        f = t[1]
        if kind == "scalar":
            out = f(pa.array([1, 2, 3]))
        elif kind == "aggregate":
            acc = f(); acc.update(pa.array([1, 2, 3])); out = acc.evaluate()
        else:
            out = f().evaluate_all([pa.array([5, 6])], 2)
        print(kind, "OK ->", out.to_pylist() if hasattr(out, "to_pylist") else out)
    except Exception as e:
        print(kind, "FAILED:", type(e).__name__, e)
