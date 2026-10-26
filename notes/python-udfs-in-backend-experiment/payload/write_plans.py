import runpy, sys
import os
OUT = os.environ.get("PYUDF_OUT", "/tmp/pyudf-research")  # where .bin files go
sys.argv = ["x"]
g = runpy.run_path(os.path.join(os.path.dirname(__file__), "probe_payload.py"))
for kind, d in g["plans"].items():
    open(f"{OUT}/plan-{kind}.bin", "wb").write(d.logical_plan().to_bytes(g["ctx"]))
