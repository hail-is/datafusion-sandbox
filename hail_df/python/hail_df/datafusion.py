"""The one module `hail_df` imports DataFusion through.

Every other module in the package imports DataFusion's names from here, so that moving to our own
build of datafusion-python, should a custom logical node ever need one, changes this module only
(ADR 0022).
"""

import datafusion
from datafusion import DataFrame, SessionContext

__all__ = ["DATAFUSION_MAJOR", "WIRE_VERSION", "DataFrame", "SessionContext"]

# The DataFusion major the wheel builds plans on, which every backend call checks.
DATAFUSION_MAJOR: int = int(datafusion.__version__.split(".")[0])

# The version of the `DFPYCHN` frame the wheel wraps codec payloads in, which every backend call
# checks: 1 since datafusion 55.0.
WIRE_VERSION: int = 1
