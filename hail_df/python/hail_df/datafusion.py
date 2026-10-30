"""The one module `hail_df` imports DataFusion through.

Every other module in the package imports DataFusion's names from here, so that moving to our own
build of datafusion-python, should a custom logical node ever need one, changes this module only
(ADR 0022).
"""

from datafusion import DataFrame, SessionContext

__all__ = ["DataFrame", "SessionContext"]
