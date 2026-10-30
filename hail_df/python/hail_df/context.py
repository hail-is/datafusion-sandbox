"""Frontend sessions: the DataFusion `SessionContext` a frontend plans in."""

from hail_df._native import Codec
from hail_df.datafusion import SessionContext


def context() -> SessionContext:
    """A fresh `SessionContext` with the backend's codec installed.

    Plans built in it encode for the backend with `frame.logical_plan().to_bytes(ctx)`. It is
    frontend state: the backend runs every plan in a session of its own.
    """
    return SessionContext().with_logical_extension_codec(Codec())
