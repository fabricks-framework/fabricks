"""Legacy streaming ingestion for Fabricks.

Streaming (Bronze `append`/`memory` modes, Silver `stream: true`, file parsers) is a
legacy load path. The default path registers an external Bronze table via a pre-invoker
notebook (`mode: register`, batch). Core imports nothing here at module-load time; these
modules are imported lazily only when a job actually requests streaming.
"""

from fabricks.utils.legacy.streaming.read import read, read_batch, read_stream
from fabricks.utils.legacy.streaming.write import write_stream

__all__ = ["read", "read_batch", "read_stream", "write_stream"]
