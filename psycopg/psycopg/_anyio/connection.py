"""
psycopg async connection objects using AnyIO
"""

# Copyright (C) 2022 The Psycopg Team


from functools import lru_cache
from typing import Any, Optional, TYPE_CHECKING

from .. import errors as e
from ..abc import PQGen, PQGenConn, RV
from ..connection_async import AsyncConnection
from ..rows import Row

if TYPE_CHECKING:
    import anyio
    import sniffio
    from . import waiting
else:
    anyio = sniffio = waiting = None


@lru_cache()
def _import_anyio() -> None:
    global anyio, sniffio, waiting
    try:
        import anyio
        import sniffio
        from . import waiting
    except ImportError as e:
        raise ImportError(
            "anyio is not installed; run `pip install psycopg[anyio]`"
        ) from e


class AnyIOConnection(AsyncConnection[Row]):
    """
    Asynchronous wrapper for a connection to the database using AnyIO
    asynchronous library.
    """

    __module__ = "psycopg"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _import_anyio()
        self._lockcls = anyio.Lock  # type: ignore[assignment]
        super().__init__(*args, **kwargs)

    @staticmethod
    def _async_library() -> str:
        _import_anyio()
        return sniffio.current_async_library()

    @staticmethod
    def _getaddrinfo() -> Any:
        _import_anyio()
        return anyio.getaddrinfo

    async def wait(self, gen: PQGen[RV]) -> RV:
        try:
            return await waiting.wait(gen, self.pgconn.socket)
        except KeyboardInterrupt:
            # TODO: this doesn't seem to work as it does for sync connections
            # see tests/test_concurrency_async.py::test_ctrl_c
            # In the test, the code doesn't reach this branch.

            # On Ctrl-C, try to cancel the query in the server, otherwise
            # otherwise the connection will be stuck in ACTIVE state
            c = self.pgconn.get_cancel()
            c.cancel()
            try:
                await waiting.wait(gen, self.pgconn.socket)
            except e.QueryCanceled:
                pass  # as expected
            raise

    @classmethod
    async def _wait_conn(cls, gen: PQGenConn[RV], timeout: Optional[int]) -> RV:
        return await waiting.wait_conn(gen, timeout)
