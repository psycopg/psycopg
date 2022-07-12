import pytest
import asyncio
import sys

from psycopg.errors import InterfaceError


@pytest.mark.skipif(sys.platform != "win32", reason="windows only test")
def test_windows_error(aconn_cls, dsn):
    loop = asyncio.ProactorEventLoop()  # type: ignore[attr-defined]

    async def go():
        with pytest.raises(
            InterfaceError,
            match="Psycopg cannot use the 'ProactorEventLoop'",
        ):
            await aconn_cls.connect(dsn)

    try:
        loop.run_until_complete(go())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
