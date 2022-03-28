"""
psycopg async cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

from types import TracebackType
from typing import Any, AsyncIterator, Iterable, List
from typing import Optional, Type, TypeVar, TYPE_CHECKING
from contextlib import asynccontextmanager

from . import errors as e
from .abc import Query, Params
from .copy import AsyncCopy
from .rows import Row, RowMaker, AsyncRowFactory
from .cursor import BaseCursor

if TYPE_CHECKING:
    from .connection_async import AsyncConnection

_C = TypeVar("_C", bound="AsyncCursor[Any]")


class AsyncCursor(BaseCursor["AsyncConnection[Any]", Row]):
    __module__ = "psycopg"
    __slots__ = ()

    def __init__(
        self,
        connection: "AsyncConnection[Any]",
        *,
        row_factory: AsyncRowFactory[Row],
    ):
        super().__init__(connection)
        self._row_factory = row_factory

    async def __aenter__(self: _C) -> _C:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def close(self) -> None:
        self._close()

    @property
    def row_factory(self) -> AsyncRowFactory[Row]:
        return self._row_factory

    @row_factory.setter
    def row_factory(self, row_factory: AsyncRowFactory[Row]) -> None:
        self._row_factory = row_factory
        if self.pgresult:
            self._make_row = row_factory(self)

    def _make_row_maker(self) -> RowMaker[Row]:
        return self._row_factory(self)

    async def execute(
        self: _C,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
        binary: Optional[bool] = None,
    ) -> _C:
        try:
            async with self._conn.lock:
                await self._conn.wait(
                    self._execute_gen(query, params, prepare=prepare, binary=binary)
                )
        except e.Error as ex:
            raise ex.with_traceback(None)
        return self

    async def executemany(
        self,
        query: Query,
        params_seq: Iterable[Params],
        *,
        returning: bool = False,
    ) -> None:
        try:
            async with self._conn.pipeline():
                async with self._conn.lock:
                    assert self._execmany_returning is None
                    self._execmany_returning = returning
                    await self._conn.wait(
                        self._executemany_gen(query, params_seq, returning)
                    )
        except e.Error as ex:
            raise ex.with_traceback(None)
        finally:
            self._execmany_returning = None

    async def stream(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        binary: Optional[bool] = None,
    ) -> AsyncIterator[Row]:
        if self._pgconn.pipeline_status:
            raise e.ProgrammingError("stream() cannot be used in pipeline mode")

        async with self._conn.lock:
            await self._conn.wait(self._stream_send_gen(query, params, binary=binary))
            first = True
            while await self._conn.wait(self._stream_fetchone_gen(first)):
                rec = self._tx.load_row(0, self._make_row)
                assert rec is not None
                yield rec
                first = False

    async def fetchone(self) -> Optional[Row]:
        await self._fetch_pipeline()
        self._check_result_for_fetch()
        rv = self._tx.load_row(self._pos, self._make_row)
        if rv is not None:
            self._pos += 1
        return rv

    async def fetchmany(self, size: int = 0) -> List[Row]:
        await self._fetch_pipeline()
        self._check_result_for_fetch()
        assert self.pgresult

        if not size:
            size = self.arraysize
        records = self._tx.load_rows(
            self._pos,
            min(self._pos + size, self.pgresult.ntuples),
            self._make_row,
        )
        self._pos += len(records)
        return records

    async def fetchall(self) -> List[Row]:
        await self._fetch_pipeline()
        self._check_result_for_fetch()
        assert self.pgresult
        records = self._tx.load_rows(self._pos, self.pgresult.ntuples, self._make_row)
        self._pos = self.pgresult.ntuples
        return records

    async def __aiter__(self) -> AsyncIterator[Row]:
        await self._fetch_pipeline()
        self._check_result_for_fetch()

        def load(pos: int) -> Optional[Row]:
            return self._tx.load_row(pos, self._make_row)

        while 1:
            row = load(self._pos)
            if row is None:
                break
            self._pos += 1
            yield row

    async def scroll(self, value: int, mode: str = "relative") -> None:
        self._scroll(value, mode)

    @asynccontextmanager
    async def copy(self, statement: Query) -> AsyncIterator[AsyncCopy]:
        """
        :rtype: AsyncCopy
        """
        async with self._conn.lock:
            await self._conn.wait(self._start_copy_gen(statement))

        async with AsyncCopy(self) as copy:
            yield copy

    async def _fetch_pipeline(self) -> None:
        if not self.pgresult and self._conn._pipeline:
            async with self._conn.lock:
                await self._conn.wait(self._conn._pipeline._fetch_gen(flush=True))
            assert self.pgresult
