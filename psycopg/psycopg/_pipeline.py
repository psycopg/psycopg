"""
commands pipeline management
"""

# Copyright (C) 2021 The Psycopg Team

import logging
from types import TracebackType
from typing import Any, List, Optional, Union, Tuple, Type, TypeVar, TYPE_CHECKING

from . import pq
from . import errors as e
from .abc import PipelineCommand, PQGen
from ._compat import Deque, TypeAlias
from ._encodings import pgconn_encoding
from ._preparing import Key, Prepare
from .generators import pipeline_communicate, pipeline_send, fetch, fetch_many, send

if TYPE_CHECKING:
    from .pq.abc import PGresult
    from .cursor import BaseCursor
    from .connection import BaseConnection, Connection
    from .connection_async import AsyncConnection


PendingResult: TypeAlias = Union[
    None,
    Tuple[
        "BaseCursor[Any, Any]",
        Optional[Tuple[Key, Prepare, bytes]],
        bool,  # single-row mode
    ],
]

FATAL_ERROR = pq.ExecStatus.FATAL_ERROR
PIPELINE_ABORTED = pq.ExecStatus.PIPELINE_ABORTED
BAD = pq.ConnStatus.BAD

ACTIVE = pq.TransactionStatus.ACTIVE

logger = logging.getLogger("psycopg")


class BasePipeline:

    command_queue: Deque[PipelineCommand]
    result_queue: Deque[PendingResult]
    _is_supported: Optional[bool] = None

    def __init__(self, conn: "BaseConnection[Any]") -> None:
        self._conn = conn
        self.pgconn = conn.pgconn
        self.command_queue = Deque[PipelineCommand]()
        self.result_queue = Deque[PendingResult]()
        self.level = 0

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self._conn.pgconn)
        return f"<{cls} {info} at 0x{id(self):x}>"

    @property
    def status(self) -> pq.PipelineStatus:
        return pq.PipelineStatus(self.pgconn.pipeline_status)

    @classmethod
    def is_supported(cls) -> bool:
        """Return `!True` if the psycopg libpq wrapper supports pipeline mode."""
        if BasePipeline._is_supported is None:
            BasePipeline._is_supported = not cls._not_supported_reason()
        return BasePipeline._is_supported

    @classmethod
    def _not_supported_reason(cls) -> str:
        """Return the reason why the pipeline mode is not supported.

        Return an empty string if pipeline mode is supported.
        """
        # Support only depends on the libpq functions available in the pq
        # wrapper, not on the database version.
        if pq.version() < 140000:
            return (
                f"libpq too old {pq.version()};"
                " v14 or greater required for pipeline mode"
            )

        if pq.__build_version__ < 140000:
            return (
                f"libpq too old: module built for {pq.__build_version__};"
                " v14 or greater required for pipeline mode"
            )

        return ""

    def _enter_gen(self) -> PQGen[None]:
        if not self.is_supported():
            raise e.NotSupportedError(
                f"pipeline mode not supported: {self._not_supported_reason()}"
            )
        if self.level == 0:
            self.pgconn.enter_pipeline_mode()
        elif self.command_queue or self.pgconn.transaction_status == ACTIVE:
            # Nested pipeline case.
            #  Transaction might be ACTIVE when the pipeline uses an "implicit
            #  transaction", typically in autocommit mode. But when entering a
            #  Psycopg transaction(), we expect the IDLE state. By sync()-ing,
            #  we make sure all previous commands are completed and the
            #  transaction gets back to IDLE.
            yield from self._sync_gen()
        self.level += 1

    def _exit(self, exc: Optional[BaseException]) -> None:
        self.level -= 1
        if self.level == 0 and self.pgconn.status != BAD:
            try:
                self.pgconn.exit_pipeline_mode()
            except e.OperationalError as exc2:
                # Notice that this error might be pretty irrecoverable. It
                # happens on COPY, for instance: even if sync succeeds, exiting
                # fails with "cannot exit pipeline mode with uncollected results"
                if exc:
                    logger.warning("error ignored exiting %r: %s", self, exc2)
                else:
                    raise exc2.with_traceback(None)

    def _sync_gen(self) -> PQGen[None]:
        self._enqueue_sync()
        yield from self._communicate_gen()
        yield from self._fetch_gen(flush=False)

    def _exit_gen(self) -> PQGen[None]:
        """
        Exit current pipeline by sending a Sync and fetch back all remaining results.
        """
        try:
            self._enqueue_sync()
            yield from self._communicate_gen()
        finally:
            # No need to force flush since we emitted a sync just before.
            yield from self._fetch_gen(flush=False)

    def _send_gen(self) -> PQGen[None]:
        """Send pending commands in the pipeline."""
        yield from pipeline_send(self.pgconn, self.command_queue)

    def _communicate_gen(self) -> PQGen[None]:
        """Communicate with pipeline to send commands and possibly fetch
        results, which are then processed.
        """
        fetched = yield from pipeline_communicate(self.pgconn, self.command_queue)
        to_process = [(self.result_queue.popleft(), results) for results in fetched]
        for queued, results in to_process:
            self._process_results(queued, results)

    def _fetch_gen(self, *, flush: bool) -> PQGen[None]:
        """Fetch available results from the connection and process them with
        pipeline queued items.

        If 'flush' is True, a PQsendFlushRequest() is issued in order to make
        sure results can be fetched. Otherwise, the caller may emit a
        PQpipelineSync() call to ensure the output buffer gets flushed before
        fetching.
        """
        if not self.result_queue:
            return

        if flush:
            self.pgconn.send_flush_request()
            yield from send(self.pgconn)

        to_process = []
        while self.result_queue:
            results = yield from fetch_many(self.pgconn)
            if not results:
                # No more results to fetch, but there may still be pending
                # commands.
                break
            queued = self.result_queue.popleft()
            to_process.append((queued, results))

        for queued, results in to_process:
            self._process_results(queued, results)

    def _stream_fetchone_gen(
        self, first: bool, cursor: "BaseCursor[Any, Any]"
    ) -> PQGen[Optional["PGresult"]]:
        """Fetch pending results until one row from *cursor* is found, and
        return it.

        *cursor* is expected to have emitted a stream() query, so as *first*
        fetch, we set single-row mode.

        Any results not bound to *cursor* will be processed normally
        (typically results from commands executed before *cursor*.stream()).
        """
        if first:
            self.pgconn.send_flush_request()
            yield from send(self.pgconn)
        while self.result_queue:
            queued = self.result_queue[0]
            if queued is not None and queued[0] is cursor and queued[2]:
                if first:
                    self.pgconn.set_single_row_mode()
                res = yield from fetch(self.pgconn)
                if res is None:
                    del self.result_queue[0]
                return res
            else:
                del self.result_queue[0]
                results = yield from fetch_many(self.pgconn)
                assert results
                self._process_results(queued, results)
        else:
            raise e.InternalError(f"{cursor} not found in pipeline results queue")

    def _process_results(
        self, queued: PendingResult, results: List["PGresult"]
    ) -> None:
        """Process a results set fetched from the current pipeline.

        This matches 'results' with its respective element in the pipeline
        queue. For commands (None value in the pipeline queue), results are
        checked directly. For prepare statement creation requests, update the
        cache. Otherwise, results are attached to their respective cursor.
        """
        if queued is None:
            (result,) = results
            if result.status == FATAL_ERROR:
                raise e.error_from_result(result, encoding=pgconn_encoding(self.pgconn))
            elif result.status == PIPELINE_ABORTED:
                raise e.PipelineAborted("pipeline aborted")
        else:
            cursor, prepinfo, srm = queued
            assert not srm, f"unexpected single-row mode {cursor} in pipeline queue"
            cursor._set_results_from_pipeline(results)
            if prepinfo:
                key, prep, name = prepinfo
                # Update the prepare state of the query.
                cursor._conn._prepared.validate(key, prep, name, results)

    def _enqueue_sync(self) -> None:
        """Enqueue a PQpipelineSync() command."""
        self.command_queue.append(self.pgconn.pipeline_sync)
        self.result_queue.append(None)


class Pipeline(BasePipeline):
    """Handler for connection in pipeline mode."""

    __module__ = "psycopg"
    _conn: "Connection[Any]"
    _Self = TypeVar("_Self", bound="Pipeline")

    def __init__(self, conn: "Connection[Any]") -> None:
        super().__init__(conn)

    def sync(self) -> None:
        """Sync the pipeline, send any pending command and receive and process
        all available results.
        """
        try:
            with self._conn.lock:
                self._conn.wait(self._sync_gen())
        except e.Error as ex:
            raise ex.with_traceback(None)

    def __enter__(self: _Self) -> _Self:
        with self._conn.lock:
            self._conn.wait(self._enter_gen())
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        try:
            with self._conn.lock:
                self._conn.wait(self._exit_gen())
        except Exception as exc2:
            # Don't clobber an exception raised in the block with this one
            if exc_val:
                logger.warning("error ignored terminating %r: %s", self, exc2)
            else:
                raise exc2.with_traceback(None)
        finally:
            self._exit(exc_val)


class AsyncPipeline(BasePipeline):
    """Handler for async connection in pipeline mode."""

    __module__ = "psycopg"
    _conn: "AsyncConnection[Any]"
    _Self = TypeVar("_Self", bound="AsyncPipeline")

    def __init__(self, conn: "AsyncConnection[Any]") -> None:
        super().__init__(conn)

    async def sync(self) -> None:
        try:
            async with self._conn.lock:
                await self._conn.wait(self._sync_gen())
        except e.Error as ex:
            raise ex.with_traceback(None)

    async def __aenter__(self: _Self) -> _Self:
        async with self._conn.lock:
            await self._conn.wait(self._enter_gen())
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        try:
            async with self._conn.lock:
                await self._conn.wait(self._exit_gen())
        except Exception as exc2:
            # Don't clobber an exception raised in the block with this one
            if exc_val:
                logger.warning("error ignored terminating %r: %s", self, exc2)
            else:
                raise exc2.with_traceback(None)
        finally:
            self._exit(exc_val)
