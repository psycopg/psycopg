"""
commands pipeline management
"""

# Copyright (C) 2021 The Psycopg Team

import logging
from types import TracebackType
from typing import Any, List, Optional, Union, Tuple, Type, TYPE_CHECKING

from . import pq
from . import errors as e
from .pq import ConnStatus, ExecStatus
from .abc import PipelineCommand, PQGen
from ._compat import Deque, TypeAlias
from ._cmodule import _psycopg
from ._encodings import pgconn_encoding
from ._preparing import Key, Prepare

if TYPE_CHECKING:
    from .pq.abc import PGresult
    from .cursor import BaseCursor
    from .connection import BaseConnection, Connection
    from .connection_async import AsyncConnection

if _psycopg:
    pipeline_communicate = _psycopg.pipeline_communicate
    fetch_many = _psycopg.fetch_many
    send = _psycopg.send

else:
    from . import generators

    pipeline_communicate = generators.pipeline_communicate
    fetch_many = generators.fetch_many
    send = generators.send

PendingResult: TypeAlias = Union[
    None, Tuple["BaseCursor[Any, Any]", Optional[Tuple[Key, Prepare, bytes]]]
]

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

    @staticmethod
    def is_supported() -> bool:
        """Return `True` if the psycopg libpq wrapper suports pipeline mode."""
        if BasePipeline._is_supported is None:
            # Support only depends on the libpq functions available in the pq
            # wrapper, not on the database version.
            pq_version = pq.__build_version__ or pq.version()
            BasePipeline._is_supported = pq_version >= 140000
        return BasePipeline._is_supported

    def _enter(self) -> None:
        if self.level == 0:
            self.pgconn.enter_pipeline_mode()
        self.level += 1

    def _exit(self) -> None:
        self.level -= 1
        if self.level == 0 and self.pgconn.status != ConnStatus.BAD:
            self.pgconn.exit_pipeline_mode()

    def _sync_gen(self) -> PQGen[None]:
        self._enqueue_sync()
        try:
            # Send any pending commands (e.g. COMMIT or Sync);
            # while processing results, we might get errors...
            yield from self._communicate_gen()
        finally:
            # then fetch all remaining results but without forcing
            # flush since we emitted a sync just before.
            yield from self._fetch_gen(flush=False)

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

    def _process_results(
        self, queued: PendingResult, results: List["PGresult"]
    ) -> None:
        """Process a results set fetched from the current pipeline.

        This matchs 'results' with its respective element in the pipeline
        queue. For commands (None value in the pipeline queue), results are
        checked directly. For prepare statement creation requests, update the
        cache. Otherwise, results are attached to their respective cursor.
        """
        if queued is None:
            (result,) = results
            if result.status == ExecStatus.FATAL_ERROR:
                raise e.error_from_result(result, encoding=pgconn_encoding(self.pgconn))
            elif result.status == ExecStatus.PIPELINE_ABORTED:
                raise e.PipelineAborted("pipeline aborted")
        else:
            cursor, prepinfo = queued
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

    def __init__(self, conn: "Connection[Any]") -> None:
        super().__init__(conn)

    def sync(self) -> None:
        """Sync the pipeline, send any pending command and fetch and process
        all available results.

        This is called when exiting the pipeline, but can be used for other
        purposes (e.g. in nested pipelines).
        """
        with self._conn.lock:
            self._conn.wait(self._sync_gen())

    def __enter__(self) -> "Pipeline":
        self._enter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        try:
            self.sync()
        except Exception as exc2:
            # Don't clobber an exception raised in the block with this one
            if exc_val:
                logger.warning("error ignored syncing %r: %s", self, exc2)
            else:
                raise
        finally:
            try:
                self._exit()
            except Exception as exc2:
                # Notice that this error might be pretty irrecoverable. It
                # happens on COPY, for insance: even if sync succeeds, exiting
                # fails with "cannot exit pipeline mode with uncollected results"
                if exc_val:
                    logger.warning("error ignored exiting %r: %s", self, exc2)
                else:
                    raise


class AsyncPipeline(BasePipeline):
    """Handler for async connection in pipeline mode."""

    __module__ = "psycopg"
    _conn: "AsyncConnection[Any]"

    def __init__(self, conn: "AsyncConnection[Any]") -> None:
        super().__init__(conn)

    async def sync(self) -> None:
        """Sync the pipeline, send any pending command and fetch and process
        all available results.

        This is called when exiting the pipeline, but can be used for other
        purposes (e.g. in nested pipelines).
        """
        async with self._conn.lock:
            await self._conn.wait(self._sync_gen())

    async def __aenter__(self) -> "AsyncPipeline":
        self._enter()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        try:
            await self.sync()
        except Exception as exc2:
            # Don't clobber an exception raised in the block with this one
            if exc_val:
                logger.warning("error ignored syncing %r: %s", self, exc2)
            else:
                raise
        finally:
            try:
                self._exit()
            except Exception as exc2:
                if exc_val:
                    logger.warning("error ignored exiting %r: %s", self, exc2)
                else:
                    raise
