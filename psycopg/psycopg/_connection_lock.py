"""Connection serialization and cleanup requested by reentrant finalizers."""

# Copyright (C) 2026 The Psycopg Team

from __future__ import annotations

import logging
import weakref
import threading
from typing import TYPE_CHECKING, Any
from collections import deque

from . import errors as e
from . import pq

if TYPE_CHECKING:
    from .connection import Connection
    from ._server_cursor_base import ServerCursorMixin

logger = logging.getLogger("psycopg")


class ConnectionLock:
    def __init__(self, conn: Connection[Any]):
        self._conn = weakref.proxy(conn)
        self._lock = threading.RLock()
        self._owner: int | None = None
        self._depth = 0
        self._pending: deque[ServerCursorMixin[Any, Any]] = deque()
        self._draining = False

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if not self._lock.acquire(blocking, timeout):
            return False
        self._owner = threading.get_ident()
        self._depth += 1
        return True

    def locked(self) -> bool:
        return self._depth != 0

    def release(self) -> None:
        if self._owner != threading.get_ident():
            raise RuntimeError("cannot release an unowned connection lock")
        try:
            if self._depth == 1 and self._pending and not self._draining:
                self._drain()
        finally:
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
            self._lock.release()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: Any) -> None:
        self.release()

    def defer_close(self, cursor: ServerCursorMixin[Any, Any]) -> bool:
        if self._owner != threading.get_ident():
            return False
        if not cursor.closed:
            self._pending.append(cursor)
        return True

    def _drain(self) -> None:
        self._draining = True
        try:
            while self._pending:
                if self._conn.closed:
                    self._pending.clear()
                    break
                if (
                    self._conn.pgconn.pipeline_status
                    or self._conn.pgconn.transaction_status
                    not in (
                        pq.TransactionStatus.IDLE,
                        pq.TransactionStatus.INTRANS,
                    )
                ):
                    # Keep WITH HOLD cursors until rollback or pipeline exit
                    # makes it possible to release them on the server.
                    break
                cursor = self._pending[0]
                # A transaction boundary or explicit SQL CLOSE may already
                # have removed this portal since cleanup was requested.
                cursor._described = False
                try:
                    self._conn.wait(cursor._close_gen())
                except e.Error as exc:
                    # Cleanup from a finalizer must not replace the unrelated
                    # outer operation's result or exception.
                    logger.warning("error ignored closing %r: %s", cursor, exc)
                    if self._conn.closed:
                        self._pending.clear()
                    break
                self._pending.popleft()
        finally:
            self._draining = False
