"""
Transaction context managers returned by Connection.transaction()
"""

# Copyright (C) 2020-2021 The Psycopg Team

import logging

from types import TracebackType
from typing import Generic, Optional, Type, Union, TYPE_CHECKING

from . import pq
from . import sql
from .pq import TransactionStatus
from .abc import ConnectionType, PQGen
from .pq.abc import PGresult

if TYPE_CHECKING:
    from typing import Any
    from .connection import Connection
    from .connection_async import AsyncConnection

logger = logging.getLogger(__name__)


class Rollback(Exception):
    """
    Exit the current `Transaction` context immediately and rollback any changes
    made within this context.

    If a transaction context is specified in the constructor, rollback
    enclosing transactions contexts up to and including the one specified.
    """

    __module__ = "psycopg"

    def __init__(
        self,
        transaction: Union["Transaction", "AsyncTransaction", None] = None,
    ):
        self.transaction = transaction

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.transaction!r})"


class BaseTransaction(Generic[ConnectionType]):
    def __init__(
        self,
        connection: ConnectionType,
        savepoint_name: Optional[str] = None,
        force_rollback: bool = False,
    ):
        self._conn = connection
        self._savepoint_name = savepoint_name or ""
        self.force_rollback = force_rollback
        self._entered = self._exited = False

    @property
    def savepoint_name(self) -> Optional[str]:
        """
        The name of the savepoint; `!None` if handling the main transaction.
        """
        # Yes, it may change on __enter__. No, I don't care, because the
        # un-entered state is outside the public interface.
        return self._savepoint_name

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self._conn.pgconn)
        if not self._entered:
            status = "inactive"
        elif not self._exited:
            status = "active"
        else:
            status = "terminated"

        sp = f"{self.savepoint_name!r} " if self.savepoint_name else ""
        return f"<{cls} {sp}({status}) {info} at 0x{id(self):x}>"

    def _enter_gen(self) -> PQGen[PGresult]:
        if self._entered:
            raise TypeError("transaction blocks can be used only once")
        self._entered = True

        self._outer_transaction = (
            self._conn.pgconn.transaction_status == TransactionStatus.IDLE
        )
        if self._outer_transaction:
            # outer transaction: if no name it's only a begin, else
            # there will be an additional savepoint
            assert not self._conn._savepoints
        else:
            # inner transaction: it always has a name
            if not self._savepoint_name:
                self._savepoint_name = (
                    f"_pg3_{len(self._conn._savepoints) + 1}"
                )

        commands = []
        if self._outer_transaction:
            assert not self._conn._savepoints, self._conn._savepoints
            commands.append(self._conn._get_tx_start_command())

        if self._savepoint_name:
            commands.append(
                sql.SQL("SAVEPOINT {}")
                .format(sql.Identifier(self._savepoint_name))
                .as_bytes(self._conn)
            )

        self._conn._savepoints.append(self._savepoint_name)
        return self._conn._exec_command(b"; ".join(commands))

    def _exit_gen(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> PQGen[bool]:
        if not exc_val and not self.force_rollback:
            yield from self._commit_gen()
            return False
        else:
            return (yield from self._rollback_gen(exc_val))

    def _commit_gen(self) -> PQGen[PGresult]:
        assert self._conn._savepoints[-1] == self._savepoint_name
        self._conn._savepoints.pop()
        self._exited = True

        commands = []
        if self._savepoint_name and not self._outer_transaction:
            commands.append(
                sql.SQL("RELEASE {}")
                .format(sql.Identifier(self._savepoint_name))
                .as_bytes(self._conn)
            )

        if self._outer_transaction:
            assert not self._conn._savepoints
            commands.append(b"COMMIT")

        return self._conn._exec_command(b"; ".join(commands))

    def _rollback_gen(self, exc_val: Optional[BaseException]) -> PQGen[bool]:
        if isinstance(exc_val, Rollback):
            logger.debug(
                f"{self._conn}: Explicit rollback from: ", exc_info=True
            )

        assert self._conn._savepoints[-1] == self._savepoint_name
        self._conn._savepoints.pop()

        commands = []
        if self._savepoint_name and not self._outer_transaction:
            commands.append(
                sql.SQL("ROLLBACK TO {n}; RELEASE {n}")
                .format(n=sql.Identifier(self._savepoint_name))
                .as_bytes(self._conn)
            )

        if self._outer_transaction:
            assert not self._conn._savepoints
            commands.append(b"ROLLBACK")

        # Also clear the prepared statements cache.
        cmd = self._conn._prepared.clear()
        if cmd:
            commands.append(cmd)

        yield from self._conn._exec_command(b"; ".join(commands))

        if isinstance(exc_val, Rollback):
            if not exc_val.transaction or exc_val.transaction is self:
                return True  # Swallow the exception

        return False


class Transaction(BaseTransaction["Connection[Any]"]):
    """
    Returned by `Connection.transaction()` to handle a transaction block.
    """

    __module__ = "psycopg"

    @property
    def connection(self) -> "Connection[Any]":
        """The connection the object is managing."""
        return self._conn

    def __enter__(self) -> "Transaction":
        with self._conn.lock:
            self._conn.wait(self._enter_gen())
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        with self._conn.lock:
            return self._conn.wait(self._exit_gen(exc_type, exc_val, exc_tb))


class AsyncTransaction(BaseTransaction["AsyncConnection[Any]"]):
    """
    Returned by `AsyncConnection.transaction()` to handle a transaction block.
    """

    __module__ = "psycopg"

    @property
    def connection(self) -> "AsyncConnection[Any]":
        return self._conn

    async def __aenter__(self) -> "AsyncTransaction":
        async with self._conn.lock:
            await self._conn.wait(self._enter_gen())
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        async with self._conn.lock:
            return await self._conn.wait(
                self._exit_gen(exc_type, exc_val, exc_tb)
            )
