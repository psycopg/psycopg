"""
Transaction context managers returned by Connection.transaction()
"""

# Copyright (C) 2020 The Psycopg Team

import logging

from types import TracebackType
from typing import Generic, List, Optional, Type, Union, TYPE_CHECKING

from . import sql
from .pq import TransactionStatus
from .proto import ConnectionType

if TYPE_CHECKING:
    from .connection import Connection, AsyncConnection  # noqa: F401

_log = logging.getLogger(__name__)


class Rollback(Exception):
    """
    Exit the current `Transaction` context immediately and rollback any changes
    made within this context.

    If a transaction context is specified in the constructor, rollback
    enclosing transactions contexts up to and including the one specified.
    """

    __module__ = "psycopg3"

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
        self._yolo = True

    @property
    def connection(self) -> ConnectionType:
        """The connection the object is managing."""
        return self._conn

    @property
    def savepoint_name(self) -> Optional[str]:
        """
        The name of the savepoint; `!None` if handling the main transaction.
        """
        # Yes, it may change on __enter__. No, I don't care, because the
        # un-entered state is outside the public interface.
        return self._savepoint_name

    def __repr__(self) -> str:
        args = [f"connection={self.connection}"]
        if not self.savepoint_name:
            args.append(f"savepoint_name={self.savepoint_name!r}")
        if self.force_rollback:
            args.append("force_rollback=True")
        return f"{self.__class__.__qualname__}({', '.join(args)})"

    def _enter_commands(self) -> List[str]:
        if not self._yolo:
            raise TypeError("transaction blocks can be used only once")
        else:
            self._yolo = False

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
            commands.append("begin")

        if self._savepoint_name:
            commands.append(
                sql.SQL("savepoint {}")
                .format(sql.Identifier(self._savepoint_name))
                .as_string(self._conn)
            )

        self._conn._savepoints.append(self._savepoint_name)
        return commands

    def _commit_commands(self) -> List[str]:
        assert self._conn._savepoints[-1] == self._savepoint_name
        self._conn._savepoints.pop()

        commands = []
        if self._savepoint_name and not self._outer_transaction:
            commands.append(
                sql.SQL("release {}")
                .format(sql.Identifier(self._savepoint_name))
                .as_string(self._conn)
            )

        if self._outer_transaction:
            assert not self._conn._savepoints
            commands.append("commit")

        return commands

    def _rollback_commands(self) -> List[str]:
        assert self._conn._savepoints[-1] == self._savepoint_name
        self._conn._savepoints.pop()

        commands = []
        if self._savepoint_name and not self._outer_transaction:
            commands.append(
                sql.SQL("rollback to {n}; release {n}")
                .format(n=sql.Identifier(self._savepoint_name))
                .as_string(self._conn)
            )

        if self._outer_transaction:
            assert not self._conn._savepoints
            commands.append("rollback")

        return commands


class Transaction(BaseTransaction["Connection"]):
    """
    Returned by `Connection.transaction()` to handle a transaction block.
    """

    __module__ = "psycopg3"

    def __enter__(self) -> "Transaction":
        with self._conn.lock:
            self._execute(self._enter_commands())
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        with self._conn.lock:
            if not exc_val and not self.force_rollback:
                self._commit()
                return False
            else:
                return self._rollback(exc_val)

    def _commit(self) -> None:
        """Commit changes made in the transaction context."""
        self._execute(self._commit_commands())

    def _rollback(self, exc_val: Optional[BaseException]) -> bool:
        # Rollback changes made in the transaction context
        if isinstance(exc_val, Rollback):
            _log.debug(
                f"{self._conn}: Explicit rollback from: ", exc_info=True
            )

        self._execute(self._rollback_commands())

        if isinstance(exc_val, Rollback):
            if exc_val.transaction in (self, None):
                return True  # Swallow the exception

        return False

    def _execute(self, commands: List[str]) -> None:
        self._conn._exec_command("; ".join(commands))


class AsyncTransaction(BaseTransaction["AsyncConnection"]):
    """
    Returned by `AsyncConnection.transaction()` to handle a transaction block.
    """

    __module__ = "psycopg3"

    async def __aenter__(self) -> "AsyncTransaction":
        async with self._conn.lock:
            await self._execute(self._enter_commands())

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        async with self._conn.lock:
            if not exc_val and not self.force_rollback:
                await self._commit()
                return False
            else:
                return await self._rollback(exc_val)

    async def _commit(self) -> None:
        """Commit changes made in the transaction context."""
        await self._execute(self._commit_commands())

    async def _rollback(self, exc_val: Optional[BaseException]) -> bool:
        # Rollback changes made in the transaction context
        if isinstance(exc_val, Rollback):
            _log.debug(
                f"{self._conn}: Explicit rollback from: ", exc_info=True
            )

        await self._execute(self._rollback_commands())

        if isinstance(exc_val, Rollback):
            if exc_val.transaction in (self, None):
                return True  # Swallow the exception

        return False

    async def _execute(self, commands: List[str]) -> None:
        await self._conn._exec_command("; ".join(commands))
