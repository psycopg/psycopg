"""
Transaction context managers returned by Connection.transaction()
"""

# Copyright (C) 2020 The Psycopg Team

import logging

from types import TracebackType
from typing import Generic, Optional, Type, Union, TYPE_CHECKING

from . import sql
from .pq import TransactionStatus
from .proto import ConnectionType
from .errors import ProgrammingError

if TYPE_CHECKING:
    from .connection import Connection, AsyncConnection  # noqa: F401

_log = logging.getLogger(__name__)


class Rollback(Exception):
    """
    Exit the current Transaction context immediately and rollback any changes
    made within this context.

    If a transaction context is specified in the constructor, rollback
    enclosing transactions contexts up to and including the one specified.
    """

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
        self._outer_transaction = (
            connection.pgconn.transaction_status == TransactionStatus.IDLE
        )

    @property
    def connection(self) -> ConnectionType:
        return self._conn

    @property
    def savepoint_name(self) -> str:
        return self._savepoint_name

    def __repr__(self) -> str:
        args = [f"connection={self.connection}"]
        if not self.savepoint_name:
            args.append(f"savepoint_name={self.savepoint_name!r}")
        if self.force_rollback:
            args.append("force_rollback=True")
        return f"{self.__class__.__qualname__}({', '.join(args)})"

    _out_of_order_err = ProgrammingError(
        "Out-of-order Transaction context exits. Are you "
        "calling __exit__() manually and getting it wrong?"
    )

    def _pop_savepoint(self) -> None:
        if self._savepoint_name:
            if self._conn._savepoints is None:
                raise self._out_of_order_err
            actual = self._conn._savepoints.pop()
            if actual != self._savepoint_name:
                raise self._out_of_order_err
        if self._outer_transaction:
            if self._conn._savepoints is None or self._conn._savepoints:
                raise self._out_of_order_err
            self._conn._savepoints = None


class Transaction(BaseTransaction["Connection"]):
    def __enter__(self) -> "Transaction":
        with self._conn.lock:
            if self._outer_transaction:
                assert self._conn._savepoints is None, self._conn._savepoints
                self._conn._savepoints = []
                self._conn._exec_command(b"begin")
            else:
                if self._conn._savepoints is None:
                    self._conn._savepoints = []
                if not self._savepoint_name:
                    self._savepoint_name = (
                        f"s{len(self._conn._savepoints) + 1}"
                    )

            if self._savepoint_name:
                self._conn._exec_command(
                    sql.SQL("savepoint {}").format(
                        sql.Identifier(self._savepoint_name)
                    )
                )
                self._conn._savepoints.append(self._savepoint_name)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        with self._conn.lock:
            if not exc_val and not self.force_rollback:
                return self._commit()
            else:
                return self._rollback(exc_val)

    def _commit(self) -> bool:
        """Commit changes made in the transaction context."""
        self._pop_savepoint()
        if self._savepoint_name:
            self._conn._exec_command(
                sql.SQL("release savepoint {}").format(
                    sql.Identifier(self._savepoint_name)
                )
            )
        if self._outer_transaction:
            self._conn._exec_command(b"commit")

        return False  # discarded

    def _rollback(self, exc_val: Optional[BaseException]) -> bool:
        # Rollback changes made in the transaction context
        if isinstance(exc_val, Rollback):
            _log.debug(
                f"{self._conn}: Explicit rollback from: ", exc_info=True
            )

        self._pop_savepoint()
        if self._savepoint_name:
            self._conn._exec_command(
                sql.SQL(
                    "rollback to savepoint {n}; release savepoint {n}"
                ).format(n=sql.Identifier(self._savepoint_name))
            )
        if self._outer_transaction:
            self._conn._exec_command(b"rollback")

        if isinstance(exc_val, Rollback):
            if exc_val.transaction in (self, None):
                return True  # Swallow the exception

        return False


class AsyncTransaction(BaseTransaction["AsyncConnection"]):
    async def __aenter__(self) -> "AsyncTransaction":
        async with self._conn.lock:
            if self._outer_transaction:
                assert self._conn._savepoints is None, self._conn._savepoints
                self._conn._savepoints = []
                await self._conn._exec_command(b"begin")
            else:
                if self._conn._savepoints is None:
                    self._conn._savepoints = []
                if not self._savepoint_name:
                    self._savepoint_name = (
                        f"s{len(self._conn._savepoints) + 1}"
                    )

            if self._savepoint_name:
                await self._conn._exec_command(
                    sql.SQL("savepoint {}").format(
                        sql.Identifier(self._savepoint_name)
                    )
                )
                self._conn._savepoints.append(self._savepoint_name)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        async with self._conn.lock:
            if not exc_val and not self.force_rollback:
                return await self._commit()
            else:
                return await self._rollback(exc_val)

    async def _commit(self) -> bool:
        """Commit changes made in the transaction context."""
        self._pop_savepoint()
        if self._savepoint_name:
            await self._conn._exec_command(
                sql.SQL("release savepoint {}").format(
                    sql.Identifier(self._savepoint_name)
                )
            )
        if self._outer_transaction:
            await self._conn._exec_command(b"commit")

        return False  # discarded

    async def _rollback(self, exc_val: Optional[BaseException]) -> bool:
        # Rollback changes made in the transaction context
        if isinstance(exc_val, Rollback):
            _log.debug(
                f"{self._conn}: Explicit rollback from: ", exc_info=True
            )

        self._pop_savepoint()
        if self._savepoint_name:
            await self._conn._exec_command(
                sql.SQL(
                    "rollback to savepoint {n}; release savepoint {n}"
                ).format(n=sql.Identifier(self._savepoint_name))
            )
        if self._outer_transaction:
            await self._conn._exec_command(b"rollback")

        if isinstance(exc_val, Rollback):
            if exc_val.transaction in (self, None):
                return True  # Swallow the exception

        return False
