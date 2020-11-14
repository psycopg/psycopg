"""
Transaction context managers returned by Connection.transaction()
"""

# Copyright (C) 2020 The Psycopg Team

import logging

from types import TracebackType
from typing import Optional, Type, TYPE_CHECKING

from . import sql
from .pq import TransactionStatus
from psycopg3.errors import ProgrammingError

if TYPE_CHECKING:
    from .connection import Connection

_log = logging.getLogger(__name__)


class Rollback(Exception):
    """
    Exit the current Transaction context immediately and rollback any changes
    made within this context.

    If a transaction context is specified in the constructor, rollback
    enclosing transactions contexts up to and including the one specified.
    """

    def __init__(self, transaction: Optional["Transaction"] = None):
        self.transaction = transaction

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.transaction!r})"


class Transaction:
    def __init__(
        self,
        connection: "Connection",
        savepoint_name: Optional[str],
        force_rollback: bool,
    ):
        self._conn = connection
        self._savepoint_name: Optional[str] = None
        if savepoint_name is not None:
            if not savepoint_name:
                raise ValueError("savepoint_name must be a non-empty string")
            self._savepoint_name = savepoint_name
        self.force_rollback = force_rollback

        self._outer_transaction: Optional[bool] = None

    @property
    def connection(self) -> "Connection":
        return self._conn

    @property
    def savepoint_name(self) -> Optional[str]:
        return self._savepoint_name

    def __enter__(self) -> "Transaction":
        with self._conn.lock:
            if self._conn.pgconn.transaction_status == TransactionStatus.IDLE:
                assert self._conn._savepoints is None, self._conn._savepoints
                self._conn._savepoints = []
                self._outer_transaction = True
                self._conn._exec_command(b"begin")
            else:
                if self._conn._savepoints is None:
                    self._conn._savepoints = []
                self._outer_transaction = False
                if self._savepoint_name is None:
                    self._savepoint_name = (
                        f"s{len(self._conn._savepoints) + 1}"
                    )

            if self._savepoint_name is not None:
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
        out_of_order_err = ProgrammingError(
            "Out-of-order Transaction context exits. Are you "
            "calling __exit__() manually and getting it wrong?"
        )
        if self._outer_transaction is None:
            raise out_of_order_err
        with self._conn.lock:
            if exc_type is None and not self.force_rollback:
                # Commit changes made in the transaction context
                if self._savepoint_name:
                    if self._conn._savepoints is None:
                        raise out_of_order_err
                    actual = self._conn._savepoints.pop()
                    if actual != self._savepoint_name:
                        raise out_of_order_err
                    self._conn._exec_command(
                        sql.SQL("release savepoint {}").format(
                            sql.Identifier(self._savepoint_name)
                        )
                    )
                if self._outer_transaction:
                    if self._conn._savepoints is None:
                        raise out_of_order_err
                    if len(self._conn._savepoints) != 0:
                        raise out_of_order_err
                    self._conn._exec_command(b"commit")
                    self._conn._savepoints = None
            else:
                # Rollback changes made in the transaction context
                if isinstance(exc_val, Rollback):
                    _log.debug(
                        f"{self._conn}: Explicit rollback from: ",
                        exc_info=True,
                    )

                if self._savepoint_name:
                    if self._conn._savepoints is None:
                        raise out_of_order_err
                    actual = self._conn._savepoints.pop()
                    if actual != self._savepoint_name:
                        raise out_of_order_err
                    self._conn._exec_command(
                        sql.SQL(
                            "rollback to savepoint {n}; release savepoint {n}"
                        ).format(n=sql.Identifier(self._savepoint_name))
                    )
                if self._outer_transaction:
                    if self._conn._savepoints is None:
                        raise out_of_order_err
                    if len(self._conn._savepoints) != 0:
                        raise out_of_order_err
                    self._conn._exec_command(b"rollback")
                    self._conn._savepoints = None

                if isinstance(exc_val, Rollback):
                    if exc_val.transaction in (self, None):
                        return True  # Swallow the exception
        return False

    def __repr__(self) -> str:
        args = [f"connection={self.connection}"]
        if self.savepoint_name is not None:
            args.append(f"savepoint_name={self.savepoint_name!r}")
        if self.force_rollback:
            args.append("force_rollback=True")
        return f"{self.__class__.__qualname__}({', '.join(args)})"
