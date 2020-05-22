"""
psycopg3 exceptions

DBAPI-defined Exceptions are defined in the following hierarchy::

    Exceptions
    |__Warning
    |__Error
       |__InterfaceError
       |__DatabaseError
          |__DataError
          |__OperationalError
          |__IntegrityError
          |__InternalError
          |__ProgrammingError
          |__NotSupportedError
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, Optional, Sequence, Type
from psycopg3.pq.proto import PGresult
from psycopg3.pq.enums import DiagnosticField


class Warning(Exception):
    """
    Exception raised for important warnings.

    For example data truncations while inserting, etc.
    """


class Error(Exception):
    """
    Base exception for all the errors psycopg3 will raise.
    """

    def __init__(
        self,
        *args: Sequence[Any],
        pgresult: Optional[PGresult] = None,
        encoding: str = "utf-8"
    ):
        super().__init__(*args)
        self.pgresult = pgresult
        self._encoding = encoding

    @property
    def diag(self) -> "Diagnostic":
        return Diagnostic(self.pgresult, encoding=self._encoding)


class InterfaceError(Error):
    """
    An error related to the database interface rather than the database itself.
    """


class DatabaseError(Error):
    """
    An error related to the database.
    """


class DataError(DatabaseError):
    """
    An error caused by  problems with the processed data.

    Examples may be division by zero, numeric value out of range, etc.
    """


class OperationalError(DatabaseError):
    """
    An error related to the database's operation.

    These errors are not necessarily under the control of the programmer, e.g.
    an unexpected disconnect occurs, the data source name is not found, a
    transaction could not be processed, a memory allocation error occurred
    during processing, etc.
    """


class IntegrityError(DatabaseError):
    """
    An error caused when the relational integrity of the database is affected.

    An example may be a foreign key check failed.
    """


class InternalError(DatabaseError):
    """
    An error generated when the database encounters an internal error,

    Examples could be the cursor is not valid anymore, the transaction is out
    of sync, etc.
    """


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors

    Examples may be table not found or already exists, syntax error in the SQL
    statement, wrong number of parameters specified, etc.
    """


class NotSupportedError(DatabaseError):
    """
    A method or database API was used which is not supported by the database,
    """


class Diagnostic:
    def __init__(self, pgresult: Optional[PGresult], encoding: str = "utf-8"):
        self.pgresult = pgresult
        self.encoding = encoding

    @property
    def severity(self) -> Optional[str]:
        return self._error_message(DiagnosticField.SEVERITY)

    @property
    def severity_nonlocalized(self) -> Optional[str]:
        return self._error_message(DiagnosticField.SEVERITY_NONLOCALIZED)

    @property
    def sqlstate(self) -> Optional[str]:
        return self._error_message(DiagnosticField.SQLSTATE)

    @property
    def message_primary(self) -> Optional[str]:
        return self._error_message(DiagnosticField.MESSAGE_PRIMARY)

    @property
    def message_detail(self) -> Optional[str]:
        return self._error_message(DiagnosticField.MESSAGE_DETAIL)

    @property
    def message_hint(self) -> Optional[str]:
        return self._error_message(DiagnosticField.MESSAGE_HINT)

    @property
    def statement_position(self) -> Optional[str]:
        return self._error_message(DiagnosticField.STATEMENT_POSITION)

    @property
    def internal_position(self) -> Optional[str]:
        return self._error_message(DiagnosticField.INTERNAL_POSITION)

    @property
    def internal_query(self) -> Optional[str]:
        return self._error_message(DiagnosticField.INTERNAL_QUERY)

    @property
    def context(self) -> Optional[str]:
        return self._error_message(DiagnosticField.CONTEXT)

    @property
    def schema_name(self) -> Optional[str]:
        return self._error_message(DiagnosticField.SCHEMA_NAME)

    @property
    def table_name(self) -> Optional[str]:
        return self._error_message(DiagnosticField.TABLE_NAME)

    @property
    def column_name(self) -> Optional[str]:
        return self._error_message(DiagnosticField.COLUMN_NAME)

    @property
    def datatype_name(self) -> Optional[str]:
        return self._error_message(DiagnosticField.DATATYPE_NAME)

    @property
    def constraint_name(self) -> Optional[str]:
        return self._error_message(DiagnosticField.CONSTRAINT_NAME)

    @property
    def source_file(self) -> Optional[str]:
        return self._error_message(DiagnosticField.SOURCE_FILE)

    @property
    def source_line(self) -> Optional[str]:
        return self._error_message(DiagnosticField.SOURCE_LINE)

    @property
    def source_function(self) -> Optional[str]:
        return self._error_message(DiagnosticField.SOURCE_FUNCTION)

    def _error_message(self, field: DiagnosticField) -> Optional[str]:
        if self.pgresult is not None:
            val = self.pgresult.error_field(field)
            if val is not None:
                return val.decode(self.encoding, "replace")

        return None


def class_for_state(sqlstate: bytes) -> Type[Error]:
    # TODO: stub
    return DatabaseError


def error_from_result(result: PGresult, encoding: str = "utf-8") -> Error:
    from psycopg3 import pq

    state = result.error_field(DiagnosticField.SQLSTATE) or b""
    cls = class_for_state(state)
    return cls(pq.error_message(result), pgresult=result, encoding=encoding)
