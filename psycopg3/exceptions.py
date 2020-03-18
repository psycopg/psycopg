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


class Warning(Exception):
    """
    Exception raised for important warnings.

    For example data truncations while inserting, etc.
    """


class Error(Exception):
    """
    Base exception for all the errors psycopg3 will raise.
    """

    def __init__(self, *args, pgresult=None):
        super().__init__(*args)
        self.pgresult = pgresult


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
