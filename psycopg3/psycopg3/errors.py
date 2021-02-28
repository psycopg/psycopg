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

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Type, Union
from typing import cast
from psycopg3.pq.proto import PGresult
from psycopg3.pq._enums import DiagnosticField


class Warning(Exception):
    """
    Exception raised for important warnings.

    Defined for DBAPI compatibility, but never raised by ``psycopg3``.
    """

    __module__ = "psycopg3"


ErrorInfo = Union[None, PGresult, Dict[int, Optional[bytes]]]


class Error(Exception):
    """
    Base exception for all the errors psycopg3 will raise.

    Exception that is the base class of all other error exceptions. You can
    use this to catch all errors with one single `!except` statement.

    This exception is guaranteed to be picklable.
    """

    __module__ = "psycopg3"

    def __init__(
        self,
        *args: Sequence[Any],
        info: ErrorInfo = None,
        encoding: str = "utf-8"
    ):
        super().__init__(*args)
        self._info = info
        self._encoding = encoding

    @property
    def diag(self) -> "Diagnostic":
        """
        A `Diagnostic` object to inspect details of the errors from the database.
        """
        return Diagnostic(self._info, encoding=self._encoding)

    def __reduce__(self) -> Union[str, Tuple[Any, ...]]:
        res = super().__reduce__()
        if isinstance(res, tuple) and len(res) >= 3:
            res[2]["_info"] = self._info_to_dict(self._info)

        return res

    @classmethod
    def _info_to_dict(cls, info: ErrorInfo) -> ErrorInfo:
        """
        Convert a PGresult to a dictionary to make the info picklable.
        """
        # PGresult is a protocol, can't use isinstance
        if hasattr(info, "error_field"):
            info = cast(PGresult, info)
            return {v: info.error_field(v) for v in DiagnosticField}
        else:
            return info


class InterfaceError(Error):
    """
    An error related to the database interface rather than the database itself.
    """

    __module__ = "psycopg3"


class DatabaseError(Error):
    """
    Exception raised for errors that are related to the database.
    """

    __module__ = "psycopg3"


class DataError(DatabaseError):
    """
    An error caused by problems with the processed data.

    Examples may be division by zero, numeric value out of range, etc.
    """

    __module__ = "psycopg3"


class OperationalError(DatabaseError):
    """
    An error related to the database's operation.

    These errors are not necessarily under the control of the programmer, e.g.
    an unexpected disconnect occurs, the data source name is not found, a
    transaction could not be processed, a memory allocation error occurred
    during processing, etc.
    """

    __module__ = "psycopg3"


class IntegrityError(DatabaseError):
    """
    An error caused when the relational integrity of the database is affected.

    An example may be a foreign key check failed.
    """

    __module__ = "psycopg3"


class InternalError(DatabaseError):
    """
    An error generated when the database encounters an internal error,

    Examples could be the cursor is not valid anymore, the transaction is out
    of sync, etc.
    """

    __module__ = "psycopg3"


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors

    Examples may be table not found or already exists, syntax error in the SQL
    statement, wrong number of parameters specified, etc.
    """

    __module__ = "psycopg3"


class NotSupportedError(DatabaseError):
    """
    A method or database API was used which is not supported by the database,
    """

    __module__ = "psycopg3"


class Diagnostic:
    """Details from a database error report."""

    def __init__(self, info: ErrorInfo, encoding: str = "utf-8"):
        self._info = info
        self._encoding = encoding

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
        if self._info:
            if isinstance(self._info, dict):
                val = self._info.get(field)
            else:
                val = self._info.error_field(field)

            if val is not None:
                return val.decode(self._encoding, "replace")

        return None

    def __reduce__(self) -> Union[str, Tuple[Any, ...]]:
        res = super().__reduce__()
        if isinstance(res, tuple) and len(res) >= 3:
            res[2]["_info"] = Error._info_to_dict(self._info)

        return res


def lookup(sqlstate: str) -> Type[Error]:
    """Lookup an error code and return its exception class.

    Raise `!KeyError` if the code is not found.
    """
    return _sqlcodes[sqlstate]


def error_from_result(result: PGresult, encoding: str = "utf-8") -> Error:
    from psycopg3 import pq

    state = result.error_field(DiagnosticField.SQLSTATE) or b""
    cls = _class_for_state(state.decode("ascii"))
    return cls(
        pq.error_message(result, encoding=encoding),
        info=result,
        encoding=encoding,
    )


def _class_for_state(sqlstate: str) -> Type[Error]:
    try:
        return lookup(sqlstate)
    except KeyError:
        return get_base_exception(sqlstate)


def get_base_exception(sqlstate: str) -> Type[Error]:
    return (
        _base_exc_map.get(sqlstate[:2])
        or _base_exc_map.get(sqlstate[0])
        or DatabaseError
    )


_base_exc_map = {
    "08": OperationalError,  # Connection Exception
    "0A": NotSupportedError,  # Feature Not Supported
    "20": ProgrammingError,  # Case Not Foud
    "21": ProgrammingError,  # Cardinality Violation
    "22": DataError,  # Data Exception
    "23": IntegrityError,  # Integrity Constraint Violation
    "24": InternalError,  # Invalid Cursor State
    "25": InternalError,  # Invalid Transaction State
    "26": ProgrammingError,  # Invalid SQL Statement Name *
    "27": OperationalError,  # Triggered Data Change Violation
    "28": OperationalError,  # Invalid Authorization Specification
    "2B": InternalError,  # Dependent Privilege Descriptors Still Exist
    "2D": InternalError,  # Invalid Transaction Termination
    "2F": OperationalError,  # SQL Routine Exception *
    "34": ProgrammingError,  # Invalid Cursor Name *
    "38": OperationalError,  # External Routine Exception *
    "39": OperationalError,  # External Routine Invocation Exception *
    "3B": OperationalError,  # Savepoint Exception *
    "3D": ProgrammingError,  # Invalid Catalog Name
    "3F": ProgrammingError,  # Invalid Schema Name
    "40": OperationalError,  # Transaction Rollback
    "42": ProgrammingError,  # Syntax Error or Access Rule Violation
    "44": ProgrammingError,  # WITH CHECK OPTION Violation
    "53": OperationalError,  # Insufficient Resources
    "54": OperationalError,  # Program Limit Exceeded
    "55": OperationalError,  # Object Not In Prerequisite State
    "57": OperationalError,  # Operator Intervention
    "58": OperationalError,  # System Error (errors external to PostgreSQL itself)
    "F": OperationalError,  # Configuration File Error
    "H": OperationalError,  # Foreign Data Wrapper Error (SQL/MED)
    "P": ProgrammingError,  # PL/pgSQL Error
    "X": InternalError,  # Internal Error
}


def sqlcode(code: str) -> Callable[[Type[Error]], Type[Error]]:
    """
    Decorator to associate an exception class to a sqlstate.
    """

    def sqlcode_(cls: Type[Error]) -> Type[Error]:
        _sqlcodes[code] = cls
        return cls

    return sqlcode_


_sqlcodes: Dict[str, Type[Error]] = {}


# Classes generated by toosls/update_errors.py
# autogenerated: start

# Class 02 - No Data (this is also a warning class per the SQL standard)


@sqlcode("02000")
class NoData(DatabaseError):
    pass


@sqlcode("02001")
class NoAdditionalDynamicResultSetsReturned(DatabaseError):
    pass


# Class 03 - SQL Statement Not Yet Complete


@sqlcode("03000")
class SqlStatementNotYetComplete(DatabaseError):
    pass


# Class 08 - Connection Exception


@sqlcode("08000")
class ConnectionException(OperationalError):
    pass


@sqlcode("08001")
class SqlclientUnableToEstablishSqlconnection(OperationalError):
    pass


@sqlcode("08003")
class ConnectionDoesNotExist(OperationalError):
    pass


@sqlcode("08004")
class SqlserverRejectedEstablishmentOfSqlconnection(OperationalError):
    pass


@sqlcode("08006")
class ConnectionFailure(OperationalError):
    pass


@sqlcode("08007")
class TransactionResolutionUnknown(OperationalError):
    pass


@sqlcode("08P01")
class ProtocolViolation(OperationalError):
    pass


# Class 09 - Triggered Action Exception


@sqlcode("09000")
class TriggeredActionException(DatabaseError):
    pass


# Class 0A - Feature Not Supported


@sqlcode("0A000")
class FeatureNotSupported(NotSupportedError):
    pass


# Class 0B - Invalid Transaction Initiation


@sqlcode("0B000")
class InvalidTransactionInitiation(DatabaseError):
    pass


# Class 0F - Locator Exception


@sqlcode("0F000")
class LocatorException(DatabaseError):
    pass


@sqlcode("0F001")
class InvalidLocatorSpecification(DatabaseError):
    pass


# Class 0L - Invalid Grantor


@sqlcode("0L000")
class InvalidGrantor(DatabaseError):
    pass


@sqlcode("0LP01")
class InvalidGrantOperation(DatabaseError):
    pass


# Class 0P - Invalid Role Specification


@sqlcode("0P000")
class InvalidRoleSpecification(DatabaseError):
    pass


# Class 0Z - Diagnostics Exception


@sqlcode("0Z000")
class DiagnosticsException(DatabaseError):
    pass


@sqlcode("0Z002")
class StackedDiagnosticsAccessedWithoutActiveHandler(DatabaseError):
    pass


# Class 20 - Case Not Found


@sqlcode("20000")
class CaseNotFound(ProgrammingError):
    pass


# Class 21 - Cardinality Violation


@sqlcode("21000")
class CardinalityViolation(ProgrammingError):
    pass


# Class 22 - Data Exception


@sqlcode("22000")
class DataException(DataError):
    pass


@sqlcode("22001")
class StringDataRightTruncation(DataError):
    pass


@sqlcode("22002")
class NullValueNoIndicatorParameter(DataError):
    pass


@sqlcode("22003")
class NumericValueOutOfRange(DataError):
    pass


@sqlcode("22004")
class NullValueNotAllowed(DataError):
    pass


@sqlcode("22005")
class ErrorInAssignment(DataError):
    pass


@sqlcode("22007")
class InvalidDatetimeFormat(DataError):
    pass


@sqlcode("22008")
class DatetimeFieldOverflow(DataError):
    pass


@sqlcode("22009")
class InvalidTimeZoneDisplacementValue(DataError):
    pass


@sqlcode("2200B")
class EscapeCharacterConflict(DataError):
    pass


@sqlcode("2200C")
class InvalidUseOfEscapeCharacter(DataError):
    pass


@sqlcode("2200D")
class InvalidEscapeOctet(DataError):
    pass


@sqlcode("2200F")
class ZeroLengthCharacterString(DataError):
    pass


@sqlcode("2200G")
class MostSpecificTypeMismatch(DataError):
    pass


@sqlcode("2200H")
class SequenceGeneratorLimitExceeded(DataError):
    pass


@sqlcode("2200L")
class NotAnXmlDocument(DataError):
    pass


@sqlcode("2200M")
class InvalidXmlDocument(DataError):
    pass


@sqlcode("2200N")
class InvalidXmlContent(DataError):
    pass


@sqlcode("2200S")
class InvalidXmlComment(DataError):
    pass


@sqlcode("2200T")
class InvalidXmlProcessingInstruction(DataError):
    pass


@sqlcode("22010")
class InvalidIndicatorParameterValue(DataError):
    pass


@sqlcode("22011")
class SubstringError(DataError):
    pass


@sqlcode("22012")
class DivisionByZero(DataError):
    pass


@sqlcode("22013")
class InvalidPrecedingOrFollowingSize(DataError):
    pass


@sqlcode("22014")
class InvalidArgumentForNtileFunction(DataError):
    pass


@sqlcode("22015")
class IntervalFieldOverflow(DataError):
    pass


@sqlcode("22016")
class InvalidArgumentForNthValueFunction(DataError):
    pass


@sqlcode("22018")
class InvalidCharacterValueForCast(DataError):
    pass


@sqlcode("22019")
class InvalidEscapeCharacter(DataError):
    pass


@sqlcode("2201B")
class InvalidRegularExpression(DataError):
    pass


@sqlcode("2201E")
class InvalidArgumentForLogarithm(DataError):
    pass


@sqlcode("2201F")
class InvalidArgumentForPowerFunction(DataError):
    pass


@sqlcode("2201G")
class InvalidArgumentForWidthBucketFunction(DataError):
    pass


@sqlcode("2201W")
class InvalidRowCountInLimitClause(DataError):
    pass


@sqlcode("2201X")
class InvalidRowCountInResultOffsetClause(DataError):
    pass


@sqlcode("22021")
class CharacterNotInRepertoire(DataError):
    pass


@sqlcode("22022")
class IndicatorOverflow(DataError):
    pass


@sqlcode("22023")
class InvalidParameterValue(DataError):
    pass


@sqlcode("22024")
class UnterminatedCString(DataError):
    pass


@sqlcode("22025")
class InvalidEscapeSequence(DataError):
    pass


@sqlcode("22026")
class StringDataLengthMismatch(DataError):
    pass


@sqlcode("22027")
class TrimError(DataError):
    pass


@sqlcode("2202E")
class ArraySubscriptError(DataError):
    pass


@sqlcode("2202G")
class InvalidTablesampleRepeat(DataError):
    pass


@sqlcode("2202H")
class InvalidTablesampleArgument(DataError):
    pass


@sqlcode("22030")
class DuplicateJsonObjectKeyValue(DataError):
    pass


@sqlcode("22031")
class InvalidArgumentForSqlJsonDatetimeFunction(DataError):
    pass


@sqlcode("22032")
class InvalidJsonText(DataError):
    pass


@sqlcode("22033")
class InvalidSqlJsonSubscript(DataError):
    pass


@sqlcode("22034")
class MoreThanOneSqlJsonItem(DataError):
    pass


@sqlcode("22035")
class NoSqlJsonItem(DataError):
    pass


@sqlcode("22036")
class NonNumericSqlJsonItem(DataError):
    pass


@sqlcode("22037")
class NonUniqueKeysInAJsonObject(DataError):
    pass


@sqlcode("22038")
class SingletonSqlJsonItemRequired(DataError):
    pass


@sqlcode("22039")
class SqlJsonArrayNotFound(DataError):
    pass


@sqlcode("2203A")
class SqlJsonMemberNotFound(DataError):
    pass


@sqlcode("2203B")
class SqlJsonNumberNotFound(DataError):
    pass


@sqlcode("2203C")
class SqlJsonObjectNotFound(DataError):
    pass


@sqlcode("2203D")
class TooManyJsonArrayElements(DataError):
    pass


@sqlcode("2203E")
class TooManyJsonObjectMembers(DataError):
    pass


@sqlcode("2203F")
class SqlJsonScalarRequired(DataError):
    pass


@sqlcode("22P01")
class FloatingPointException(DataError):
    pass


@sqlcode("22P02")
class InvalidTextRepresentation(DataError):
    pass


@sqlcode("22P03")
class InvalidBinaryRepresentation(DataError):
    pass


@sqlcode("22P04")
class BadCopyFileFormat(DataError):
    pass


@sqlcode("22P05")
class UntranslatableCharacter(DataError):
    pass


@sqlcode("22P06")
class NonstandardUseOfEscapeCharacter(DataError):
    pass


# Class 23 - Integrity Constraint Violation


@sqlcode("23000")
class IntegrityConstraintViolation(IntegrityError):
    pass


@sqlcode("23001")
class RestrictViolation(IntegrityError):
    pass


@sqlcode("23502")
class NotNullViolation(IntegrityError):
    pass


@sqlcode("23503")
class ForeignKeyViolation(IntegrityError):
    pass


@sqlcode("23505")
class UniqueViolation(IntegrityError):
    pass


@sqlcode("23514")
class CheckViolation(IntegrityError):
    pass


@sqlcode("23P01")
class ExclusionViolation(IntegrityError):
    pass


# Class 24 - Invalid Cursor State


@sqlcode("24000")
class InvalidCursorState(InternalError):
    pass


# Class 25 - Invalid Transaction State


@sqlcode("25000")
class InvalidTransactionState(InternalError):
    pass


@sqlcode("25001")
class ActiveSqlTransaction(InternalError):
    pass


@sqlcode("25002")
class BranchTransactionAlreadyActive(InternalError):
    pass


@sqlcode("25003")
class InappropriateAccessModeForBranchTransaction(InternalError):
    pass


@sqlcode("25004")
class InappropriateIsolationLevelForBranchTransaction(InternalError):
    pass


@sqlcode("25005")
class NoActiveSqlTransactionForBranchTransaction(InternalError):
    pass


@sqlcode("25006")
class ReadOnlySqlTransaction(InternalError):
    pass


@sqlcode("25007")
class SchemaAndDataStatementMixingNotSupported(InternalError):
    pass


@sqlcode("25008")
class HeldCursorRequiresSameIsolationLevel(InternalError):
    pass


@sqlcode("25P01")
class NoActiveSqlTransaction(InternalError):
    pass


@sqlcode("25P02")
class InFailedSqlTransaction(InternalError):
    pass


@sqlcode("25P03")
class IdleInTransactionSessionTimeout(InternalError):
    pass


# Class 26 - Invalid SQL Statement Name


@sqlcode("26000")
class InvalidSqlStatementName(ProgrammingError):
    pass


# Class 27 - Triggered Data Change Violation


@sqlcode("27000")
class TriggeredDataChangeViolation(OperationalError):
    pass


# Class 28 - Invalid Authorization Specification


@sqlcode("28000")
class InvalidAuthorizationSpecification(OperationalError):
    pass


@sqlcode("28P01")
class InvalidPassword(OperationalError):
    pass


# Class 2B - Dependent Privilege Descriptors Still Exist


@sqlcode("2B000")
class DependentPrivilegeDescriptorsStillExist(InternalError):
    pass


@sqlcode("2BP01")
class DependentObjectsStillExist(InternalError):
    pass


# Class 2D - Invalid Transaction Termination


@sqlcode("2D000")
class InvalidTransactionTermination(InternalError):
    pass


# Class 2F - SQL Routine Exception


@sqlcode("2F000")
class SqlRoutineException(OperationalError):
    pass


@sqlcode("2F002")
class ModifyingSqlDataNotPermitted(OperationalError):
    pass


@sqlcode("2F003")
class ProhibitedSqlStatementAttempted(OperationalError):
    pass


@sqlcode("2F004")
class ReadingSqlDataNotPermitted(OperationalError):
    pass


@sqlcode("2F005")
class FunctionExecutedNoReturnStatement(OperationalError):
    pass


# Class 34 - Invalid Cursor Name


@sqlcode("34000")
class InvalidCursorName(ProgrammingError):
    pass


# Class 38 - External Routine Exception


@sqlcode("38000")
class ExternalRoutineException(OperationalError):
    pass


@sqlcode("38001")
class ContainingSqlNotPermitted(OperationalError):
    pass


@sqlcode("38002")
class ModifyingSqlDataNotPermittedExt(OperationalError):
    pass


@sqlcode("38003")
class ProhibitedSqlStatementAttemptedExt(OperationalError):
    pass


@sqlcode("38004")
class ReadingSqlDataNotPermittedExt(OperationalError):
    pass


# Class 39 - External Routine Invocation Exception


@sqlcode("39000")
class ExternalRoutineInvocationException(OperationalError):
    pass


@sqlcode("39001")
class InvalidSqlstateReturned(OperationalError):
    pass


@sqlcode("39004")
class NullValueNotAllowedExt(OperationalError):
    pass


@sqlcode("39P01")
class TriggerProtocolViolated(OperationalError):
    pass


@sqlcode("39P02")
class SrfProtocolViolated(OperationalError):
    pass


@sqlcode("39P03")
class EventTriggerProtocolViolated(OperationalError):
    pass


# Class 3B - Savepoint Exception


@sqlcode("3B000")
class SavepointException(OperationalError):
    pass


@sqlcode("3B001")
class InvalidSavepointSpecification(OperationalError):
    pass


# Class 3D - Invalid Catalog Name


@sqlcode("3D000")
class InvalidCatalogName(ProgrammingError):
    pass


# Class 3F - Invalid Schema Name


@sqlcode("3F000")
class InvalidSchemaName(ProgrammingError):
    pass


# Class 40 - Transaction Rollback


@sqlcode("40000")
class TransactionRollback(OperationalError):
    pass


@sqlcode("40001")
class SerializationFailure(OperationalError):
    pass


@sqlcode("40002")
class TransactionIntegrityConstraintViolation(OperationalError):
    pass


@sqlcode("40003")
class StatementCompletionUnknown(OperationalError):
    pass


@sqlcode("40P01")
class DeadlockDetected(OperationalError):
    pass


# Class 42 - Syntax Error or Access Rule Violation


@sqlcode("42000")
class SyntaxErrorOrAccessRuleViolation(ProgrammingError):
    pass


@sqlcode("42501")
class InsufficientPrivilege(ProgrammingError):
    pass


@sqlcode("42601")
class SyntaxError(ProgrammingError):
    pass


@sqlcode("42602")
class InvalidName(ProgrammingError):
    pass


@sqlcode("42611")
class InvalidColumnDefinition(ProgrammingError):
    pass


@sqlcode("42622")
class NameTooLong(ProgrammingError):
    pass


@sqlcode("42701")
class DuplicateColumn(ProgrammingError):
    pass


@sqlcode("42702")
class AmbiguousColumn(ProgrammingError):
    pass


@sqlcode("42703")
class UndefinedColumn(ProgrammingError):
    pass


@sqlcode("42704")
class UndefinedObject(ProgrammingError):
    pass


@sqlcode("42710")
class DuplicateObject(ProgrammingError):
    pass


@sqlcode("42712")
class DuplicateAlias(ProgrammingError):
    pass


@sqlcode("42723")
class DuplicateFunction(ProgrammingError):
    pass


@sqlcode("42725")
class AmbiguousFunction(ProgrammingError):
    pass


@sqlcode("42803")
class GroupingError(ProgrammingError):
    pass


@sqlcode("42804")
class DatatypeMismatch(ProgrammingError):
    pass


@sqlcode("42809")
class WrongObjectType(ProgrammingError):
    pass


@sqlcode("42830")
class InvalidForeignKey(ProgrammingError):
    pass


@sqlcode("42846")
class CannotCoerce(ProgrammingError):
    pass


@sqlcode("42883")
class UndefinedFunction(ProgrammingError):
    pass


@sqlcode("428C9")
class GeneratedAlways(ProgrammingError):
    pass


@sqlcode("42939")
class ReservedName(ProgrammingError):
    pass


@sqlcode("42P01")
class UndefinedTable(ProgrammingError):
    pass


@sqlcode("42P02")
class UndefinedParameter(ProgrammingError):
    pass


@sqlcode("42P03")
class DuplicateCursor(ProgrammingError):
    pass


@sqlcode("42P04")
class DuplicateDatabase(ProgrammingError):
    pass


@sqlcode("42P05")
class DuplicatePreparedStatement(ProgrammingError):
    pass


@sqlcode("42P06")
class DuplicateSchema(ProgrammingError):
    pass


@sqlcode("42P07")
class DuplicateTable(ProgrammingError):
    pass


@sqlcode("42P08")
class AmbiguousParameter(ProgrammingError):
    pass


@sqlcode("42P09")
class AmbiguousAlias(ProgrammingError):
    pass


@sqlcode("42P10")
class InvalidColumnReference(ProgrammingError):
    pass


@sqlcode("42P11")
class InvalidCursorDefinition(ProgrammingError):
    pass


@sqlcode("42P12")
class InvalidDatabaseDefinition(ProgrammingError):
    pass


@sqlcode("42P13")
class InvalidFunctionDefinition(ProgrammingError):
    pass


@sqlcode("42P14")
class InvalidPreparedStatementDefinition(ProgrammingError):
    pass


@sqlcode("42P15")
class InvalidSchemaDefinition(ProgrammingError):
    pass


@sqlcode("42P16")
class InvalidTableDefinition(ProgrammingError):
    pass


@sqlcode("42P17")
class InvalidObjectDefinition(ProgrammingError):
    pass


@sqlcode("42P18")
class IndeterminateDatatype(ProgrammingError):
    pass


@sqlcode("42P19")
class InvalidRecursion(ProgrammingError):
    pass


@sqlcode("42P20")
class WindowingError(ProgrammingError):
    pass


@sqlcode("42P21")
class CollationMismatch(ProgrammingError):
    pass


@sqlcode("42P22")
class IndeterminateCollation(ProgrammingError):
    pass


# Class 44 - WITH CHECK OPTION Violation


@sqlcode("44000")
class WithCheckOptionViolation(ProgrammingError):
    pass


# Class 53 - Insufficient Resources


@sqlcode("53000")
class InsufficientResources(OperationalError):
    pass


@sqlcode("53100")
class DiskFull(OperationalError):
    pass


@sqlcode("53200")
class OutOfMemory(OperationalError):
    pass


@sqlcode("53300")
class TooManyConnections(OperationalError):
    pass


@sqlcode("53400")
class ConfigurationLimitExceeded(OperationalError):
    pass


# Class 54 - Program Limit Exceeded


@sqlcode("54000")
class ProgramLimitExceeded(OperationalError):
    pass


@sqlcode("54001")
class StatementTooComplex(OperationalError):
    pass


@sqlcode("54011")
class TooManyColumns(OperationalError):
    pass


@sqlcode("54023")
class TooManyArguments(OperationalError):
    pass


# Class 55 - Object Not In Prerequisite State


@sqlcode("55000")
class ObjectNotInPrerequisiteState(OperationalError):
    pass


@sqlcode("55006")
class ObjectInUse(OperationalError):
    pass


@sqlcode("55P02")
class CantChangeRuntimeParam(OperationalError):
    pass


@sqlcode("55P03")
class LockNotAvailable(OperationalError):
    pass


@sqlcode("55P04")
class UnsafeNewEnumValueUsage(OperationalError):
    pass


# Class 57 - Operator Intervention


@sqlcode("57000")
class OperatorIntervention(OperationalError):
    pass


@sqlcode("57014")
class QueryCanceled(OperationalError):
    pass


@sqlcode("57P01")
class AdminShutdown(OperationalError):
    pass


@sqlcode("57P02")
class CrashShutdown(OperationalError):
    pass


@sqlcode("57P03")
class CannotConnectNow(OperationalError):
    pass


@sqlcode("57P04")
class DatabaseDropped(OperationalError):
    pass


# Class 58 - System Error (errors external to PostgreSQL itself)


@sqlcode("58000")
class SystemError(OperationalError):
    pass


@sqlcode("58030")
class IoError(OperationalError):
    pass


@sqlcode("58P01")
class UndefinedFile(OperationalError):
    pass


@sqlcode("58P02")
class DuplicateFile(OperationalError):
    pass


# Class 72 - Snapshot Failure


@sqlcode("72000")
class SnapshotTooOld(DatabaseError):
    pass


# Class F0 - Configuration File Error


@sqlcode("F0000")
class ConfigFileError(OperationalError):
    pass


@sqlcode("F0001")
class LockFileExists(OperationalError):
    pass


# Class HV - Foreign Data Wrapper Error (SQL/MED)


@sqlcode("HV000")
class FdwError(OperationalError):
    pass


@sqlcode("HV001")
class FdwOutOfMemory(OperationalError):
    pass


@sqlcode("HV002")
class FdwDynamicParameterValueNeeded(OperationalError):
    pass


@sqlcode("HV004")
class FdwInvalidDataType(OperationalError):
    pass


@sqlcode("HV005")
class FdwColumnNameNotFound(OperationalError):
    pass


@sqlcode("HV006")
class FdwInvalidDataTypeDescriptors(OperationalError):
    pass


@sqlcode("HV007")
class FdwInvalidColumnName(OperationalError):
    pass


@sqlcode("HV008")
class FdwInvalidColumnNumber(OperationalError):
    pass


@sqlcode("HV009")
class FdwInvalidUseOfNullPointer(OperationalError):
    pass


@sqlcode("HV00A")
class FdwInvalidStringFormat(OperationalError):
    pass


@sqlcode("HV00B")
class FdwInvalidHandle(OperationalError):
    pass


@sqlcode("HV00C")
class FdwInvalidOptionIndex(OperationalError):
    pass


@sqlcode("HV00D")
class FdwInvalidOptionName(OperationalError):
    pass


@sqlcode("HV00J")
class FdwOptionNameNotFound(OperationalError):
    pass


@sqlcode("HV00K")
class FdwReplyHandle(OperationalError):
    pass


@sqlcode("HV00L")
class FdwUnableToCreateExecution(OperationalError):
    pass


@sqlcode("HV00M")
class FdwUnableToCreateReply(OperationalError):
    pass


@sqlcode("HV00N")
class FdwUnableToEstablishConnection(OperationalError):
    pass


@sqlcode("HV00P")
class FdwNoSchemas(OperationalError):
    pass


@sqlcode("HV00Q")
class FdwSchemaNotFound(OperationalError):
    pass


@sqlcode("HV00R")
class FdwTableNotFound(OperationalError):
    pass


@sqlcode("HV010")
class FdwFunctionSequenceError(OperationalError):
    pass


@sqlcode("HV014")
class FdwTooManyHandles(OperationalError):
    pass


@sqlcode("HV021")
class FdwInconsistentDescriptorInformation(OperationalError):
    pass


@sqlcode("HV024")
class FdwInvalidAttributeValue(OperationalError):
    pass


@sqlcode("HV090")
class FdwInvalidStringLengthOrBufferLength(OperationalError):
    pass


@sqlcode("HV091")
class FdwInvalidDescriptorFieldIdentifier(OperationalError):
    pass


# Class P0 - PL/pgSQL Error


@sqlcode("P0000")
class PlpgsqlError(ProgrammingError):
    pass


@sqlcode("P0001")
class RaiseException(ProgrammingError):
    pass


@sqlcode("P0002")
class NoDataFound(ProgrammingError):
    pass


@sqlcode("P0003")
class TooManyRows(ProgrammingError):
    pass


@sqlcode("P0004")
class AssertFailure(ProgrammingError):
    pass


# Class XX - Internal Error


@sqlcode("XX000")
class InternalError_(InternalError):
    pass


@sqlcode("XX001")
class DataCorrupted(InternalError):
    pass


@sqlcode("XX002")
class IndexCorrupted(InternalError):
    pass


# autogenerated: end
