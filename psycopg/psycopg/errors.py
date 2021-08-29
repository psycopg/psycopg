"""
psycopg exceptions

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

from psycopg.pq.abc import PGresult
from psycopg.pq._enums import DiagnosticField


class Warning(Exception):
    """
    Exception raised for important warnings.

    Defined for DBAPI compatibility, but never raised by ``psycopg``.
    """

    __module__ = "psycopg"


ErrorInfo = Union[None, PGresult, Dict[int, Optional[bytes]]]


class Error(Exception):
    """
    Base exception for all the errors psycopg will raise.

    Exception that is the base class of all other error exceptions. You can
    use this to catch all errors with one single `!except` statement.

    This exception is guaranteed to be picklable.
    """

    __module__ = "psycopg"

    sqlstate: Optional[str] = None

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

    __module__ = "psycopg"


class DatabaseError(Error):
    """
    Exception raised for errors that are related to the database.
    """

    __module__ = "psycopg"


class DataError(DatabaseError):
    """
    An error caused by problems with the processed data.

    Examples may be division by zero, numeric value out of range, etc.
    """

    __module__ = "psycopg"


class OperationalError(DatabaseError):
    """
    An error related to the database's operation.

    These errors are not necessarily under the control of the programmer, e.g.
    an unexpected disconnect occurs, the data source name is not found, a
    transaction could not be processed, a memory allocation error occurred
    during processing, etc.
    """

    __module__ = "psycopg"


class IntegrityError(DatabaseError):
    """
    An error caused when the relational integrity of the database is affected.

    An example may be a foreign key check failed.
    """

    __module__ = "psycopg"


class InternalError(DatabaseError):
    """
    An error generated when the database encounters an internal error,

    Examples could be the cursor is not valid anymore, the transaction is out
    of sync, etc.
    """

    __module__ = "psycopg"


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors

    Examples may be table not found or already exists, syntax error in the SQL
    statement, wrong number of parameters specified, etc.
    """

    __module__ = "psycopg"


class NotSupportedError(DatabaseError):
    """
    A method or database API was used which is not supported by the database,
    """

    __module__ = "psycopg"


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
    """Lookup an error code or `constant name`__ and return its exception class.

    Raise `!KeyError` if the code is not found.

    .. __: https://www.postgresql.org/docs/current/errcodes-appendix.html
            #ERRCODES-TABLE
    """
    return _sqlcodes[sqlstate.upper()]


def error_from_result(result: PGresult, encoding: str = "utf-8") -> Error:
    from psycopg import pq

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


def sqlcode(
    const_name: str, code: str
) -> Callable[[Type[Error]], Type[Error]]:
    """
    Decorator to associate an exception class to a sqlstate.
    """

    def sqlcode_(cls: Type[Error]) -> Type[Error]:
        _sqlcodes[code] = _sqlcodes[const_name] = cls
        cls.sqlstate = code
        return cls

    return sqlcode_


_sqlcodes: Dict[str, Type[Error]] = {}


# Classes generated by toosls/update_errors.py
# autogenerated: start

# Class 02 - No Data (this is also a warning class per the SQL standard)


@sqlcode("NO_DATA", "02000")
class NoData(DatabaseError):
    pass


@sqlcode("NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED", "02001")
class NoAdditionalDynamicResultSetsReturned(DatabaseError):
    pass


# Class 03 - SQL Statement Not Yet Complete


@sqlcode("SQL_STATEMENT_NOT_YET_COMPLETE", "03000")
class SqlStatementNotYetComplete(DatabaseError):
    pass


# Class 08 - Connection Exception


@sqlcode("CONNECTION_EXCEPTION", "08000")
class ConnectionException(OperationalError):
    pass


@sqlcode("SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION", "08001")
class SqlclientUnableToEstablishSqlconnection(OperationalError):
    pass


@sqlcode("CONNECTION_DOES_NOT_EXIST", "08003")
class ConnectionDoesNotExist(OperationalError):
    pass


@sqlcode("SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION", "08004")
class SqlserverRejectedEstablishmentOfSqlconnection(OperationalError):
    pass


@sqlcode("CONNECTION_FAILURE", "08006")
class ConnectionFailure(OperationalError):
    pass


@sqlcode("TRANSACTION_RESOLUTION_UNKNOWN", "08007")
class TransactionResolutionUnknown(OperationalError):
    pass


@sqlcode("PROTOCOL_VIOLATION", "08P01")
class ProtocolViolation(OperationalError):
    pass


# Class 09 - Triggered Action Exception


@sqlcode("TRIGGERED_ACTION_EXCEPTION", "09000")
class TriggeredActionException(DatabaseError):
    pass


# Class 0A - Feature Not Supported


@sqlcode("FEATURE_NOT_SUPPORTED", "0A000")
class FeatureNotSupported(NotSupportedError):
    pass


# Class 0B - Invalid Transaction Initiation


@sqlcode("INVALID_TRANSACTION_INITIATION", "0B000")
class InvalidTransactionInitiation(DatabaseError):
    pass


# Class 0F - Locator Exception


@sqlcode("LOCATOR_EXCEPTION", "0F000")
class LocatorException(DatabaseError):
    pass


@sqlcode("INVALID_LOCATOR_SPECIFICATION", "0F001")
class InvalidLocatorSpecification(DatabaseError):
    pass


# Class 0L - Invalid Grantor


@sqlcode("INVALID_GRANTOR", "0L000")
class InvalidGrantor(DatabaseError):
    pass


@sqlcode("INVALID_GRANT_OPERATION", "0LP01")
class InvalidGrantOperation(DatabaseError):
    pass


# Class 0P - Invalid Role Specification


@sqlcode("INVALID_ROLE_SPECIFICATION", "0P000")
class InvalidRoleSpecification(DatabaseError):
    pass


# Class 0Z - Diagnostics Exception


@sqlcode("DIAGNOSTICS_EXCEPTION", "0Z000")
class DiagnosticsException(DatabaseError):
    pass


@sqlcode("STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER", "0Z002")
class StackedDiagnosticsAccessedWithoutActiveHandler(DatabaseError):
    pass


# Class 20 - Case Not Found


@sqlcode("CASE_NOT_FOUND", "20000")
class CaseNotFound(ProgrammingError):
    pass


# Class 21 - Cardinality Violation


@sqlcode("CARDINALITY_VIOLATION", "21000")
class CardinalityViolation(ProgrammingError):
    pass


# Class 22 - Data Exception


@sqlcode("DATA_EXCEPTION", "22000")
class DataException(DataError):
    pass


@sqlcode("STRING_DATA_RIGHT_TRUNCATION", "22001")
class StringDataRightTruncation(DataError):
    pass


@sqlcode("NULL_VALUE_NO_INDICATOR_PARAMETER", "22002")
class NullValueNoIndicatorParameter(DataError):
    pass


@sqlcode("NUMERIC_VALUE_OUT_OF_RANGE", "22003")
class NumericValueOutOfRange(DataError):
    pass


@sqlcode("NULL_VALUE_NOT_ALLOWED", "22004")
class NullValueNotAllowed(DataError):
    pass


@sqlcode("ERROR_IN_ASSIGNMENT", "22005")
class ErrorInAssignment(DataError):
    pass


@sqlcode("INVALID_DATETIME_FORMAT", "22007")
class InvalidDatetimeFormat(DataError):
    pass


@sqlcode("DATETIME_FIELD_OVERFLOW", "22008")
class DatetimeFieldOverflow(DataError):
    pass


@sqlcode("INVALID_TIME_ZONE_DISPLACEMENT_VALUE", "22009")
class InvalidTimeZoneDisplacementValue(DataError):
    pass


@sqlcode("ESCAPE_CHARACTER_CONFLICT", "2200B")
class EscapeCharacterConflict(DataError):
    pass


@sqlcode("INVALID_USE_OF_ESCAPE_CHARACTER", "2200C")
class InvalidUseOfEscapeCharacter(DataError):
    pass


@sqlcode("INVALID_ESCAPE_OCTET", "2200D")
class InvalidEscapeOctet(DataError):
    pass


@sqlcode("ZERO_LENGTH_CHARACTER_STRING", "2200F")
class ZeroLengthCharacterString(DataError):
    pass


@sqlcode("MOST_SPECIFIC_TYPE_MISMATCH", "2200G")
class MostSpecificTypeMismatch(DataError):
    pass


@sqlcode("SEQUENCE_GENERATOR_LIMIT_EXCEEDED", "2200H")
class SequenceGeneratorLimitExceeded(DataError):
    pass


@sqlcode("NOT_AN_XML_DOCUMENT", "2200L")
class NotAnXmlDocument(DataError):
    pass


@sqlcode("INVALID_XML_DOCUMENT", "2200M")
class InvalidXmlDocument(DataError):
    pass


@sqlcode("INVALID_XML_CONTENT", "2200N")
class InvalidXmlContent(DataError):
    pass


@sqlcode("INVALID_XML_COMMENT", "2200S")
class InvalidXmlComment(DataError):
    pass


@sqlcode("INVALID_XML_PROCESSING_INSTRUCTION", "2200T")
class InvalidXmlProcessingInstruction(DataError):
    pass


@sqlcode("INVALID_INDICATOR_PARAMETER_VALUE", "22010")
class InvalidIndicatorParameterValue(DataError):
    pass


@sqlcode("SUBSTRING_ERROR", "22011")
class SubstringError(DataError):
    pass


@sqlcode("DIVISION_BY_ZERO", "22012")
class DivisionByZero(DataError):
    pass


@sqlcode("INVALID_PRECEDING_OR_FOLLOWING_SIZE", "22013")
class InvalidPrecedingOrFollowingSize(DataError):
    pass


@sqlcode("INVALID_ARGUMENT_FOR_NTILE_FUNCTION", "22014")
class InvalidArgumentForNtileFunction(DataError):
    pass


@sqlcode("INTERVAL_FIELD_OVERFLOW", "22015")
class IntervalFieldOverflow(DataError):
    pass


@sqlcode("INVALID_ARGUMENT_FOR_NTH_VALUE_FUNCTION", "22016")
class InvalidArgumentForNthValueFunction(DataError):
    pass


@sqlcode("INVALID_CHARACTER_VALUE_FOR_CAST", "22018")
class InvalidCharacterValueForCast(DataError):
    pass


@sqlcode("INVALID_ESCAPE_CHARACTER", "22019")
class InvalidEscapeCharacter(DataError):
    pass


@sqlcode("INVALID_REGULAR_EXPRESSION", "2201B")
class InvalidRegularExpression(DataError):
    pass


@sqlcode("INVALID_ARGUMENT_FOR_LOGARITHM", "2201E")
class InvalidArgumentForLogarithm(DataError):
    pass


@sqlcode("INVALID_ARGUMENT_FOR_POWER_FUNCTION", "2201F")
class InvalidArgumentForPowerFunction(DataError):
    pass


@sqlcode("INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION", "2201G")
class InvalidArgumentForWidthBucketFunction(DataError):
    pass


@sqlcode("INVALID_ROW_COUNT_IN_LIMIT_CLAUSE", "2201W")
class InvalidRowCountInLimitClause(DataError):
    pass


@sqlcode("INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE", "2201X")
class InvalidRowCountInResultOffsetClause(DataError):
    pass


@sqlcode("CHARACTER_NOT_IN_REPERTOIRE", "22021")
class CharacterNotInRepertoire(DataError):
    pass


@sqlcode("INDICATOR_OVERFLOW", "22022")
class IndicatorOverflow(DataError):
    pass


@sqlcode("INVALID_PARAMETER_VALUE", "22023")
class InvalidParameterValue(DataError):
    pass


@sqlcode("UNTERMINATED_C_STRING", "22024")
class UnterminatedCString(DataError):
    pass


@sqlcode("INVALID_ESCAPE_SEQUENCE", "22025")
class InvalidEscapeSequence(DataError):
    pass


@sqlcode("STRING_DATA_LENGTH_MISMATCH", "22026")
class StringDataLengthMismatch(DataError):
    pass


@sqlcode("TRIM_ERROR", "22027")
class TrimError(DataError):
    pass


@sqlcode("ARRAY_SUBSCRIPT_ERROR", "2202E")
class ArraySubscriptError(DataError):
    pass


@sqlcode("INVALID_TABLESAMPLE_REPEAT", "2202G")
class InvalidTablesampleRepeat(DataError):
    pass


@sqlcode("INVALID_TABLESAMPLE_ARGUMENT", "2202H")
class InvalidTablesampleArgument(DataError):
    pass


@sqlcode("DUPLICATE_JSON_OBJECT_KEY_VALUE", "22030")
class DuplicateJsonObjectKeyValue(DataError):
    pass


@sqlcode("INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION", "22031")
class InvalidArgumentForSqlJsonDatetimeFunction(DataError):
    pass


@sqlcode("INVALID_JSON_TEXT", "22032")
class InvalidJsonText(DataError):
    pass


@sqlcode("INVALID_SQL_JSON_SUBSCRIPT", "22033")
class InvalidSqlJsonSubscript(DataError):
    pass


@sqlcode("MORE_THAN_ONE_SQL_JSON_ITEM", "22034")
class MoreThanOneSqlJsonItem(DataError):
    pass


@sqlcode("NO_SQL_JSON_ITEM", "22035")
class NoSqlJsonItem(DataError):
    pass


@sqlcode("NON_NUMERIC_SQL_JSON_ITEM", "22036")
class NonNumericSqlJsonItem(DataError):
    pass


@sqlcode("NON_UNIQUE_KEYS_IN_A_JSON_OBJECT", "22037")
class NonUniqueKeysInAJsonObject(DataError):
    pass


@sqlcode("SINGLETON_SQL_JSON_ITEM_REQUIRED", "22038")
class SingletonSqlJsonItemRequired(DataError):
    pass


@sqlcode("SQL_JSON_ARRAY_NOT_FOUND", "22039")
class SqlJsonArrayNotFound(DataError):
    pass


@sqlcode("SQL_JSON_MEMBER_NOT_FOUND", "2203A")
class SqlJsonMemberNotFound(DataError):
    pass


@sqlcode("SQL_JSON_NUMBER_NOT_FOUND", "2203B")
class SqlJsonNumberNotFound(DataError):
    pass


@sqlcode("SQL_JSON_OBJECT_NOT_FOUND", "2203C")
class SqlJsonObjectNotFound(DataError):
    pass


@sqlcode("TOO_MANY_JSON_ARRAY_ELEMENTS", "2203D")
class TooManyJsonArrayElements(DataError):
    pass


@sqlcode("TOO_MANY_JSON_OBJECT_MEMBERS", "2203E")
class TooManyJsonObjectMembers(DataError):
    pass


@sqlcode("SQL_JSON_SCALAR_REQUIRED", "2203F")
class SqlJsonScalarRequired(DataError):
    pass


@sqlcode("FLOATING_POINT_EXCEPTION", "22P01")
class FloatingPointException(DataError):
    pass


@sqlcode("INVALID_TEXT_REPRESENTATION", "22P02")
class InvalidTextRepresentation(DataError):
    pass


@sqlcode("INVALID_BINARY_REPRESENTATION", "22P03")
class InvalidBinaryRepresentation(DataError):
    pass


@sqlcode("BAD_COPY_FILE_FORMAT", "22P04")
class BadCopyFileFormat(DataError):
    pass


@sqlcode("UNTRANSLATABLE_CHARACTER", "22P05")
class UntranslatableCharacter(DataError):
    pass


@sqlcode("NONSTANDARD_USE_OF_ESCAPE_CHARACTER", "22P06")
class NonstandardUseOfEscapeCharacter(DataError):
    pass


# Class 23 - Integrity Constraint Violation


@sqlcode("INTEGRITY_CONSTRAINT_VIOLATION", "23000")
class IntegrityConstraintViolation(IntegrityError):
    pass


@sqlcode("RESTRICT_VIOLATION", "23001")
class RestrictViolation(IntegrityError):
    pass


@sqlcode("NOT_NULL_VIOLATION", "23502")
class NotNullViolation(IntegrityError):
    pass


@sqlcode("FOREIGN_KEY_VIOLATION", "23503")
class ForeignKeyViolation(IntegrityError):
    pass


@sqlcode("UNIQUE_VIOLATION", "23505")
class UniqueViolation(IntegrityError):
    pass


@sqlcode("CHECK_VIOLATION", "23514")
class CheckViolation(IntegrityError):
    pass


@sqlcode("EXCLUSION_VIOLATION", "23P01")
class ExclusionViolation(IntegrityError):
    pass


# Class 24 - Invalid Cursor State


@sqlcode("INVALID_CURSOR_STATE", "24000")
class InvalidCursorState(InternalError):
    pass


# Class 25 - Invalid Transaction State


@sqlcode("INVALID_TRANSACTION_STATE", "25000")
class InvalidTransactionState(InternalError):
    pass


@sqlcode("ACTIVE_SQL_TRANSACTION", "25001")
class ActiveSqlTransaction(InternalError):
    pass


@sqlcode("BRANCH_TRANSACTION_ALREADY_ACTIVE", "25002")
class BranchTransactionAlreadyActive(InternalError):
    pass


@sqlcode("INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION", "25003")
class InappropriateAccessModeForBranchTransaction(InternalError):
    pass


@sqlcode("INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION", "25004")
class InappropriateIsolationLevelForBranchTransaction(InternalError):
    pass


@sqlcode("NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION", "25005")
class NoActiveSqlTransactionForBranchTransaction(InternalError):
    pass


@sqlcode("READ_ONLY_SQL_TRANSACTION", "25006")
class ReadOnlySqlTransaction(InternalError):
    pass


@sqlcode("SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED", "25007")
class SchemaAndDataStatementMixingNotSupported(InternalError):
    pass


@sqlcode("HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL", "25008")
class HeldCursorRequiresSameIsolationLevel(InternalError):
    pass


@sqlcode("NO_ACTIVE_SQL_TRANSACTION", "25P01")
class NoActiveSqlTransaction(InternalError):
    pass


@sqlcode("IN_FAILED_SQL_TRANSACTION", "25P02")
class InFailedSqlTransaction(InternalError):
    pass


@sqlcode("IDLE_IN_TRANSACTION_SESSION_TIMEOUT", "25P03")
class IdleInTransactionSessionTimeout(InternalError):
    pass


# Class 26 - Invalid SQL Statement Name


@sqlcode("INVALID_SQL_STATEMENT_NAME", "26000")
class InvalidSqlStatementName(ProgrammingError):
    pass


# Class 27 - Triggered Data Change Violation


@sqlcode("TRIGGERED_DATA_CHANGE_VIOLATION", "27000")
class TriggeredDataChangeViolation(OperationalError):
    pass


# Class 28 - Invalid Authorization Specification


@sqlcode("INVALID_AUTHORIZATION_SPECIFICATION", "28000")
class InvalidAuthorizationSpecification(OperationalError):
    pass


@sqlcode("INVALID_PASSWORD", "28P01")
class InvalidPassword(OperationalError):
    pass


# Class 2B - Dependent Privilege Descriptors Still Exist


@sqlcode("DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST", "2B000")
class DependentPrivilegeDescriptorsStillExist(InternalError):
    pass


@sqlcode("DEPENDENT_OBJECTS_STILL_EXIST", "2BP01")
class DependentObjectsStillExist(InternalError):
    pass


# Class 2D - Invalid Transaction Termination


@sqlcode("INVALID_TRANSACTION_TERMINATION", "2D000")
class InvalidTransactionTermination(InternalError):
    pass


# Class 2F - SQL Routine Exception


@sqlcode("SQL_ROUTINE_EXCEPTION", "2F000")
class SqlRoutineException(OperationalError):
    pass


@sqlcode("MODIFYING_SQL_DATA_NOT_PERMITTED", "2F002")
class ModifyingSqlDataNotPermitted(OperationalError):
    pass


@sqlcode("PROHIBITED_SQL_STATEMENT_ATTEMPTED", "2F003")
class ProhibitedSqlStatementAttempted(OperationalError):
    pass


@sqlcode("READING_SQL_DATA_NOT_PERMITTED", "2F004")
class ReadingSqlDataNotPermitted(OperationalError):
    pass


@sqlcode("FUNCTION_EXECUTED_NO_RETURN_STATEMENT", "2F005")
class FunctionExecutedNoReturnStatement(OperationalError):
    pass


# Class 34 - Invalid Cursor Name


@sqlcode("INVALID_CURSOR_NAME", "34000")
class InvalidCursorName(ProgrammingError):
    pass


# Class 38 - External Routine Exception


@sqlcode("EXTERNAL_ROUTINE_EXCEPTION", "38000")
class ExternalRoutineException(OperationalError):
    pass


@sqlcode("CONTAINING_SQL_NOT_PERMITTED", "38001")
class ContainingSqlNotPermitted(OperationalError):
    pass


@sqlcode("MODIFYING_SQL_DATA_NOT_PERMITTED", "38002")
class ModifyingSqlDataNotPermittedExt(OperationalError):
    pass


@sqlcode("PROHIBITED_SQL_STATEMENT_ATTEMPTED", "38003")
class ProhibitedSqlStatementAttemptedExt(OperationalError):
    pass


@sqlcode("READING_SQL_DATA_NOT_PERMITTED", "38004")
class ReadingSqlDataNotPermittedExt(OperationalError):
    pass


# Class 39 - External Routine Invocation Exception


@sqlcode("EXTERNAL_ROUTINE_INVOCATION_EXCEPTION", "39000")
class ExternalRoutineInvocationException(OperationalError):
    pass


@sqlcode("INVALID_SQLSTATE_RETURNED", "39001")
class InvalidSqlstateReturned(OperationalError):
    pass


@sqlcode("NULL_VALUE_NOT_ALLOWED", "39004")
class NullValueNotAllowedExt(OperationalError):
    pass


@sqlcode("TRIGGER_PROTOCOL_VIOLATED", "39P01")
class TriggerProtocolViolated(OperationalError):
    pass


@sqlcode("SRF_PROTOCOL_VIOLATED", "39P02")
class SrfProtocolViolated(OperationalError):
    pass


@sqlcode("EVENT_TRIGGER_PROTOCOL_VIOLATED", "39P03")
class EventTriggerProtocolViolated(OperationalError):
    pass


# Class 3B - Savepoint Exception


@sqlcode("SAVEPOINT_EXCEPTION", "3B000")
class SavepointException(OperationalError):
    pass


@sqlcode("INVALID_SAVEPOINT_SPECIFICATION", "3B001")
class InvalidSavepointSpecification(OperationalError):
    pass


# Class 3D - Invalid Catalog Name


@sqlcode("INVALID_CATALOG_NAME", "3D000")
class InvalidCatalogName(ProgrammingError):
    pass


# Class 3F - Invalid Schema Name


@sqlcode("INVALID_SCHEMA_NAME", "3F000")
class InvalidSchemaName(ProgrammingError):
    pass


# Class 40 - Transaction Rollback


@sqlcode("TRANSACTION_ROLLBACK", "40000")
class TransactionRollback(OperationalError):
    pass


@sqlcode("SERIALIZATION_FAILURE", "40001")
class SerializationFailure(OperationalError):
    pass


@sqlcode("TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION", "40002")
class TransactionIntegrityConstraintViolation(OperationalError):
    pass


@sqlcode("STATEMENT_COMPLETION_UNKNOWN", "40003")
class StatementCompletionUnknown(OperationalError):
    pass


@sqlcode("DEADLOCK_DETECTED", "40P01")
class DeadlockDetected(OperationalError):
    pass


# Class 42 - Syntax Error or Access Rule Violation


@sqlcode("SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION", "42000")
class SyntaxErrorOrAccessRuleViolation(ProgrammingError):
    pass


@sqlcode("INSUFFICIENT_PRIVILEGE", "42501")
class InsufficientPrivilege(ProgrammingError):
    pass


@sqlcode("SYNTAX_ERROR", "42601")
class SyntaxError(ProgrammingError):
    pass


@sqlcode("INVALID_NAME", "42602")
class InvalidName(ProgrammingError):
    pass


@sqlcode("INVALID_COLUMN_DEFINITION", "42611")
class InvalidColumnDefinition(ProgrammingError):
    pass


@sqlcode("NAME_TOO_LONG", "42622")
class NameTooLong(ProgrammingError):
    pass


@sqlcode("DUPLICATE_COLUMN", "42701")
class DuplicateColumn(ProgrammingError):
    pass


@sqlcode("AMBIGUOUS_COLUMN", "42702")
class AmbiguousColumn(ProgrammingError):
    pass


@sqlcode("UNDEFINED_COLUMN", "42703")
class UndefinedColumn(ProgrammingError):
    pass


@sqlcode("UNDEFINED_OBJECT", "42704")
class UndefinedObject(ProgrammingError):
    pass


@sqlcode("DUPLICATE_OBJECT", "42710")
class DuplicateObject(ProgrammingError):
    pass


@sqlcode("DUPLICATE_ALIAS", "42712")
class DuplicateAlias(ProgrammingError):
    pass


@sqlcode("DUPLICATE_FUNCTION", "42723")
class DuplicateFunction(ProgrammingError):
    pass


@sqlcode("AMBIGUOUS_FUNCTION", "42725")
class AmbiguousFunction(ProgrammingError):
    pass


@sqlcode("GROUPING_ERROR", "42803")
class GroupingError(ProgrammingError):
    pass


@sqlcode("DATATYPE_MISMATCH", "42804")
class DatatypeMismatch(ProgrammingError):
    pass


@sqlcode("WRONG_OBJECT_TYPE", "42809")
class WrongObjectType(ProgrammingError):
    pass


@sqlcode("INVALID_FOREIGN_KEY", "42830")
class InvalidForeignKey(ProgrammingError):
    pass


@sqlcode("CANNOT_COERCE", "42846")
class CannotCoerce(ProgrammingError):
    pass


@sqlcode("UNDEFINED_FUNCTION", "42883")
class UndefinedFunction(ProgrammingError):
    pass


@sqlcode("GENERATED_ALWAYS", "428C9")
class GeneratedAlways(ProgrammingError):
    pass


@sqlcode("RESERVED_NAME", "42939")
class ReservedName(ProgrammingError):
    pass


@sqlcode("UNDEFINED_TABLE", "42P01")
class UndefinedTable(ProgrammingError):
    pass


@sqlcode("UNDEFINED_PARAMETER", "42P02")
class UndefinedParameter(ProgrammingError):
    pass


@sqlcode("DUPLICATE_CURSOR", "42P03")
class DuplicateCursor(ProgrammingError):
    pass


@sqlcode("DUPLICATE_DATABASE", "42P04")
class DuplicateDatabase(ProgrammingError):
    pass


@sqlcode("DUPLICATE_PREPARED_STATEMENT", "42P05")
class DuplicatePreparedStatement(ProgrammingError):
    pass


@sqlcode("DUPLICATE_SCHEMA", "42P06")
class DuplicateSchema(ProgrammingError):
    pass


@sqlcode("DUPLICATE_TABLE", "42P07")
class DuplicateTable(ProgrammingError):
    pass


@sqlcode("AMBIGUOUS_PARAMETER", "42P08")
class AmbiguousParameter(ProgrammingError):
    pass


@sqlcode("AMBIGUOUS_ALIAS", "42P09")
class AmbiguousAlias(ProgrammingError):
    pass


@sqlcode("INVALID_COLUMN_REFERENCE", "42P10")
class InvalidColumnReference(ProgrammingError):
    pass


@sqlcode("INVALID_CURSOR_DEFINITION", "42P11")
class InvalidCursorDefinition(ProgrammingError):
    pass


@sqlcode("INVALID_DATABASE_DEFINITION", "42P12")
class InvalidDatabaseDefinition(ProgrammingError):
    pass


@sqlcode("INVALID_FUNCTION_DEFINITION", "42P13")
class InvalidFunctionDefinition(ProgrammingError):
    pass


@sqlcode("INVALID_PREPARED_STATEMENT_DEFINITION", "42P14")
class InvalidPreparedStatementDefinition(ProgrammingError):
    pass


@sqlcode("INVALID_SCHEMA_DEFINITION", "42P15")
class InvalidSchemaDefinition(ProgrammingError):
    pass


@sqlcode("INVALID_TABLE_DEFINITION", "42P16")
class InvalidTableDefinition(ProgrammingError):
    pass


@sqlcode("INVALID_OBJECT_DEFINITION", "42P17")
class InvalidObjectDefinition(ProgrammingError):
    pass


@sqlcode("INDETERMINATE_DATATYPE", "42P18")
class IndeterminateDatatype(ProgrammingError):
    pass


@sqlcode("INVALID_RECURSION", "42P19")
class InvalidRecursion(ProgrammingError):
    pass


@sqlcode("WINDOWING_ERROR", "42P20")
class WindowingError(ProgrammingError):
    pass


@sqlcode("COLLATION_MISMATCH", "42P21")
class CollationMismatch(ProgrammingError):
    pass


@sqlcode("INDETERMINATE_COLLATION", "42P22")
class IndeterminateCollation(ProgrammingError):
    pass


# Class 44 - WITH CHECK OPTION Violation


@sqlcode("WITH_CHECK_OPTION_VIOLATION", "44000")
class WithCheckOptionViolation(ProgrammingError):
    pass


# Class 53 - Insufficient Resources


@sqlcode("INSUFFICIENT_RESOURCES", "53000")
class InsufficientResources(OperationalError):
    pass


@sqlcode("DISK_FULL", "53100")
class DiskFull(OperationalError):
    pass


@sqlcode("OUT_OF_MEMORY", "53200")
class OutOfMemory(OperationalError):
    pass


@sqlcode("TOO_MANY_CONNECTIONS", "53300")
class TooManyConnections(OperationalError):
    pass


@sqlcode("CONFIGURATION_LIMIT_EXCEEDED", "53400")
class ConfigurationLimitExceeded(OperationalError):
    pass


# Class 54 - Program Limit Exceeded


@sqlcode("PROGRAM_LIMIT_EXCEEDED", "54000")
class ProgramLimitExceeded(OperationalError):
    pass


@sqlcode("STATEMENT_TOO_COMPLEX", "54001")
class StatementTooComplex(OperationalError):
    pass


@sqlcode("TOO_MANY_COLUMNS", "54011")
class TooManyColumns(OperationalError):
    pass


@sqlcode("TOO_MANY_ARGUMENTS", "54023")
class TooManyArguments(OperationalError):
    pass


# Class 55 - Object Not In Prerequisite State


@sqlcode("OBJECT_NOT_IN_PREREQUISITE_STATE", "55000")
class ObjectNotInPrerequisiteState(OperationalError):
    pass


@sqlcode("OBJECT_IN_USE", "55006")
class ObjectInUse(OperationalError):
    pass


@sqlcode("CANT_CHANGE_RUNTIME_PARAM", "55P02")
class CantChangeRuntimeParam(OperationalError):
    pass


@sqlcode("LOCK_NOT_AVAILABLE", "55P03")
class LockNotAvailable(OperationalError):
    pass


@sqlcode("UNSAFE_NEW_ENUM_VALUE_USAGE", "55P04")
class UnsafeNewEnumValueUsage(OperationalError):
    pass


# Class 57 - Operator Intervention


@sqlcode("OPERATOR_INTERVENTION", "57000")
class OperatorIntervention(OperationalError):
    pass


@sqlcode("QUERY_CANCELED", "57014")
class QueryCanceled(OperationalError):
    pass


@sqlcode("ADMIN_SHUTDOWN", "57P01")
class AdminShutdown(OperationalError):
    pass


@sqlcode("CRASH_SHUTDOWN", "57P02")
class CrashShutdown(OperationalError):
    pass


@sqlcode("CANNOT_CONNECT_NOW", "57P03")
class CannotConnectNow(OperationalError):
    pass


@sqlcode("DATABASE_DROPPED", "57P04")
class DatabaseDropped(OperationalError):
    pass


@sqlcode("IDLE_SESSION_TIMEOUT", "57P05")
class IdleSessionTimeout(OperationalError):
    pass


# Class 58 - System Error (errors external to PostgreSQL itself)


@sqlcode("SYSTEM_ERROR", "58000")
class SystemError(OperationalError):
    pass


@sqlcode("IO_ERROR", "58030")
class IoError(OperationalError):
    pass


@sqlcode("UNDEFINED_FILE", "58P01")
class UndefinedFile(OperationalError):
    pass


@sqlcode("DUPLICATE_FILE", "58P02")
class DuplicateFile(OperationalError):
    pass


# Class 72 - Snapshot Failure


@sqlcode("SNAPSHOT_TOO_OLD", "72000")
class SnapshotTooOld(DatabaseError):
    pass


# Class F0 - Configuration File Error


@sqlcode("CONFIG_FILE_ERROR", "F0000")
class ConfigFileError(OperationalError):
    pass


@sqlcode("LOCK_FILE_EXISTS", "F0001")
class LockFileExists(OperationalError):
    pass


# Class HV - Foreign Data Wrapper Error (SQL/MED)


@sqlcode("FDW_ERROR", "HV000")
class FdwError(OperationalError):
    pass


@sqlcode("FDW_OUT_OF_MEMORY", "HV001")
class FdwOutOfMemory(OperationalError):
    pass


@sqlcode("FDW_DYNAMIC_PARAMETER_VALUE_NEEDED", "HV002")
class FdwDynamicParameterValueNeeded(OperationalError):
    pass


@sqlcode("FDW_INVALID_DATA_TYPE", "HV004")
class FdwInvalidDataType(OperationalError):
    pass


@sqlcode("FDW_COLUMN_NAME_NOT_FOUND", "HV005")
class FdwColumnNameNotFound(OperationalError):
    pass


@sqlcode("FDW_INVALID_DATA_TYPE_DESCRIPTORS", "HV006")
class FdwInvalidDataTypeDescriptors(OperationalError):
    pass


@sqlcode("FDW_INVALID_COLUMN_NAME", "HV007")
class FdwInvalidColumnName(OperationalError):
    pass


@sqlcode("FDW_INVALID_COLUMN_NUMBER", "HV008")
class FdwInvalidColumnNumber(OperationalError):
    pass


@sqlcode("FDW_INVALID_USE_OF_NULL_POINTER", "HV009")
class FdwInvalidUseOfNullPointer(OperationalError):
    pass


@sqlcode("FDW_INVALID_STRING_FORMAT", "HV00A")
class FdwInvalidStringFormat(OperationalError):
    pass


@sqlcode("FDW_INVALID_HANDLE", "HV00B")
class FdwInvalidHandle(OperationalError):
    pass


@sqlcode("FDW_INVALID_OPTION_INDEX", "HV00C")
class FdwInvalidOptionIndex(OperationalError):
    pass


@sqlcode("FDW_INVALID_OPTION_NAME", "HV00D")
class FdwInvalidOptionName(OperationalError):
    pass


@sqlcode("FDW_OPTION_NAME_NOT_FOUND", "HV00J")
class FdwOptionNameNotFound(OperationalError):
    pass


@sqlcode("FDW_REPLY_HANDLE", "HV00K")
class FdwReplyHandle(OperationalError):
    pass


@sqlcode("FDW_UNABLE_TO_CREATE_EXECUTION", "HV00L")
class FdwUnableToCreateExecution(OperationalError):
    pass


@sqlcode("FDW_UNABLE_TO_CREATE_REPLY", "HV00M")
class FdwUnableToCreateReply(OperationalError):
    pass


@sqlcode("FDW_UNABLE_TO_ESTABLISH_CONNECTION", "HV00N")
class FdwUnableToEstablishConnection(OperationalError):
    pass


@sqlcode("FDW_NO_SCHEMAS", "HV00P")
class FdwNoSchemas(OperationalError):
    pass


@sqlcode("FDW_SCHEMA_NOT_FOUND", "HV00Q")
class FdwSchemaNotFound(OperationalError):
    pass


@sqlcode("FDW_TABLE_NOT_FOUND", "HV00R")
class FdwTableNotFound(OperationalError):
    pass


@sqlcode("FDW_FUNCTION_SEQUENCE_ERROR", "HV010")
class FdwFunctionSequenceError(OperationalError):
    pass


@sqlcode("FDW_TOO_MANY_HANDLES", "HV014")
class FdwTooManyHandles(OperationalError):
    pass


@sqlcode("FDW_INCONSISTENT_DESCRIPTOR_INFORMATION", "HV021")
class FdwInconsistentDescriptorInformation(OperationalError):
    pass


@sqlcode("FDW_INVALID_ATTRIBUTE_VALUE", "HV024")
class FdwInvalidAttributeValue(OperationalError):
    pass


@sqlcode("FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH", "HV090")
class FdwInvalidStringLengthOrBufferLength(OperationalError):
    pass


@sqlcode("FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER", "HV091")
class FdwInvalidDescriptorFieldIdentifier(OperationalError):
    pass


# Class P0 - PL/pgSQL Error


@sqlcode("PLPGSQL_ERROR", "P0000")
class PlpgsqlError(ProgrammingError):
    pass


@sqlcode("RAISE_EXCEPTION", "P0001")
class RaiseException(ProgrammingError):
    pass


@sqlcode("NO_DATA_FOUND", "P0002")
class NoDataFound(ProgrammingError):
    pass


@sqlcode("TOO_MANY_ROWS", "P0003")
class TooManyRows(ProgrammingError):
    pass


@sqlcode("ASSERT_FAILURE", "P0004")
class AssertFailure(ProgrammingError):
    pass


# Class XX - Internal Error


@sqlcode("INTERNAL_ERROR", "XX000")
class InternalError_(InternalError):
    pass


@sqlcode("DATA_CORRUPTED", "XX001")
class DataCorrupted(InternalError):
    pass


@sqlcode("INDEX_CORRUPTED", "XX002")
class IndexCorrupted(InternalError):
    pass


# autogenerated: end
