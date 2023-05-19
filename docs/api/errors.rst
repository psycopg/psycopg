`errors` -- Package exceptions
==============================

.. module:: psycopg.errors

.. index::
    single: Error; Class

This module exposes objects to represent and examine database errors.


.. currentmodule:: psycopg

.. index::
    single: Exceptions; DB-API

.. _dbapi-exceptions:

DB-API exceptions
-----------------

In compliance with the DB-API, all the exceptions raised by Psycopg
derive from the following classes:

.. parsed-literal::

    `!Exception`
    \|__ `Warning`
    \|__ `Error`
        \|__ `InterfaceError`
        \|__ `DatabaseError`
            \|__ `DataError`
            \|__ `OperationalError`
            \|__ `IntegrityError`
            \|__ `InternalError`
            \|__ `ProgrammingError`
            \|__ `NotSupportedError`

These classes are exposed both by this module and the root `psycopg` module.

.. autoexception:: Error()

    .. autoattribute:: diag
    .. autoattribute:: sqlstate

        The code of the error, if received from the server.

        This attribute is also available as class attribute on the
        :ref:`sqlstate-exceptions` classes.

    .. autoattribute:: pgconn

        It has been closed and will be in `~psycopg.pq.ConnStatus.BAD` state;
        however it might be useful to verify precisely what went wrong, for
        instance checking the `~psycopg.pq.PGconn.needs_password` and
        `~psycopg.pq.PGconn.used_password` attributes.
        Attempting to operate this connection will raise an
        :exc:`OperationalError`.

        .. versionadded:: 3.1

    .. autoattribute:: pgresult

        .. versionadded:: 3.1


.. autoexception:: Warning()
.. autoexception:: InterfaceError()
.. autoexception:: DatabaseError()
.. autoexception:: DataError()
.. autoexception:: OperationalError()
.. autoexception:: IntegrityError()
.. autoexception:: InternalError()
.. autoexception:: ProgrammingError()
.. autoexception:: NotSupportedError()


Other Psycopg errors
^^^^^^^^^^^^^^^^^^^^

.. currentmodule:: psycopg.errors


In addition to the standard DB-API errors, Psycopg defines a few more specific
ones.

.. autoexception:: ConnectionTimeout()
.. autoexception:: PipelineAborted()



.. index::
    single: Exceptions; PostgreSQL

Error diagnostics
-----------------

.. autoclass:: Diagnostic()

    The object is available as the `~psycopg.Error`.\ `~psycopg.Error.diag`
    attribute and is passed to the callback functions registered with
    `~psycopg.Connection.add_notice_handler()`.

    All the information available from the :pq:`PQresultErrorField()` function
    are exposed as attributes by the object. For instance the `!severity`
    attribute returns the `!PG_DIAG_SEVERITY` code. Please refer to the
    PostgreSQL documentation for the meaning of all the attributes.

    The attributes available are:

    .. attribute::
        column_name
        constraint_name
        context
        datatype_name
        internal_position
        internal_query
        message_detail
        message_hint
        message_primary
        schema_name
        severity
        severity_nonlocalized
        source_file
        source_function
        source_line
        sqlstate
        statement_position
        table_name

        A string with the error field if available; `!None` if not available.
        The attribute value is available only for errors sent by the server:
        not all the fields are available for all the errors and for all the
        server versions.


.. _sqlstate-exceptions:

SQLSTATE exceptions
-------------------

Errors coming from a database server (as opposite as ones generated
client-side, such as connection failed) usually have a 5-letters error code
called SQLSTATE (available in the `~Diagnostic.sqlstate` attribute of the
error's `~psycopg.Error.diag` attribute).

Psycopg exposes a different class for each SQLSTATE value, allowing to
write idiomatic error handling code according to specific conditions happening
in the database:

.. code-block:: python

    try:
        cur.execute("LOCK TABLE mytable IN ACCESS EXCLUSIVE MODE NOWAIT")
    except psycopg.errors.LockNotAvailable:
        locked = True

The exception names are generated from the PostgreSQL source code and includes
classes for every error defined by PostgreSQL in versions between 9.6 and 15.
Every class in the module is named after what referred as "condition name" `in
the documentation`__, converted to CamelCase: e.g. the error 22012,
``division_by_zero`` is exposed by this module as the class `!DivisionByZero`.
There is a handful of... exceptions to this rule, required for disambiguate
name clashes: please refer to the :ref:`table below <exceptions-list>` for all
the classes defined.

.. __: https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE

Every exception class is a subclass of one of the :ref:`standard DB-API
exception <dbapi-exceptions>`, thus exposing the `~psycopg.Error` interface.

.. versionchanged:: 3.1.4
    Added exceptions introduced in PostgreSQL 15.

.. autofunction:: lookup

    Example: if you have code using constant names or sql codes you can use
    them to look up the exception class.

    .. code-block:: python

        try:
            cur.execute("LOCK TABLE mytable IN ACCESS EXCLUSIVE MODE NOWAIT")
        except psycopg.errors.lookup("UNDEFINED_TABLE"):
            missing = True
        except psycopg.errors.lookup("55P03"):
            locked = True


.. _exceptions-list:

List of known exceptions
^^^^^^^^^^^^^^^^^^^^^^^^

The following are all the SQLSTATE-related error classed defined by this
module, together with the base DBAPI exception they derive from.

.. autogenerated: start

========= ================================================== ====================
SQLSTATE  Exception                                          Base exception
========= ================================================== ====================
**Class 02** - No Data (this is also a warning class per the SQL standard)
---------------------------------------------------------------------------------
``02000`` `!NoData`                                          `!DatabaseError`
``02001`` `!NoAdditionalDynamicResultSetsReturned`           `!DatabaseError`
**Class 03** - SQL Statement Not Yet Complete
---------------------------------------------------------------------------------
``03000`` `!SqlStatementNotYetComplete`                      `!DatabaseError`
**Class 08** - Connection Exception
---------------------------------------------------------------------------------
``08000`` `!ConnectionException`                             `!OperationalError`
``08001`` `!SqlclientUnableToEstablishSqlconnection`         `!OperationalError`
``08003`` `!ConnectionDoesNotExist`                          `!OperationalError`
``08004`` `!SqlserverRejectedEstablishmentOfSqlconnection`   `!OperationalError`
``08006`` `!ConnectionFailure`                               `!OperationalError`
``08007`` `!TransactionResolutionUnknown`                    `!OperationalError`
``08P01`` `!ProtocolViolation`                               `!OperationalError`
**Class 09** - Triggered Action Exception
---------------------------------------------------------------------------------
``09000`` `!TriggeredActionException`                        `!DatabaseError`
**Class 0A** - Feature Not Supported
---------------------------------------------------------------------------------
``0A000`` `!FeatureNotSupported`                             `!NotSupportedError`
**Class 0B** - Invalid Transaction Initiation
---------------------------------------------------------------------------------
``0B000`` `!InvalidTransactionInitiation`                    `!DatabaseError`
**Class 0F** - Locator Exception
---------------------------------------------------------------------------------
``0F000`` `!LocatorException`                                `!DatabaseError`
``0F001`` `!InvalidLocatorSpecification`                     `!DatabaseError`
**Class 0L** - Invalid Grantor
---------------------------------------------------------------------------------
``0L000`` `!InvalidGrantor`                                  `!DatabaseError`
``0LP01`` `!InvalidGrantOperation`                           `!DatabaseError`
**Class 0P** - Invalid Role Specification
---------------------------------------------------------------------------------
``0P000`` `!InvalidRoleSpecification`                        `!DatabaseError`
**Class 0Z** - Diagnostics Exception
---------------------------------------------------------------------------------
``0Z000`` `!DiagnosticsException`                            `!DatabaseError`
``0Z002`` `!StackedDiagnosticsAccessedWithoutActiveHandler`  `!DatabaseError`
**Class 20** - Case Not Found
---------------------------------------------------------------------------------
``20000`` `!CaseNotFound`                                    `!ProgrammingError`
**Class 21** - Cardinality Violation
---------------------------------------------------------------------------------
``21000`` `!CardinalityViolation`                            `!ProgrammingError`
**Class 22** - Data Exception
---------------------------------------------------------------------------------
``22000`` `!DataException`                                   `!DataError`
``22001`` `!StringDataRightTruncation`                       `!DataError`
``22002`` `!NullValueNoIndicatorParameter`                   `!DataError`
``22003`` `!NumericValueOutOfRange`                          `!DataError`
``22004`` `!NullValueNotAllowed`                             `!DataError`
``22005`` `!ErrorInAssignment`                               `!DataError`
``22007`` `!InvalidDatetimeFormat`                           `!DataError`
``22008`` `!DatetimeFieldOverflow`                           `!DataError`
``22009`` `!InvalidTimeZoneDisplacementValue`                `!DataError`
``2200B`` `!EscapeCharacterConflict`                         `!DataError`
``2200C`` `!InvalidUseOfEscapeCharacter`                     `!DataError`
``2200D`` `!InvalidEscapeOctet`                              `!DataError`
``2200F`` `!ZeroLengthCharacterString`                       `!DataError`
``2200G`` `!MostSpecificTypeMismatch`                        `!DataError`
``2200H`` `!SequenceGeneratorLimitExceeded`                  `!DataError`
``2200L`` `!NotAnXmlDocument`                                `!DataError`
``2200M`` `!InvalidXmlDocument`                              `!DataError`
``2200N`` `!InvalidXmlContent`                               `!DataError`
``2200S`` `!InvalidXmlComment`                               `!DataError`
``2200T`` `!InvalidXmlProcessingInstruction`                 `!DataError`
``22010`` `!InvalidIndicatorParameterValue`                  `!DataError`
``22011`` `!SubstringError`                                  `!DataError`
``22012`` `!DivisionByZero`                                  `!DataError`
``22013`` `!InvalidPrecedingOrFollowingSize`                 `!DataError`
``22014`` `!InvalidArgumentForNtileFunction`                 `!DataError`
``22015`` `!IntervalFieldOverflow`                           `!DataError`
``22016`` `!InvalidArgumentForNthValueFunction`              `!DataError`
``22018`` `!InvalidCharacterValueForCast`                    `!DataError`
``22019`` `!InvalidEscapeCharacter`                          `!DataError`
``2201B`` `!InvalidRegularExpression`                        `!DataError`
``2201E`` `!InvalidArgumentForLogarithm`                     `!DataError`
``2201F`` `!InvalidArgumentForPowerFunction`                 `!DataError`
``2201G`` `!InvalidArgumentForWidthBucketFunction`           `!DataError`
``2201W`` `!InvalidRowCountInLimitClause`                    `!DataError`
``2201X`` `!InvalidRowCountInResultOffsetClause`             `!DataError`
``22021`` `!CharacterNotInRepertoire`                        `!DataError`
``22022`` `!IndicatorOverflow`                               `!DataError`
``22023`` `!InvalidParameterValue`                           `!DataError`
``22024`` `!UnterminatedCString`                             `!DataError`
``22025`` `!InvalidEscapeSequence`                           `!DataError`
``22026`` `!StringDataLengthMismatch`                        `!DataError`
``22027`` `!TrimError`                                       `!DataError`
``2202E`` `!ArraySubscriptError`                             `!DataError`
``2202G`` `!InvalidTablesampleRepeat`                        `!DataError`
``2202H`` `!InvalidTablesampleArgument`                      `!DataError`
``22030`` `!DuplicateJsonObjectKeyValue`                     `!DataError`
``22031`` `!InvalidArgumentForSqlJsonDatetimeFunction`       `!DataError`
``22032`` `!InvalidJsonText`                                 `!DataError`
``22033`` `!InvalidSqlJsonSubscript`                         `!DataError`
``22034`` `!MoreThanOneSqlJsonItem`                          `!DataError`
``22035`` `!NoSqlJsonItem`                                   `!DataError`
``22036`` `!NonNumericSqlJsonItem`                           `!DataError`
``22037`` `!NonUniqueKeysInAJsonObject`                      `!DataError`
``22038`` `!SingletonSqlJsonItemRequired`                    `!DataError`
``22039`` `!SqlJsonArrayNotFound`                            `!DataError`
``2203A`` `!SqlJsonMemberNotFound`                           `!DataError`
``2203B`` `!SqlJsonNumberNotFound`                           `!DataError`
``2203C`` `!SqlJsonObjectNotFound`                           `!DataError`
``2203D`` `!TooManyJsonArrayElements`                        `!DataError`
``2203E`` `!TooManyJsonObjectMembers`                        `!DataError`
``2203F`` `!SqlJsonScalarRequired`                           `!DataError`
``2203G`` `!SqlJsonItemCannotBeCastToTargetType`             `!DataError`
``22P01`` `!FloatingPointException`                          `!DataError`
``22P02`` `!InvalidTextRepresentation`                       `!DataError`
``22P03`` `!InvalidBinaryRepresentation`                     `!DataError`
``22P04`` `!BadCopyFileFormat`                               `!DataError`
``22P05`` `!UntranslatableCharacter`                         `!DataError`
``22P06`` `!NonstandardUseOfEscapeCharacter`                 `!DataError`
**Class 23** - Integrity Constraint Violation
---------------------------------------------------------------------------------
``23000`` `!IntegrityConstraintViolation`                    `!IntegrityError`
``23001`` `!RestrictViolation`                               `!IntegrityError`
``23502`` `!NotNullViolation`                                `!IntegrityError`
``23503`` `!ForeignKeyViolation`                             `!IntegrityError`
``23505`` `!UniqueViolation`                                 `!IntegrityError`
``23514`` `!CheckViolation`                                  `!IntegrityError`
``23P01`` `!ExclusionViolation`                              `!IntegrityError`
**Class 24** - Invalid Cursor State
---------------------------------------------------------------------------------
``24000`` `!InvalidCursorState`                              `!InternalError`
**Class 25** - Invalid Transaction State
---------------------------------------------------------------------------------
``25000`` `!InvalidTransactionState`                         `!InternalError`
``25001`` `!ActiveSqlTransaction`                            `!InternalError`
``25002`` `!BranchTransactionAlreadyActive`                  `!InternalError`
``25003`` `!InappropriateAccessModeForBranchTransaction`     `!InternalError`
``25004`` `!InappropriateIsolationLevelForBranchTransaction` `!InternalError`
``25005`` `!NoActiveSqlTransactionForBranchTransaction`      `!InternalError`
``25006`` `!ReadOnlySqlTransaction`                          `!InternalError`
``25007`` `!SchemaAndDataStatementMixingNotSupported`        `!InternalError`
``25008`` `!HeldCursorRequiresSameIsolationLevel`            `!InternalError`
``25P01`` `!NoActiveSqlTransaction`                          `!InternalError`
``25P02`` `!InFailedSqlTransaction`                          `!InternalError`
``25P03`` `!IdleInTransactionSessionTimeout`                 `!InternalError`
**Class 26** - Invalid SQL Statement Name
---------------------------------------------------------------------------------
``26000`` `!InvalidSqlStatementName`                         `!ProgrammingError`
**Class 27** - Triggered Data Change Violation
---------------------------------------------------------------------------------
``27000`` `!TriggeredDataChangeViolation`                    `!OperationalError`
**Class 28** - Invalid Authorization Specification
---------------------------------------------------------------------------------
``28000`` `!InvalidAuthorizationSpecification`               `!OperationalError`
``28P01`` `!InvalidPassword`                                 `!OperationalError`
**Class 2B** - Dependent Privilege Descriptors Still Exist
---------------------------------------------------------------------------------
``2B000`` `!DependentPrivilegeDescriptorsStillExist`         `!InternalError`
``2BP01`` `!DependentObjectsStillExist`                      `!InternalError`
**Class 2D** - Invalid Transaction Termination
---------------------------------------------------------------------------------
``2D000`` `!InvalidTransactionTermination`                   `!InternalError`
**Class 2F** - SQL Routine Exception
---------------------------------------------------------------------------------
``2F000`` `!SqlRoutineException`                             `!OperationalError`
``2F002`` `!ModifyingSqlDataNotPermitted`                    `!OperationalError`
``2F003`` `!ProhibitedSqlStatementAttempted`                 `!OperationalError`
``2F004`` `!ReadingSqlDataNotPermitted`                      `!OperationalError`
``2F005`` `!FunctionExecutedNoReturnStatement`               `!OperationalError`
**Class 34** - Invalid Cursor Name
---------------------------------------------------------------------------------
``34000`` `!InvalidCursorName`                               `!ProgrammingError`
**Class 38** - External Routine Exception
---------------------------------------------------------------------------------
``38000`` `!ExternalRoutineException`                        `!OperationalError`
``38001`` `!ContainingSqlNotPermitted`                       `!OperationalError`
``38002`` `!ModifyingSqlDataNotPermittedExt`                 `!OperationalError`
``38003`` `!ProhibitedSqlStatementAttemptedExt`              `!OperationalError`
``38004`` `!ReadingSqlDataNotPermittedExt`                   `!OperationalError`
**Class 39** - External Routine Invocation Exception
---------------------------------------------------------------------------------
``39000`` `!ExternalRoutineInvocationException`              `!OperationalError`
``39001`` `!InvalidSqlstateReturned`                         `!OperationalError`
``39004`` `!NullValueNotAllowedExt`                          `!OperationalError`
``39P01`` `!TriggerProtocolViolated`                         `!OperationalError`
``39P02`` `!SrfProtocolViolated`                             `!OperationalError`
``39P03`` `!EventTriggerProtocolViolated`                    `!OperationalError`
**Class 3B** - Savepoint Exception
---------------------------------------------------------------------------------
``3B000`` `!SavepointException`                              `!OperationalError`
``3B001`` `!InvalidSavepointSpecification`                   `!OperationalError`
**Class 3D** - Invalid Catalog Name
---------------------------------------------------------------------------------
``3D000`` `!InvalidCatalogName`                              `!ProgrammingError`
**Class 3F** - Invalid Schema Name
---------------------------------------------------------------------------------
``3F000`` `!InvalidSchemaName`                               `!ProgrammingError`
**Class 40** - Transaction Rollback
---------------------------------------------------------------------------------
``40000`` `!TransactionRollback`                             `!OperationalError`
``40001`` `!SerializationFailure`                            `!OperationalError`
``40002`` `!TransactionIntegrityConstraintViolation`         `!OperationalError`
``40003`` `!StatementCompletionUnknown`                      `!OperationalError`
``40P01`` `!DeadlockDetected`                                `!OperationalError`
**Class 42** - Syntax Error or Access Rule Violation
---------------------------------------------------------------------------------
``42000`` `!SyntaxErrorOrAccessRuleViolation`                `!ProgrammingError`
``42501`` `!InsufficientPrivilege`                           `!ProgrammingError`
``42601`` `!SyntaxError`                                     `!ProgrammingError`
``42602`` `!InvalidName`                                     `!ProgrammingError`
``42611`` `!InvalidColumnDefinition`                         `!ProgrammingError`
``42622`` `!NameTooLong`                                     `!ProgrammingError`
``42701`` `!DuplicateColumn`                                 `!ProgrammingError`
``42702`` `!AmbiguousColumn`                                 `!ProgrammingError`
``42703`` `!UndefinedColumn`                                 `!ProgrammingError`
``42704`` `!UndefinedObject`                                 `!ProgrammingError`
``42710`` `!DuplicateObject`                                 `!ProgrammingError`
``42712`` `!DuplicateAlias`                                  `!ProgrammingError`
``42723`` `!DuplicateFunction`                               `!ProgrammingError`
``42725`` `!AmbiguousFunction`                               `!ProgrammingError`
``42803`` `!GroupingError`                                   `!ProgrammingError`
``42804`` `!DatatypeMismatch`                                `!ProgrammingError`
``42809`` `!WrongObjectType`                                 `!ProgrammingError`
``42830`` `!InvalidForeignKey`                               `!ProgrammingError`
``42846`` `!CannotCoerce`                                    `!ProgrammingError`
``42883`` `!UndefinedFunction`                               `!ProgrammingError`
``428C9`` `!GeneratedAlways`                                 `!ProgrammingError`
``42939`` `!ReservedName`                                    `!ProgrammingError`
``42P01`` `!UndefinedTable`                                  `!ProgrammingError`
``42P02`` `!UndefinedParameter`                              `!ProgrammingError`
``42P03`` `!DuplicateCursor`                                 `!ProgrammingError`
``42P04`` `!DuplicateDatabase`                               `!ProgrammingError`
``42P05`` `!DuplicatePreparedStatement`                      `!ProgrammingError`
``42P06`` `!DuplicateSchema`                                 `!ProgrammingError`
``42P07`` `!DuplicateTable`                                  `!ProgrammingError`
``42P08`` `!AmbiguousParameter`                              `!ProgrammingError`
``42P09`` `!AmbiguousAlias`                                  `!ProgrammingError`
``42P10`` `!InvalidColumnReference`                          `!ProgrammingError`
``42P11`` `!InvalidCursorDefinition`                         `!ProgrammingError`
``42P12`` `!InvalidDatabaseDefinition`                       `!ProgrammingError`
``42P13`` `!InvalidFunctionDefinition`                       `!ProgrammingError`
``42P14`` `!InvalidPreparedStatementDefinition`              `!ProgrammingError`
``42P15`` `!InvalidSchemaDefinition`                         `!ProgrammingError`
``42P16`` `!InvalidTableDefinition`                          `!ProgrammingError`
``42P17`` `!InvalidObjectDefinition`                         `!ProgrammingError`
``42P18`` `!IndeterminateDatatype`                           `!ProgrammingError`
``42P19`` `!InvalidRecursion`                                `!ProgrammingError`
``42P20`` `!WindowingError`                                  `!ProgrammingError`
``42P21`` `!CollationMismatch`                               `!ProgrammingError`
``42P22`` `!IndeterminateCollation`                          `!ProgrammingError`
**Class 44** - WITH CHECK OPTION Violation
---------------------------------------------------------------------------------
``44000`` `!WithCheckOptionViolation`                        `!ProgrammingError`
**Class 53** - Insufficient Resources
---------------------------------------------------------------------------------
``53000`` `!InsufficientResources`                           `!OperationalError`
``53100`` `!DiskFull`                                        `!OperationalError`
``53200`` `!OutOfMemory`                                     `!OperationalError`
``53300`` `!TooManyConnections`                              `!OperationalError`
``53400`` `!ConfigurationLimitExceeded`                      `!OperationalError`
**Class 54** - Program Limit Exceeded
---------------------------------------------------------------------------------
``54000`` `!ProgramLimitExceeded`                            `!OperationalError`
``54001`` `!StatementTooComplex`                             `!OperationalError`
``54011`` `!TooManyColumns`                                  `!OperationalError`
``54023`` `!TooManyArguments`                                `!OperationalError`
**Class 55** - Object Not In Prerequisite State
---------------------------------------------------------------------------------
``55000`` `!ObjectNotInPrerequisiteState`                    `!OperationalError`
``55006`` `!ObjectInUse`                                     `!OperationalError`
``55P02`` `!CantChangeRuntimeParam`                          `!OperationalError`
``55P03`` `!LockNotAvailable`                                `!OperationalError`
``55P04`` `!UnsafeNewEnumValueUsage`                         `!OperationalError`
**Class 57** - Operator Intervention
---------------------------------------------------------------------------------
``57000`` `!OperatorIntervention`                            `!OperationalError`
``57014`` `!QueryCanceled`                                   `!OperationalError`
``57P01`` `!AdminShutdown`                                   `!OperationalError`
``57P02`` `!CrashShutdown`                                   `!OperationalError`
``57P03`` `!CannotConnectNow`                                `!OperationalError`
``57P04`` `!DatabaseDropped`                                 `!OperationalError`
``57P05`` `!IdleSessionTimeout`                              `!OperationalError`
**Class 58** - System Error (errors external to PostgreSQL itself)
---------------------------------------------------------------------------------
``58000`` `!SystemError`                                     `!OperationalError`
``58030`` `!IoError`                                         `!OperationalError`
``58P01`` `!UndefinedFile`                                   `!OperationalError`
``58P02`` `!DuplicateFile`                                   `!OperationalError`
**Class 72** - Snapshot Failure
---------------------------------------------------------------------------------
``72000`` `!SnapshotTooOld`                                  `!DatabaseError`
**Class F0** - Configuration File Error
---------------------------------------------------------------------------------
``F0000`` `!ConfigFileError`                                 `!OperationalError`
``F0001`` `!LockFileExists`                                  `!OperationalError`
**Class HV** - Foreign Data Wrapper Error (SQL/MED)
---------------------------------------------------------------------------------
``HV000`` `!FdwError`                                        `!OperationalError`
``HV001`` `!FdwOutOfMemory`                                  `!OperationalError`
``HV002`` `!FdwDynamicParameterValueNeeded`                  `!OperationalError`
``HV004`` `!FdwInvalidDataType`                              `!OperationalError`
``HV005`` `!FdwColumnNameNotFound`                           `!OperationalError`
``HV006`` `!FdwInvalidDataTypeDescriptors`                   `!OperationalError`
``HV007`` `!FdwInvalidColumnName`                            `!OperationalError`
``HV008`` `!FdwInvalidColumnNumber`                          `!OperationalError`
``HV009`` `!FdwInvalidUseOfNullPointer`                      `!OperationalError`
``HV00A`` `!FdwInvalidStringFormat`                          `!OperationalError`
``HV00B`` `!FdwInvalidHandle`                                `!OperationalError`
``HV00C`` `!FdwInvalidOptionIndex`                           `!OperationalError`
``HV00D`` `!FdwInvalidOptionName`                            `!OperationalError`
``HV00J`` `!FdwOptionNameNotFound`                           `!OperationalError`
``HV00K`` `!FdwReplyHandle`                                  `!OperationalError`
``HV00L`` `!FdwUnableToCreateExecution`                      `!OperationalError`
``HV00M`` `!FdwUnableToCreateReply`                          `!OperationalError`
``HV00N`` `!FdwUnableToEstablishConnection`                  `!OperationalError`
``HV00P`` `!FdwNoSchemas`                                    `!OperationalError`
``HV00Q`` `!FdwSchemaNotFound`                               `!OperationalError`
``HV00R`` `!FdwTableNotFound`                                `!OperationalError`
``HV010`` `!FdwFunctionSequenceError`                        `!OperationalError`
``HV014`` `!FdwTooManyHandles`                               `!OperationalError`
``HV021`` `!FdwInconsistentDescriptorInformation`            `!OperationalError`
``HV024`` `!FdwInvalidAttributeValue`                        `!OperationalError`
``HV090`` `!FdwInvalidStringLengthOrBufferLength`            `!OperationalError`
``HV091`` `!FdwInvalidDescriptorFieldIdentifier`             `!OperationalError`
**Class P0** - PL/pgSQL Error
---------------------------------------------------------------------------------
``P0000`` `!PlpgsqlError`                                    `!ProgrammingError`
``P0001`` `!RaiseException`                                  `!ProgrammingError`
``P0002`` `!NoDataFound`                                     `!ProgrammingError`
``P0003`` `!TooManyRows`                                     `!ProgrammingError`
``P0004`` `!AssertFailure`                                   `!ProgrammingError`
**Class XX** - Internal Error
---------------------------------------------------------------------------------
``XX000`` `!InternalError_`                                  `!InternalError`
``XX001`` `!DataCorrupted`                                   `!InternalError`
``XX002`` `!IndexCorrupted`                                  `!InternalError`
========= ================================================== ====================

.. autogenerated: end

.. versionadded:: 3.1.4
    Exception `!SqlJsonItemCannotBeCastToTargetType`, introduced in PostgreSQL
    15.
