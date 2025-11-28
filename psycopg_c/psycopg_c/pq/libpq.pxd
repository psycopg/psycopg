"""
Libpq header definition for the cython psycopg.pq implementation.
"""

# Copyright (C) 2020 The Psycopg Team

cdef extern from "stdio.h":

    ctypedef struct FILE:
        pass

cdef extern from "pg_config.h":

    int PG_VERSION_NUM


cdef extern from "libpq-fe.h":

    # structures and types

    ctypedef unsigned int Oid

    ctypedef struct PGconn:
        pass

    ctypedef struct PGresult:
        pass

    ctypedef struct PQconninfoOption:
        char *keyword
        char *envvar
        char *compiled
        char *val
        char *label
        char *dispchar
        int dispsize

    ctypedef struct PGnotify:
        char *relname
        int be_pid
        char *extra

    ctypedef struct PGcancelConn:
        pass

    ctypedef struct PGcancel:
        pass

    ctypedef struct PGresAttDesc:
        char *name
        Oid tableid
        int columnid
        int format
        Oid typid
        int typlen
        int atttypmod

    # enums

    # Check in src/interfaces/libpq/libpq-fe.h for updates.

    ctypedef enum PostgresPollingStatusType:
        PGRES_POLLING_FAILED = 0
        PGRES_POLLING_READING
        PGRES_POLLING_WRITING
        PGRES_POLLING_OK
        PGRES_POLLING_ACTIVE

    ctypedef enum PGPing:
        PQPING_OK
        PQPING_REJECT
        PQPING_NO_RESPONSE
        PQPING_NO_ATTEMPT

    ctypedef enum ConnStatusType:
        CONNECTION_OK
        CONNECTION_BAD
        CONNECTION_STARTED
        CONNECTION_MADE
        CONNECTION_AWAITING_RESPONSE
        CONNECTION_AUTH_OK
        CONNECTION_SETENV
        CONNECTION_SSL_STARTUP
        CONNECTION_NEEDED
        CONNECTION_CHECK_WRITABLE
        CONNECTION_CONSUME
        CONNECTION_GSS_STARTUP
        CONNECTION_CHECK_TARGET
        CONNECTION_CHECK_STANDBY
        CONNECTION_ALLOCATED
        CONNECTION_AUTHENTICATING

    ctypedef enum PGTransactionStatusType:
        PQTRANS_IDLE
        PQTRANS_ACTIVE
        PQTRANS_INTRANS
        PQTRANS_INERROR
        PQTRANS_UNKNOWN

    ctypedef enum ExecStatusType:
        PGRES_EMPTY_QUERY = 0
        PGRES_COMMAND_OK
        PGRES_TUPLES_OK
        PGRES_COPY_OUT
        PGRES_COPY_IN
        PGRES_BAD_RESPONSE
        PGRES_NONFATAL_ERROR
        PGRES_FATAL_ERROR
        PGRES_COPY_BOTH
        PGRES_SINGLE_TUPLE
        PGRES_PIPELINE_SYNC
        PGRES_PIPELINE_ABORTED
        PGRES_TUPLES_CHUNK

    # 33.1. Database Connection Control Functions
    PGconn *PQconnectdb(const char *conninfo) noexcept nogil
    PGconn *PQconnectStart(const char *conninfo) noexcept nogil
    PostgresPollingStatusType PQconnectPoll(PGconn *conn) noexcept nogil
    PQconninfoOption *PQconndefaults() noexcept nogil
    PQconninfoOption *PQconninfo(PGconn *conn) noexcept nogil
    PQconninfoOption *PQconninfoParse(const char *conninfo,
                                      char **errmsg) noexcept nogil
    void PQfinish(PGconn *conn) noexcept nogil
    void PQreset(PGconn *conn) noexcept nogil
    int PQresetStart(PGconn *conn) noexcept nogil
    PostgresPollingStatusType PQresetPoll(PGconn *conn) noexcept nogil
    PGPing PQping(const char *conninfo) noexcept nogil

    # 33.2. Connection Status Functions
    char *PQdb(const PGconn *conn) noexcept nogil
    char *PQuser(const PGconn *conn) noexcept nogil
    char *PQpass(const PGconn *conn) noexcept nogil
    char *PQhost(const PGconn *conn) noexcept nogil
    char *PQhostaddr(const PGconn *conn) noexcept nogil
    char *PQport(const PGconn *conn) noexcept nogil
    char *PQtty(const PGconn *conn) noexcept nogil
    char *PQoptions(const PGconn *conn) noexcept nogil
    ConnStatusType PQstatus(const PGconn *conn) noexcept nogil
    PGTransactionStatusType PQtransactionStatus(const PGconn *conn) noexcept nogil
    const char *PQparameterStatus(const PGconn *conn,
                                  const char *paramName) noexcept nogil
    int PQprotocolVersion(const PGconn *conn) noexcept nogil
    int PQfullProtocolVersion(const PGconn *conn) noexcept nogil
    int PQserverVersion(const PGconn *conn) noexcept nogil
    char *PQerrorMessage(const PGconn *conn) noexcept nogil
    int PQsocket(const PGconn *conn) noexcept nogil
    int PQbackendPID(const PGconn *conn) noexcept nogil
    int PQconnectionNeedsPassword(const PGconn *conn) noexcept nogil
    int PQconnectionUsedPassword(const PGconn *conn) noexcept nogil
    int PQconnectionUsedGSSAPI(const PGconn *conn) noexcept nogil
    # TODO PQsslInUse: const in PG 12 docs - verify/report
    int PQsslInUse(PGconn *conn) noexcept nogil
    # TODO: PQsslAttribute, PQsslAttributeNames, PQsslStruct, PQgetssl

    # 33.3. Command Execution Functions
    PGresult *PQexec(PGconn *conn, const char *command) noexcept nogil
    PGresult *PQexecParams(PGconn *conn,
                           const char *command,
                           int nParams,
                           const Oid *paramTypes,
                           const char * const *paramValues,
                           const int *paramLengths,
                           const int *paramFormats,
                           int resultFormat) noexcept nogil
    PGresult *PQprepare(PGconn *conn,
                        const char *stmtName,
                        const char *query,
                        int nParams,
                        const Oid *paramTypes) noexcept nogil
    PGresult *PQexecPrepared(PGconn *conn,
                             const char *stmtName,
                             int nParams,
                             const char * const *paramValues,
                             const int *paramLengths,
                             const int *paramFormats,
                             int resultFormat) noexcept nogil
    PGresult *PQdescribePrepared(PGconn *conn, const char *stmtName) noexcept nogil
    PGresult *PQdescribePortal(PGconn *conn, const char *portalName) noexcept nogil
    PGresult *PQclosePrepared(PGconn *conn, const char *stmtName) noexcept nogil
    PGresult *PQclosePortal(PGconn *conn, const char *portalName) noexcept nogil
    ExecStatusType PQresultStatus(const PGresult *res) noexcept nogil
    # PQresStatus: not needed, we have pretty enums
    char *PQresultErrorMessage(const PGresult *res) noexcept nogil
    # TODO: PQresultVerboseErrorMessage
    char *PQresultErrorField(const PGresult *res, int fieldcode) noexcept nogil
    void PQclear(PGresult *res) noexcept nogil

    # 33.3.2. Retrieving Query Result Information
    int PQntuples(const PGresult *res) noexcept nogil
    int PQnfields(const PGresult *res) noexcept nogil
    char *PQfname(const PGresult *res, int column_number) noexcept nogil
    int PQfnumber(const PGresult *res, const char *column_name) noexcept nogil
    Oid PQftable(const PGresult *res, int column_number) noexcept nogil
    int PQftablecol(const PGresult *res, int column_number) noexcept nogil
    int PQfformat(const PGresult *res, int column_number) noexcept nogil
    Oid PQftype(const PGresult *res, int column_number) noexcept nogil
    int PQfmod(const PGresult *res, int column_number) noexcept nogil
    int PQfsize(const PGresult *res, int column_number) noexcept nogil
    int PQbinaryTuples(const PGresult *res) noexcept nogil
    char *PQgetvalue(const PGresult *res,
                     int row_number,
                     int column_number) noexcept nogil
    int PQgetisnull(const PGresult *res,
                    int row_number,
                    int column_number) noexcept nogil
    int PQgetlength(const PGresult *res,
                    int row_number,
                    int column_number) noexcept nogil
    int PQnparams(const PGresult *res) noexcept nogil
    Oid PQparamtype(const PGresult *res, int param_number) noexcept nogil
    # PQprint: pretty useless

    # 33.3.3. Retrieving Other Result Information
    char *PQcmdStatus(PGresult *res) noexcept nogil
    char *PQcmdTuples(PGresult *res) noexcept nogil
    Oid PQoidValue(const PGresult *res) noexcept nogil

    # 33.3.4. Escaping Strings for Inclusion in SQL Commands
    char *PQescapeIdentifier(PGconn *conn,
                             const char *str,
                             size_t length) noexcept nogil
    char *PQescapeLiteral(PGconn *conn, const char *str, size_t length) noexcept nogil
    size_t PQescapeStringConn(PGconn *conn,
                              char *to, const char *from_, size_t length,
                              int *error) noexcept nogil
    size_t PQescapeString(char *to, const char *from_, size_t length) noexcept nogil
    unsigned char *PQescapeByteaConn(PGconn *conn,
                                     const unsigned char *src,
                                     size_t from_length,
                                     size_t *to_length) noexcept nogil
    unsigned char *PQescapeBytea(const unsigned char *src,
                                 size_t from_length,
                                 size_t *to_length) noexcept nogil
    unsigned char *PQunescapeBytea(const unsigned char *src,
                                   size_t *to_length) noexcept nogil

    # 33.4. Asynchronous Command Processing
    int PQsendQuery(PGconn *conn, const char *command) noexcept nogil
    int PQsendQueryParams(PGconn *conn,
                          const char *command,
                          int nParams,
                          const Oid *paramTypes,
                          const char * const *paramValues,
                          const int *paramLengths,
                          const int *paramFormats,
                          int resultFormat) noexcept nogil
    int PQsendPrepare(PGconn *conn,
                      const char *stmtName,
                      const char *query,
                      int nParams,
                      const Oid *paramTypes) noexcept nogil
    int PQsendQueryPrepared(PGconn *conn,
                            const char *stmtName,
                            int nParams,
                            const char * const *paramValues,
                            const int *paramLengths,
                            const int *paramFormats,
                            int resultFormat) noexcept nogil
    int PQsendDescribePrepared(PGconn *conn, const char *stmtName) noexcept nogil
    int PQsendDescribePortal(PGconn *conn, const char *portalName) noexcept nogil
    int PQsendClosePrepared(PGconn *conn, const char *stmtName) noexcept nogil
    int PQsendClosePortal(PGconn *conn, const char *portalName) noexcept nogil
    PGresult *PQgetResult(PGconn *conn) noexcept nogil
    int PQconsumeInput(PGconn *conn) noexcept nogil
    int PQisBusy(PGconn *conn) noexcept nogil
    int PQsetnonblocking(PGconn *conn, int arg) noexcept nogil
    int PQisnonblocking(const PGconn *conn) noexcept nogil
    int PQflush(PGconn *conn) noexcept nogil

    # 32.6. Retrieving Query Results in Chunks
    int PQsetSingleRowMode(PGconn *conn) noexcept nogil
    int PQsetChunkedRowsMode(PGconn *conn, int chunkSize) noexcept nogil

    # 34.7. Canceling Queries in Progress
    PGcancelConn *PQcancelCreate(PGconn *conn) noexcept nogil
    int PQcancelStart(PGcancelConn *cancelConn) noexcept nogil
    int PQcancelBlocking(PGcancelConn *cancelConn) noexcept nogil
    PostgresPollingStatusType PQcancelPoll(PGcancelConn *cancelConn) noexcept nogil
    ConnStatusType PQcancelStatus(const PGcancelConn *cancelConn) noexcept nogil
    int PQcancelSocket(PGcancelConn *cancelConn) noexcept nogil
    char *PQcancelErrorMessage(const PGcancelConn *cancelConn) noexcept nogil
    void PQcancelReset(PGcancelConn *cancelConn) noexcept nogil
    void PQcancelFinish(PGcancelConn *cancelConn) noexcept nogil
    PGcancel *PQgetCancel(PGconn *conn) noexcept nogil
    void PQfreeCancel(PGcancel *cancel) noexcept nogil
    int PQcancel(PGcancel *cancel, char *errbuf, int errbufsize) noexcept nogil

    # 33.8. Asynchronous Notification
    PGnotify *PQnotifies(PGconn *conn) noexcept nogil

    # 33.9. Functions Associated with the COPY Command
    int PQputCopyData(PGconn *conn, const char *buffer, int nbytes) noexcept nogil
    int PQputCopyEnd(PGconn *conn, const char *errormsg) noexcept nogil
    int PQgetCopyData(PGconn *conn, char **buffer, int async) noexcept nogil

    # 33.10. Control Functions
    void PQtrace(PGconn *conn, FILE *stream) noexcept nogil
    void PQsetTraceFlags(PGconn *conn, int flags) noexcept nogil
    void PQuntrace(PGconn *conn) noexcept nogil

    # 33.11. Miscellaneous Functions
    void PQfreemem(void *ptr) noexcept nogil
    void PQconninfoFree(PQconninfoOption *connOptions) noexcept nogil
    char *PQencryptPasswordConn(PGconn *conn,
                                const char *passwd,
                                const char *user,
                                const char *algorithm) noexcept nogil
    PGresult *PQchangePassword(PGconn *conn,
                               const char *user,
                               const char *passwd) noexcept nogil
    PGresult *PQmakeEmptyPGresult(PGconn *conn, ExecStatusType status) noexcept nogil
    int PQsetResultAttrs(PGresult *res,
                         int numAttributes,
                         PGresAttDesc *attDescs) noexcept nogil
    int PQlibVersion() noexcept nogil

    # 33.12. Notice Processing
    ctypedef void (*PQnoticeReceiver)(void *arg, const PGresult *res)
    PQnoticeReceiver PQsetNoticeReceiver(
        PGconn *conn, PQnoticeReceiver prog, void *arg) noexcept nogil

    # 33.18. SSL Support
    void PQinitOpenSSL(int do_ssl, int do_crypto) noexcept nogil

    # 34.5 Pipeline Mode

    ctypedef enum PGpipelineStatus:
        PQ_PIPELINE_OFF
        PQ_PIPELINE_ON
        PQ_PIPELINE_ABORTED

    PGpipelineStatus PQpipelineStatus(const PGconn *conn) noexcept nogil
    int PQenterPipelineMode(PGconn *conn) noexcept nogil
    int PQexitPipelineMode(PGconn *conn) noexcept nogil
    int PQpipelineSync(PGconn *conn) noexcept nogil
    int PQsendFlushRequest(PGconn *conn) noexcept nogil

cdef extern from *:
    """
/* Hack to allow the use of old libpq versions */
#if PG_VERSION_NUM < 100000
#define PQencryptPasswordConn(conn, passwd, user, algorithm) NULL
#endif

#if PG_VERSION_NUM < 120000
#define PQhostaddr(conn) NULL
#endif

#if PG_VERSION_NUM < 140000
#define PGRES_PIPELINE_SYNC 10
#define PGRES_PIPELINE_ABORTED 11
typedef enum {
    PQ_PIPELINE_OFF,
    PQ_PIPELINE_ON,
    PQ_PIPELINE_ABORTED
} PGpipelineStatus;
#define PQpipelineStatus(conn) PQ_PIPELINE_OFF
#define PQenterPipelineMode(conn) 0
#define PQexitPipelineMode(conn) 1
#define PQpipelineSync(conn) 0
#define PQsendFlushRequest(conn) 0
#define PQsetTraceFlags(conn, stream) do {} while (0)
#endif

#if PG_VERSION_NUM < 160000
#define PQconnectionUsedGSSAPI(conn) 0
#endif

#if PG_VERSION_NUM < 170000
typedef struct pg_cancel_conn PGcancelConn;
#define PQchangePassword(conn, user, passwd) NULL
#define PQclosePrepared(conn, name) NULL
#define PQclosePortal(conn, name) NULL
#define PQsendClosePrepared(conn, name) 0
#define PQsendClosePortal(conn, name) 0
#define PQcancelCreate(conn) NULL
#define PQcancelStart(cancelConn) 0
#define PQcancelBlocking(cancelConn) 0
#define PQcancelPoll(cancelConn) CONNECTION_OK
#define PQcancelStatus(cancelConn) 0
#define PQcancelSocket(cancelConn) -1
#define PQcancelErrorMessage(cancelConn) NULL
#define PQcancelReset(cancelConn) 0
#define PQcancelFinish(cancelConn) 0
#define PQsetChunkedRowsMode(conn, chunkSize) 0
#endif

#if PG_VERSION_NUM < 180000
#define PQfullProtocolVersion(conn) 0
#endif
"""
