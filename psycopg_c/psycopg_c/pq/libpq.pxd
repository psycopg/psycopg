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
        char   *keyword
        char   *envvar
        char   *compiled
        char   *val
        char   *label
        char   *dispchar
        int     dispsize

    ctypedef struct PGnotify:
        char   *relname
        int     be_pid
        char   *extra

    ctypedef struct PGcancelConn:
        pass

    ctypedef struct PGcancel:
        pass

    ctypedef struct PGresAttDesc:
        char   *name
        Oid     tableid
        int     columnid
        int     format
        Oid     typid
        int     typlen
        int     atttypmod

    # enums

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
        PGRES_PIPELINE_ABORT
        PGRES_TUPLES_CHUNK

    # 33.1. Database Connection Control Functions
    PGconn *PQconnectdb(const char *conninfo)
    PGconn *PQconnectStart(const char *conninfo)
    PostgresPollingStatusType PQconnectPoll(PGconn *conn) nogil
    PQconninfoOption *PQconndefaults()
    PQconninfoOption *PQconninfo(PGconn *conn)
    PQconninfoOption *PQconninfoParse(const char *conninfo, char **errmsg)
    void PQfinish(PGconn *conn)
    void PQreset(PGconn *conn)
    int PQresetStart(PGconn *conn)
    PostgresPollingStatusType PQresetPoll(PGconn *conn)
    PGPing PQping(const char *conninfo)

    # 33.2. Connection Status Functions
    char *PQdb(const PGconn *conn)
    char *PQuser(const PGconn *conn)
    char *PQpass(const PGconn *conn)
    char *PQhost(const PGconn *conn)
    char *PQhostaddr(const PGconn *conn)
    char *PQport(const PGconn *conn)
    char *PQtty(const PGconn *conn)
    char *PQoptions(const PGconn *conn)
    ConnStatusType PQstatus(const PGconn *conn)
    PGTransactionStatusType PQtransactionStatus(const PGconn *conn)
    const char *PQparameterStatus(const PGconn *conn, const char *paramName)
    int PQprotocolVersion(const PGconn *conn)
    int PQserverVersion(const PGconn *conn)
    char *PQerrorMessage(const PGconn *conn)
    int PQsocket(const PGconn *conn) nogil
    int PQbackendPID(const PGconn *conn)
    int PQconnectionNeedsPassword(const PGconn *conn)
    int PQconnectionUsedPassword(const PGconn *conn)
    int PQsslInUse(PGconn *conn)   # TODO: const in PG 12 docs - verify/report
    # TODO: PQsslAttribute, PQsslAttributeNames, PQsslStruct, PQgetssl

    # 33.3. Command Execution Functions
    PGresult *PQexec(PGconn *conn, const char *command) nogil
    PGresult *PQexecParams(PGconn *conn,
                           const char *command,
                           int nParams,
                           const Oid *paramTypes,
                           const char * const *paramValues,
                           const int *paramLengths,
                           const int *paramFormats,
                           int resultFormat) nogil
    PGresult *PQprepare(PGconn *conn,
                        const char *stmtName,
                        const char *query,
                        int nParams,
                        const Oid *paramTypes) nogil
    PGresult *PQexecPrepared(PGconn *conn,
                             const char *stmtName,
                             int nParams,
                             const char * const *paramValues,
                             const int *paramLengths,
                             const int *paramFormats,
                             int resultFormat) nogil
    PGresult *PQdescribePrepared(PGconn *conn, const char *stmtName) nogil
    PGresult *PQdescribePortal(PGconn *conn, const char *portalName) nogil
    PGresult *PQclosePrepared(PGconn *conn, const char *stmtName) nogil
    PGresult *PQclosePortal(PGconn *conn, const char *portalName) nogil
    ExecStatusType PQresultStatus(const PGresult *res) nogil
    # PQresStatus: not needed, we have pretty enums
    char *PQresultErrorMessage(const PGresult *res) nogil
    # TODO: PQresultVerboseErrorMessage
    char *PQresultErrorField(const PGresult *res, int fieldcode) nogil
    void PQclear(PGresult *res) nogil

    # 33.3.2. Retrieving Query Result Information
    int PQntuples(const PGresult *res)
    int PQnfields(const PGresult *res)
    char *PQfname(const PGresult *res, int column_number)
    int PQfnumber(const PGresult *res, const char *column_name)
    Oid PQftable(const PGresult *res, int column_number)
    int PQftablecol(const PGresult *res, int column_number)
    int PQfformat(const PGresult *res, int column_number)
    Oid PQftype(const PGresult *res, int column_number)
    int PQfmod(const PGresult *res, int column_number)
    int PQfsize(const PGresult *res, int column_number)
    int PQbinaryTuples(const PGresult *res)
    char *PQgetvalue(const PGresult *res, int row_number, int column_number)
    int PQgetisnull(const PGresult *res, int row_number, int column_number)
    int PQgetlength(const PGresult *res, int row_number, int column_number)
    int PQnparams(const PGresult *res)
    Oid PQparamtype(const PGresult *res, int param_number)
    # PQprint: pretty useless

    # 33.3.3. Retrieving Other Result Information
    char *PQcmdStatus(PGresult *res)
    char *PQcmdTuples(PGresult *res)
    Oid PQoidValue(const PGresult *res)

    # 33.3.4. Escaping Strings for Inclusion in SQL Commands
    char *PQescapeIdentifier(PGconn *conn, const char *str, size_t length)
    char *PQescapeLiteral(PGconn *conn, const char *str, size_t length)
    size_t PQescapeStringConn(PGconn *conn,
                              char *to, const char *from_, size_t length,
                              int *error)
    size_t PQescapeString(char *to, const char *from_, size_t length)
    unsigned char *PQescapeByteaConn(PGconn *conn,
                                     const unsigned char *src,
                                     size_t from_length,
                                     size_t *to_length)
    unsigned char *PQescapeBytea(const unsigned char *src,
                                 size_t from_length,
                                 size_t *to_length)
    unsigned char *PQunescapeBytea(const unsigned char *src, size_t *to_length)


    # 33.4. Asynchronous Command Processing
    int PQsendQuery(PGconn *conn, const char *command) nogil
    int PQsendQueryParams(PGconn *conn,
                          const char *command,
                          int nParams,
                          const Oid *paramTypes,
                          const char * const *paramValues,
                          const int *paramLengths,
                          const int *paramFormats,
                          int resultFormat) nogil
    int PQsendPrepare(PGconn *conn,
                      const char *stmtName,
                      const char *query,
                      int nParams,
                      const Oid *paramTypes) nogil
    int PQsendQueryPrepared(PGconn *conn,
                            const char *stmtName,
                            int nParams,
                            const char * const *paramValues,
                            const int *paramLengths,
                            const int *paramFormats,
                            int resultFormat) nogil
    int PQsendDescribePrepared(PGconn *conn, const char *stmtName) nogil
    int PQsendDescribePortal(PGconn *conn, const char *portalName) nogil
    int PQsendClosePrepared(PGconn *conn, const char *stmtName) nogil
    int PQsendClosePortal(PGconn *conn, const char *portalName) nogil
    PGresult *PQgetResult(PGconn *conn) nogil
    int PQconsumeInput(PGconn *conn) nogil
    int PQisBusy(PGconn *conn) nogil
    int PQsetnonblocking(PGconn *conn, int arg) nogil
    int PQisnonblocking(const PGconn *conn)
    int PQflush(PGconn *conn) nogil

    # 32.6. Retrieving Query Results in Chunks
    int PQsetSingleRowMode(PGconn *conn)
    int PQsetChunkedRowsMode(PGconn *conn, int chunkSize)

    # 34.7. Canceling Queries in Progress
    PGcancelConn *PQcancelCreate(PGconn *conn)
    int PQcancelStart(PGcancelConn *cancelConn)
    int PQcancelBlocking(PGcancelConn *cancelConn)
    PostgresPollingStatusType PQcancelPoll(PGcancelConn *cancelConn) nogil
    ConnStatusType PQcancelStatus(const PGcancelConn *cancelConn)
    int PQcancelSocket(PGcancelConn *cancelConn)
    char *PQcancelErrorMessage(const PGcancelConn *cancelConn)
    void PQcancelReset(PGcancelConn *cancelConn)
    void PQcancelFinish(PGcancelConn *cancelConn)
    PGcancel *PQgetCancel(PGconn *conn)
    void PQfreeCancel(PGcancel *cancel)
    int PQcancel(PGcancel *cancel, char *errbuf, int errbufsize)

    # 33.8. Asynchronous Notification
    PGnotify *PQnotifies(PGconn *conn) nogil

    # 33.9. Functions Associated with the COPY Command
    int PQputCopyData(PGconn *conn, const char *buffer, int nbytes) nogil
    int PQputCopyEnd(PGconn *conn, const char *errormsg) nogil
    int PQgetCopyData(PGconn *conn, char **buffer, int async) nogil

    # 33.10. Control Functions
    void PQtrace(PGconn *conn, FILE *stream);
    void PQsetTraceFlags(PGconn *conn, int flags);
    void PQuntrace(PGconn *conn);

    # 33.11. Miscellaneous Functions
    void PQfreemem(void *ptr) nogil
    void PQconninfoFree(PQconninfoOption *connOptions)
    char *PQencryptPasswordConn(
        PGconn *conn, const char *passwd, const char *user, const char *algorithm);
    PGresult *PQchangePassword(PGconn *conn, const char *user, const char *passwd);
    PGresult *PQmakeEmptyPGresult(PGconn *conn, ExecStatusType status)
    int PQsetResultAttrs(PGresult *res, int numAttributes, PGresAttDesc *attDescs)
    int PQlibVersion()

    # 33.12. Notice Processing
    ctypedef void (*PQnoticeReceiver)(void *arg, const PGresult *res)
    PQnoticeReceiver PQsetNoticeReceiver(
        PGconn *conn, PQnoticeReceiver prog, void *arg)

    # 33.18. SSL Support
    void PQinitOpenSSL(int do_ssl, int do_crypto)

    # 34.5 Pipeline Mode

    ctypedef enum PGpipelineStatus:
        PQ_PIPELINE_OFF
        PQ_PIPELINE_ON
        PQ_PIPELINE_ABORTED

    PGpipelineStatus PQpipelineStatus(const PGconn *conn)
    int PQenterPipelineMode(PGconn *conn)
    int PQexitPipelineMode(PGconn *conn)
    int PQpipelineSync(PGconn *conn)
    int PQsendFlushRequest(PGconn *conn)

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
"""
