"""
C implementation of waiting functions
"""

# Copyright (C) 2022 The Psycopg Team

from cpython.object cimport PyObject

from os import fstat, strerror
from time import monotonic
from threading import current_thread, main_thread
from typing import TypeVar

from psycopg import errors as e
from psycopg._encodings import conninfo_encoding

RV = TypeVar("RV")


cdef extern from *:
    r"""
#if defined(HAVE_POLL) && !defined(HAVE_BROKEN_POLL)

#if defined(HAVE_POLL_H)
#include <poll.h>
#elif defined(HAVE_SYS_POLL_H)
#include <sys/poll.h>
#endif

#else  /* no poll available */

#ifdef MS_WINDOWS
#include <winsock2.h>
#else
#include <sys/select.h>
#endif

#endif  /* HAVE_POLL */

#define SELECT_EV_READ 1
#define SELECT_EV_WRITE 2
#define CWAIT_SELECT_ERROR -1
#define CWAIT_SOCKET_ERROR -2

#define SEC_TO_MS 1000
#define SEC_TO_US (1000 * 1000)

/* Use select to wait for readiness on fileno.
 *
 * - Return SELECT_EV_* if the file is ready
 * - Return SELECT_EV_NONE on timeout
 * - Return CWAIT_SELECT_ERROR (and set an exception) on error.
 * - Return CWAIT_SOCKET_ERROR on poll success but fd error.
 *
 * The wisdom of this function comes from:
 *
 * - PostgreSQL libpq (see src/interfaces/libpq/fe-misc.c)
 * - Python select module (see Modules/selectmodule.c)
 */
static int
wait_c_impl(int fileno, int wait, float timeout)
{
    int select_rv;
    int rv = CWAIT_SELECT_ERROR;

#if defined(HAVE_POLL) && !defined(HAVE_BROKEN_POLL)

    struct pollfd input_fd;
    int timeout_ms;

    input_fd.fd = fileno;
    input_fd.events = POLLERR;
    input_fd.revents = 0;

    if (wait & SELECT_EV_READ) { input_fd.events |= POLLIN; }
    if (wait & SELECT_EV_WRITE) { input_fd.events |= POLLOUT; }

    if (timeout < 0.0) {
        timeout_ms = -1;
    } else {
        timeout_ms = (int)(timeout * SEC_TO_MS);
    }

retry_eintr:

    Py_BEGIN_ALLOW_THREADS
    errno = 0;
    select_rv = poll(&input_fd, 1, timeout_ms);
    Py_END_ALLOW_THREADS

    /* The grace of PEP 475 */
    if (errno == EINTR) {
        goto retry_eintr;
    }

    if (PyErr_CheckSignals()) { goto finally; }
    if (select_rv < 0) { goto error; }  /* poll error */

    rv = 0;  /* success, maybe with timeout */
    if (select_rv >= 0) {
        if (input_fd.revents & POLLIN) { rv |= SELECT_EV_READ; }
        if (input_fd.revents & POLLOUT) { rv |= SELECT_EV_WRITE; }
        if (!rv && (input_fd.revents & ~(POLLIN | POLLOUT))) {
            rv = CWAIT_SOCKET_ERROR;
        }
    }

#else

    fd_set ifds;
    fd_set ofds;
    fd_set efds;
    struct timeval tv, *tvptr;

#ifndef MS_WINDOWS
    if (fileno >= 1024) {
        PyErr_SetString(
            PyExc_ValueError,  /* same exception of Python's 'select.select()' */
            "connection file descriptor out of range for 'select()'");
        return CWAIT_SELECT_ERROR;
    }
#endif

    FD_ZERO(&ifds);
    FD_ZERO(&ofds);
    FD_ZERO(&efds);

    if (wait & SELECT_EV_READ) { FD_SET(fileno, &ifds); }
    if (wait & SELECT_EV_WRITE) { FD_SET(fileno, &ofds); }
    FD_SET(fileno, &efds);

    /* Compute appropriate timeout interval */
    if (timeout < 0.0) {
        tvptr = NULL;
    }
    else {
        tv.tv_sec = (int)timeout;
        tv.tv_usec = (int)((long)(timeout * SEC_TO_US) % SEC_TO_US);
        tvptr = &tv;
    }

retry_eintr:

    Py_BEGIN_ALLOW_THREADS
    errno = 0;
    select_rv = select(fileno + 1, &ifds, &ofds, &efds, tvptr);
    Py_END_ALLOW_THREADS

    /* The grace of PEP 475 */
    if (errno == EINTR) {
        goto retry_eintr;
    }

    if (PyErr_CheckSignals()) { goto finally; }
    if (select_rv < 0) { goto error; }  /* select error */

    rv = 0;
    if (select_rv > 0) {
        if (!FD_ISSET(fileno, &efds)) {
            if (FD_ISSET(fileno, &ifds)) { rv |= SELECT_EV_READ; }
            if (FD_ISSET(fileno, &ofds)) { rv |= SELECT_EV_WRITE; }
        }
        else {
            /* There is an error on the FD. Assume it means it is closed. We
             * want to raise a chained exception, which is tricky in C, so
             * return the special value CWAIT_SOCKET_ERROR to signal the Cython
             * wrapper to check the fd and raise the appropriate exception.
             */
            rv = CWAIT_SOCKET_ERROR;
        }
    }

#endif  /* HAVE_POLL */

    return rv;

error:

    rv = CWAIT_SELECT_ERROR;

#ifdef MS_WINDOWS
    if (select_rv == SOCKET_ERROR) {
        PyErr_SetExcFromWindowsErr(PyExc_OSError, WSAGetLastError());
    }
#else
    if (select_rv < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
    }
#endif
    else {
        PyErr_SetString(PyExc_OSError, "unexpected error from select()");
    }

finally:

    return rv;

}

/* Return the SELECT_EV_* events fileno is ready for, without waiting.
 *
 * - Return 0 if not ready, or if the fd is in error: the caller will find
 *   out the error once it waits on it.
 * - Return CWAIT_SELECT_ERROR (and set an exception) on error.
 *
 * Unlike wait_c_impl(), don't release the GIL: the call doesn't block.
 */
static int
ready_c_impl(int fileno, int wait)
{
    int select_rv;
    int rv = 0;

#if defined(HAVE_POLL) && !defined(HAVE_BROKEN_POLL)

    struct pollfd input_fd;

    input_fd.fd = fileno;
    input_fd.events = 0;
    input_fd.revents = 0;

    if (wait & SELECT_EV_READ) { input_fd.events |= POLLIN; }
    if (wait & SELECT_EV_WRITE) { input_fd.events |= POLLOUT; }

    do {
        errno = 0;
        select_rv = poll(&input_fd, 1, 0);
    } while (select_rv < 0 && errno == EINTR);

    if (select_rv < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return CWAIT_SELECT_ERROR;
    }

    if (input_fd.revents & POLLIN) { rv |= SELECT_EV_READ; }
    if (input_fd.revents & POLLOUT) { rv |= SELECT_EV_WRITE; }

#else

    fd_set ifds;
    fd_set ofds;
    fd_set efds;
    struct timeval tv = {0, 0};

#ifndef MS_WINDOWS
    if (fileno >= 1024) {
        PyErr_SetString(
            PyExc_ValueError,  /* same exception of Python's 'select.select()' */
            "connection file descriptor out of range for 'select()'");
        return CWAIT_SELECT_ERROR;
    }
#endif

    FD_ZERO(&ifds);
    FD_ZERO(&ofds);
    FD_ZERO(&efds);

    if (wait & SELECT_EV_READ) { FD_SET(fileno, &ifds); }
    if (wait & SELECT_EV_WRITE) { FD_SET(fileno, &ofds); }
    FD_SET(fileno, &efds);

    do {
        errno = 0;
        select_rv = select(fileno + 1, &ifds, &ofds, &efds, &tv);
    } while (select_rv < 0 && errno == EINTR);

    if (select_rv < 0) {
#ifdef MS_WINDOWS
        PyErr_SetExcFromWindowsErr(PyExc_OSError, WSAGetLastError());
#else
        PyErr_SetFromErrno(PyExc_OSError);
#endif
        return CWAIT_SELECT_ERROR;
    }

    if (select_rv > 0 && !FD_ISSET(fileno, &efds)) {
        if (FD_ISSET(fileno, &ifds)) { rv |= SELECT_EV_READ; }
        if (FD_ISSET(fileno, &ofds)) { rv |= SELECT_EV_WRITE; }
    }

#endif  /* HAVE_POLL */

    return rv;
}
    """
    cdef int wait_c_impl(int fileno, int wait, float timeout) except -1
    cdef int ready_c_impl(int fileno, int wait) except -1
    cdef int CWAIT_SOCKET_ERROR


cdef extern from "Python.h":
    # Resume a generator returning its return value, rather than raising
    # StopIteration, which is costly to catch. Return PYGEN_RETURN or
    # PYGEN_NEXT (PYGEN_ERROR = -1 if an exception is raised).
    int PyIter_Send(object iter, object arg, PyObject **presult) except -1
    int PYGEN_RETURN
    void Py_DecRef(PyObject *o)
    int PyErr_CheckSignals() except -1


cdef inline object _steal(PyObject *o):
    """Return a new reference as a Python object, without increfing it."""
    rv = <object>o
    Py_DecRef(o)
    return rv


def wait_c(gen: PQGen[RV], int fileno, interval = 0.0, timeout = None) -> RV:
    """
    Wait for a generator using poll or select.
    """
    cdef float cinterval, ctimeout
    cdef double deadline = 0.0, remaining
    cdef bint has_deadline = False
    cdef int wait, ready
    cdef object pyready
    cdef PyObject *pyrv

    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    cinterval = <float>float(interval)
    if cinterval < 0.0:
        cinterval = 0.0

    if timeout is not None:
        # Note: use a double: a float would lose precision on large values.
        has_deadline = True
        deadline = monotonic() + float(timeout)

    # Most of the generators complete without blocking (e.g. at every copy()
    # row): return their value as quickly as possible.
    if PyIter_Send(gen, None, &pyrv) == PYGEN_RETURN:
        return _steal(pyrv)
    wait = _steal(pyrv)

    while True:
        ctimeout = cinterval
        if has_deadline:
            remaining = deadline - monotonic()
            if remaining < ctimeout:
                ctimeout = <float>remaining if remaining > 0.0 else 0.0

        ready = wait_c_impl(fileno, wait, ctimeout)
        if ready == READY_NONE:
            pyready = PY_READY_NONE
        elif ready == READY_R:
            pyready = PY_READY_R
        elif ready == READY_RW:
            pyready = PY_READY_RW
        elif ready == READY_W:
            pyready = PY_READY_W
        elif ready == CWAIT_SOCKET_ERROR:  # FD closed?
            try:
                fstat(fileno)
            except Exception as ex:
                raise e.OperationalError("connection socket closed") from ex
            else:
                raise e.OperationalError("connection socket closed")
        else:
            raise AssertionError(f"unexpected ready value: {ready}")

        if PyIter_Send(gen, pyready, &pyrv) == PYGEN_RETURN:
            return _steal(pyrv)
        wait = _steal(pyrv)

        # Check the deadline after resuming the generator, so that the data
        # already available is processed and a timeout of 0 polls once.
        if has_deadline and monotonic() >= deadline:
            raise e._WaitTimeout("wait timeout expired")


def ready_c(int fileno, int wait):
    """
    Return the `Ready` state of `!fileno` right now, without waiting.

    Used by `wait_async()` to check if the socket is ready before going
    through the event loop.
    """
    cdef int ready = ready_c_impl(fileno, wait)
    if ready == READY_NONE:
        return PY_READY_NONE
    elif ready == READY_W:
        return PY_READY_W
    elif ready == READY_RW:
        return PY_READY_RW
    elif ready == READY_R:
        return PY_READY_R
    else:
        raise AssertionError(f"unexpected ready value: {ready}")


cdef extern from *:
    r"""
#include <libpq-fe.h>

#ifndef MS_WINDOWS
#include <errno.h>
#include <time.h>
#endif

#define CN_OK 0
#define CN_FAILED 1
#define CN_TIMEOUT 2
#define CN_INTERRUPT 3    /* check for signals, then call again */
#define CN_WAIT_ERROR 4   /* errno, or WSA error on Windows, in *err */
#define CN_FD_RANGE 5
#define CN_UNEXPECTED 6

#ifdef MS_WINDOWS
#define CN_ERR_IS_WSA 1
#else
#define CN_ERR_IS_WSA 0
#endif

/* PGRES_POLLING_ACTIVE is unused by libpq: use it for "call PQconnectPoll()" */
#define CN_NEED_POLL PGRES_POLLING_ACTIVE

static double
cn_now(void)
{
#ifdef MS_WINDOWS
    static LARGE_INTEGER freq = {0};
    LARGE_INTEGER count;
    if (!freq.QuadPart) { QueryPerformanceFrequency(&freq); }
    QueryPerformanceCounter(&count);
    return (double)count.QuadPart / (double)freq.QuadPart;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
#endif
}

/* Like wait_c_impl(), but callable without the GIL.
 *
 * Return > 0 if ready, 0 on timeout, a negated CN_* value on error.
 */
static int
cn_wait(int fileno, int reading, int timeout_ms, int *err)
{
    int rv;

#if defined(HAVE_POLL) && !defined(HAVE_BROKEN_POLL)

    struct pollfd pfd;
    pfd.fd = fileno;
    pfd.events = reading ? POLLIN : POLLOUT;
    pfd.revents = 0;

    errno = 0;
    rv = poll(&pfd, 1, timeout_ms);
    if (rv < 0) {
        if (errno == EINTR) { return -CN_INTERRUPT; }
        *err = errno;
        return -CN_WAIT_ERROR;
    }
    return rv;

#else

    fd_set ifds;
    fd_set ofds;
    fd_set efds;
    struct timeval tv;

#ifndef MS_WINDOWS
    if (fileno >= FD_SETSIZE) { return -CN_FD_RANGE; }
#endif

    FD_ZERO(&ifds);
    FD_ZERO(&ofds);
    FD_ZERO(&efds);
    if (reading) { FD_SET(fileno, &ifds); } else { FD_SET(fileno, &ofds); }
    FD_SET(fileno, &efds);

    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

#ifdef MS_WINDOWS
    rv = select(0, &ifds, &ofds, &efds, &tv);
    if (rv == SOCKET_ERROR) {
        *err = WSAGetLastError();
        return -CN_WAIT_ERROR;
    }
#else
    errno = 0;
    rv = select(fileno + 1, &ifds, &ofds, &efds, &tv);
    if (rv < 0) {
        if (errno == EINTR) { return -CN_INTERRUPT; }
        *err = errno;
        return -CN_WAIT_ERROR;
    }
#endif
    return rv;

#endif  /* HAVE_POLL */
}

/* Complete a connection started by PQconnectStart(), without the GIL.
 *
 * Same loop as libpq's pqConnectDBComplete(): initialise *state to
 * PGRES_POLLING_WRITING. If interval > 0, return CN_INTERRUPT every interval
 * seconds, and call again with the same *state to resume.
 */
static int
connect_nogil_impl(
    PGconn *conn, double deadline, double interval, int *state, int *err)
{
    double slice_end = interval > 0.0 ? cn_now() + interval : 0.0;
    double now, until;
    int timeout_ms, rv;

    for (;;) {
        if (*state == CN_NEED_POLL) {
            *state = PQconnectPoll(conn);
            switch (*state) {
            case PGRES_POLLING_OK: return CN_OK;
            case PGRES_POLLING_FAILED: return CN_FAILED;
            case PGRES_POLLING_READING:
            case PGRES_POLLING_WRITING: break;
            default: return CN_UNEXPECTED;
            }
        }

        now = cn_now();
        until = deadline;
        if (slice_end > 0.0 && slice_end < until) { until = slice_end; }
        timeout_ms = until > now ? (int)((until - now) * 1000.0) + 1 : 0;

        /* The socket may change, e.g. trying the next address. */
        rv = cn_wait(
            PQsocket(conn), *state == PGRES_POLLING_READING, timeout_ms, err);

        if (rv > 0) {
            *state = CN_NEED_POLL;
            continue;
        }
        if (rv == -CN_INTERRUPT) {
            if (interval > 0.0) { return CN_INTERRUPT; }
            continue;  /* signals are only handled in the main thread */
        }
        if (rv < 0) { return -rv; }

        now = cn_now();
        if (now >= deadline) { return CN_TIMEOUT; }
        if (slice_end > 0.0 && now >= slice_end) { return CN_INTERRUPT; }
    }
}
    """
    int CN_OK, CN_FAILED, CN_TIMEOUT, CN_INTERRUPT, CN_WAIT_ERROR
    int CN_FD_RANGE, CN_UNEXPECTED, CN_NEED_POLL, CN_ERR_IS_WSA
    double cn_now() nogil
    int connect_nogil_impl(
        libpq.PGconn *conn, double deadline, double interval, int *state, int *err
    ) nogil


def connect_nogil(conninfo: str, timeout: float, interval: float) -> abc.PGconn:
    """
    Connect to the database without holding the GIL, like `PQconnectdb()`.

    In the main thread, check for signals every `!interval` seconds.
    """
    cdef pq.PGconn conn = pq.PGconn.connect_start(conninfo.encode())
    cdef libpq.PGconn *pgconn_ptr = conn._pgconn_ptr
    cdef double deadline = cn_now() + float(timeout)
    cdef double cinterval = 0.0
    cdef int state = libpq.PGRES_POLLING_WRITING
    cdef int err = 0
    cdef int rv

    if current_thread() is main_thread():
        cinterval = float(interval)

    # A bad connection has no socket to wait on.
    if libpq.PQstatus(pgconn_ptr) == libpq.CONNECTION_BAD:
        encoding = conninfo_encoding(conninfo)
        raise e.OperationalError(
            f"connection is bad: {conn.get_error_message(encoding)}",
            pgconn=conn
        )

    logger.debug("connection started: %s", conn)
    while True:
        with nogil:
            rv = connect_nogil_impl(pgconn_ptr, deadline, cinterval, &state, &err)

        if rv == CN_OK:
            break
        elif rv == CN_INTERRUPT:
            PyErr_CheckSignals()
        elif rv == CN_TIMEOUT:
            conn.finish()
            raise e._WaitTimeout("wait timeout expired")
        elif rv == CN_FAILED:
            encoding = conninfo_encoding(conninfo)
            raise e.OperationalError(
                f"connection failed: {conn.get_error_message(encoding)}",
                pgconn=e.finish_pgconn(conn),
            )
        elif rv == CN_WAIT_ERROR:
            conn.finish()
            if CN_ERR_IS_WSA:
                # On Windows, OSError maps the winerror argument to errno.
                raise OSError(0, "select() failed", None, err)
            else:
                raise OSError(err, strerror(err))
        elif rv == CN_FD_RANGE:
            conn.finish()
            raise ValueError(  # same exception of Python's 'select.select()'
                "connection file descriptor out of range for 'select()'"
            )
        else:
            raise e.InternalError(
                f"unexpected poll status: {state}",
                pgconn=e.finish_pgconn(conn),
            )

    conn.nonblocking = 1
    return conn
