"""
C implementation of waiting functions
"""

# Copyright (C) 2022 The Psycopg Team

from cpython.object cimport PyObject_CallFunctionObjArgs

cdef extern from *:
    """
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

#define SEC_TO_MS 1000
#define SEC_TO_US (1000 * 1000)

/* Use select to wait for readiness on fileno.
 *
 * - Return SELECT_EV_* if the file is ready
 * - Return 0 on timeout
 * - Return -1 (and set an exception) on error.
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
    int rv = 0;

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

    Py_BEGIN_ALLOW_THREADS
    errno = 0;
    select_rv = poll(&input_fd, 1, timeout_ms);
    Py_END_ALLOW_THREADS

    if (PyErr_CheckSignals()) { goto finally; }

    if (select_rv < 0) {
        goto error;
    }

    if (input_fd.events & POLLIN) { rv |= SELECT_EV_READ; }
    if (input_fd.events & POLLOUT) { rv |= SELECT_EV_WRITE; }

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
        return -1;
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
        tv.tv_usec = (int)(((long)timeout * SEC_TO_US) % SEC_TO_US);
        tvptr = &tv;
    }

    Py_BEGIN_ALLOW_THREADS
    errno = 0;
    select_rv = select(fileno + 1, &ifds, &ofds, &efds, tvptr);
    Py_END_ALLOW_THREADS

    if (PyErr_CheckSignals()) { goto finally; }

    if (select_rv < 0) {
        goto error;
    }

    if (FD_ISSET(fileno, &ifds)) { rv |= SELECT_EV_READ; }
    if (FD_ISSET(fileno, &ofds)) { rv |= SELECT_EV_WRITE; }

#endif  /* HAVE_POLL */

    return rv;

error:

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

    return -1;

}
    """
    cdef int wait_c_impl(int fileno, int wait, float timeout) except -1


def wait_c(gen: PQGen[RV], int fileno, timeout = None) -> RV:
    """
    Wait for a generator using poll or select.
    """
    cdef float ctimeout
    cdef int wait, ready
    cdef PyObject *pyready

    if timeout is None:
        ctimeout = -1.0
    else:
        ctimeout = float(timeout)
        if ctimeout < 0.0:
            ctimeout = -1.0

    send = gen.send

    try:
        wait = next(gen)

        while True:
            ready = wait_c_impl(fileno, wait, ctimeout)
            if ready == 0:
                continue
            elif ready == READY_R:
                pyready = <PyObject *>PY_READY_R
            elif ready == READY_RW:
                pyready = <PyObject *>PY_READY_RW
            elif ready == READY_W:
                pyready = <PyObject *>PY_READY_W
            else:
                raise AssertionError(f"unexpected ready value: {ready}")

            wait = PyObject_CallFunctionObjArgs(send, pyready, NULL)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv
