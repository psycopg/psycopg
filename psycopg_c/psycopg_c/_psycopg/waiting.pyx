"""
C implementation of waiting functions
"""

# Copyright (C) 2022 The Psycopg Team

cdef extern from *:
    """
#ifdef MS_WINDOWS
#include <winsock2.h>
#else
#include <sys/select.h>
#endif

#define SELECT_EV_READ 1
#define SELECT_EV_WRITE 2
#define SEC_TO_US (1000 * 1000)

/* Use select to wait for readiness on fileno.
 *
 * - Return SELECT_EV_* if the file is ready
 * - Return 0 on timeout
 * - Return -1 (and set an exception) on error.
 */
static int
select_impl(int fileno, int wait, float timeout)
{
    fd_set ifds;
    fd_set ofds;
    fd_set efds;
    struct timeval tv, *tvptr;
    int select_rv;

    FD_ZERO(&ifds);
    FD_ZERO(&ofds);
    FD_ZERO(&efds);

    if (wait & SELECT_EV_READ) {
        FD_SET(fileno, &ifds);
    }
    if (wait & SELECT_EV_WRITE) {
        FD_SET(fileno, &ofds);
    }
    FD_SET(fileno, &efds);

    /* Compute appropriate timeout interval */
    if (timeout < 0.0) {
        tvptr = NULL;
    }
    else
    {
        tv.tv_sec = (int)timeout;
        tv.tv_usec = (int)(((long)timeout * SEC_TO_US) % SEC_TO_US);
        tvptr = &tv;
    }

    Py_BEGIN_ALLOW_THREADS
    errno = 0;
    select_rv = select(fileno + 1, &ifds, &ofds, &efds, tvptr);
    Py_END_ALLOW_THREADS

    if (PyErr_CheckSignals()) {
        return -1;
    }

    if (select_rv < 0) {

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
        return -1;
    }
    else {
        int rv = 0;

        if (select_rv >= 0) {
            if (FD_ISSET(fileno, &ifds)) {
                rv = SELECT_EV_READ;
            }
            if (FD_ISSET(fileno, &ofds)) {
                rv |= SELECT_EV_WRITE;
            }
        }
        return rv;
    }
}
    """
    const int SELECT_EV_READ
    const int SELECT_EV_WRITE
    cdef int select_impl(int fileno, int wait, float timeout) except -1


def wait_select(gen: PQGen[RV], int fileno, timeout = None) -> RV:
    """
    Wait for a generator using select.
    """
    cdef float ctimeout
    cdef int wait, ready

    if timeout is None or timeout < 0:
        ctimeout = -1.0
    else:
        ctimeout = float(timeout)

    try:
        wait = next(gen)

        while True:
            ready = select_impl(fileno, wait, ctimeout)
            if ready == 0:
                continue

            wait = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv
