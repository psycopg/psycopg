"""
C implementation of waiting functions
"""

# Copyright (C) 2022 The Psycopg Team

cdef extern from *:
    """
#include <sys/select.h>

#define SELECT_EV_READ 1
#define SELECT_EV_WRITE 2
#define SEC_TO_US (1000 * 1000)

static int
select_impl(int fileno, int wait, float timeout)
{
    fd_set ifds;
    fd_set ofds;
    fd_set efds;
    struct timeval tv, *tvptr;
    int select_rv, rv = 0;

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

    if (select_rv <= 0) {
        rv = select_rv;
    }
    else {
        if (FD_ISSET(fileno, &ifds)) {
            rv |= SELECT_EV_READ;
        }
        if (FD_ISSET(fileno, &ofds)) {
            rv |= SELECT_EV_WRITE;
        }
    }

    return rv;
}

static int
select_raise(int n)
{
#ifdef MS_WINDOWS
    if (n == SOCKET_ERROR) {
        PyErr_SetExcFromWindowsErr(PyExc_OSError, WSAGetLastError());
        return -1;
    }
#else
    if (n < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
#endif

    PyErr_SetString(PyExc_OSError, "unexpected error from select()");
    return -1;
}
    """
    const int SELECT_EV_READ
    const int SELECT_EV_WRITE
    cdef int select_impl(int fileno, int wait, float timeout)
    cdef int select_raise(int e) except -1


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
            elif ready < 0:
                select_raise(ready)

            wait = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv
