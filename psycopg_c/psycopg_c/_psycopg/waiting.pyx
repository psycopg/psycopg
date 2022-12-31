"""
C implementation of waiting functions
"""

# Copyright (C) 2022 The Psycopg Team

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

#define WAIT_ERR_NO_ERROR 0
#define WAIT_ERR_GENERIC -1
#define WAIT_ERR_FD_TOO_LARGE -2
#define WAIT_ERR_WINDOWS_SPECIFIC -3

/* Use poll() or select() to wait for readiness on fileno.
 *
 * - Return a combination of SELECT_EV_* if the file is ready
 * - Return 0 on timeout
 * - Return < 0 on error (a WAIT_ERR_* constant)
 *
 * This function can be called without the GIL. Call wait_c_set_exception()
 * holding the GIL in case of error, to raise the appropriate Python exception.
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
    int rv = WAIT_ERR_GENERIC;

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

    errno = 0;
    select_rv = poll(&input_fd, 1, timeout_ms);
    if (select_rv < 0) { goto exit; }

    rv = 0;
    if (input_fd.events & POLLIN) { rv |= SELECT_EV_READ; }
    if (input_fd.events & POLLOUT) { rv |= SELECT_EV_WRITE; }

#else

    fd_set ifds;
    fd_set ofds;
    fd_set efds;
    struct timeval tv, *tvptr;

#ifndef MS_WINDOWS
    if (fileno >= 1024) {
        rv = WAIT_ERR_FD_TOO_LARGE;
        goto exit;
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

    errno = 0;
    select_rv = select(fileno + 1, &ifds, &ofds, &efds, tvptr);
#ifdef MS_WINDOWS
    if (select_rv == SOCKET_ERROR) {
        rv = WAIT_ERR_WINDOWS_SPECIFIC;
        goto exit;
    }
#else
    if (select_rv < 0) {
        goto exit;
    }
#endif

    rv = 0;
    if (FD_ISSET(fileno, &ifds)) { rv |= SELECT_EV_READ; }
    if (FD_ISSET(fileno, &ofds)) { rv |= SELECT_EV_WRITE; }

#endif  /* HAVE_POLL */

exit:

    return rv;

}


/* Set a Python exception based on the return value from wait_c_impl()
 *
 * This function exists so that the happy path can be called without ever
 * acquiring the GIL, and only acquire it in "rare occurrences", i.e. on
 * exceptions or to periodically check signals
 *
 * Note that his function doesn't "raise": it only set the exception.
 * 
 */
static int
wait_c_set_error(int err)
{
    int rv = -1;

    switch (err) {

        case WAIT_ERR_NO_ERROR:
            if (0 == PyErr_CheckSignals()) {
                rv = 0;
            }
            break;

#ifdef MS_WINDOWS

        case WAIT_ERR_WINDOWS_SPECIFIC:
            PyErr_SetExcFromWindowsErr(PyExc_OSError, WSAGetLastError());
            break;

#else

        case WAIT_ERR_FD_TOO_LARGE:
            PyErr_SetString(
                PyExc_ValueError,  /* same exception of Python's 'select.select()' */
                "connection file descriptor out of range for 'select()'");
            break;

#endif

        default:
            PyErr_SetFromErrno(PyExc_OSError);
            break;

    }

    return rv;

}


/* Raise the currently set Python exception.
 *
 * This function only exists as a workaround for Cython, because a function
 * marked nogil cannot raise. It actually can, after having re-acquired the
 * gil, but the function caller is not aware of it.
 *
 * So, this is a trick, and in Cython's eyes, this function will always raise
 * something (but it should be called after an exception has been set, typically
 * after calling wait_c_set_error().
 */
static PyObject *
wait_dummy_raise()
{
    return NULL;
}
    """
    int wait_c_impl(int fileno, int wait, float timeout) nogil
    int wait_c_set_error(int err)
    PyObject *wait_dummy_raise() except NULL


cdef int wait_ng(int fileno, int wait) nogil:
    """
    Wait for a descriptor to be ready for read/write.

    This function is meant to be called from Cython code. Note that the function
    doesn't "raise", as far as Cython is concerned, mostly because I don't see
    a way to declare a function both `except` and `nogil`. So, in case of
    return with an error, Cython code should call `wait_dummy_raise()` as soon
    as it has the `gil` back.

    Args:
        `fileno`: the file descriptor to wait on.
        `wait`: the condition to wait on. Binary-or of SELECT_EV_* constants.

    Returns:
        - > 0 on success: the readiness state (binary-or of SELECT_EV_* constants.
        - -1 on failure, and set a Python exception.
    """
    cdef int rv

    while True:
        rv = wait_c_impl(fileno, wait, 0.1)
        if rv > 0:
            return rv

        # We take this branch "rarely": either in case of error, or on
        # timeout, in which case we should check the signals.
        # In case of exception, we terminate the function with an exception
        # set, but not raised, according to Cython.
        # Raising must be done downstream, once our caller has re-acquired
        # the GIL, calling `wait_dummy_raise()`.
        with gil:
            rv = wait_c_set_error(rv)
            if rv < 0:
                return rv


def wait_c(int fileno, int wait) -> int:
    """
    Wait for a descriptor to be ready for read/write.

    This function is meant to be called from Python code.

    Args:
        `fileno`: the file descriptor to wait on.
        `wait`: the condition to wait on. Binary-or of SELECT_EV_* constants.

    Returns:
        - > 0 on success: the readiness state (binary-or of SELECT_EV_* constants.
        - -1 on failure, and set a Python exception.
    """
    cdef int rv
    with nogil:
        rv = wait_ng(fileno, wait)
    if rv < 0:
        wait_dummy_raise()
    return rv
