"""
Code concerned with waiting in different contexts (blocking, async, etc).

These functions are designed to consume the generators returned by the
`generators` module function and to return their final value.

"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import os
import sys
import select
import logging
import selectors
from time import monotonic
from asyncio import Future, get_running_loop
from selectors import DefaultSelector

from . import errors as e
from .abc import RV, PQGen, PQGenConn, WaitFunc
from ._enums import Ready as Ready
from ._enums import Wait as Wait  # re-exported
from ._cmodule import _psycopg

WAIT_R = Wait.R
WAIT_W = Wait.W
WAIT_RW = Wait.RW
READY_NONE = Ready.NONE
READY_R = Ready.R
READY_W = Ready.W
READY_RW = Ready.RW

logger = logging.getLogger(__name__)


if sys.platform != "win32":

    def _check_fd_closed(fileno: int) -> None:
        """
        Raise OperationalError if the connection is lost.
        """
        try:
            os.fstat(fileno)
        except Exception as ex:
            raise e.OperationalError("connection socket closed") from ex

else:

    # On windows we cannot use os.fstat() to check a socket.
    def _check_fd_closed(fileno: int) -> None:
        return


def _wait_time(interval: float, deadline: float) -> float:
    """
    Return the time to wait before the next wake-up, not going past the deadline.
    """
    return max(0.0, min(interval, deadline - monotonic()))


def wait_selector(
    gen: PQGen[RV], fileno: int, interval: float = 0.0, timeout: float | None = None
) -> RV:
    """
    Wait for a generator using the best strategy available.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :param interval: interval (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :param timeout: maximum time (in seconds) to wait for `!gen` to complete.
        Raise `~psycopg.errors._WaitTimeout` when it expires. `!None` means no
        timeout.
    :return: whatever `!gen` returns on completion.

    Consume `!gen`, scheduling `fileno` for completion when it is reported to
    block. Once ready again send the ready state back to `!gen`.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    deadline = monotonic() + timeout if timeout is not None else None
    try:
        s = next(gen)
        with DefaultSelector() as sel:
            sel.register(fileno, (last_s := s))
            while True:
                t = interval if deadline is None else _wait_time(interval, deadline)
                if not (rlist := sel.select(timeout=t)):
                    # Check if it was a timeout or we were disconnected
                    _check_fd_closed(fileno)
                    gen.send(READY_NONE)
                else:
                    ready = rlist[0][1]
                    s = gen.send(ready)
                    if last_s != s:
                        sel.unregister(fileno)
                        sel.register(fileno, (last_s := s))

                # Check the deadline after resuming the generator, so that the
                # data already available is processed and a timeout of 0 polls
                # once.
                if deadline is not None and monotonic() >= deadline:
                    raise e._WaitTimeout("wait timeout expired")

    except StopIteration as ex:
        rv: RV = ex.value
        return rv


def wait_conn(
    gen: PQGenConn[RV], interval: float = 0.0, timeout: float | None = None
) -> RV:
    """
    Wait for a connection generator using the best strategy available.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param interval: interval (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :param timeout: maximum time (in seconds) to wait for `!gen` to complete.
        Raise `~psycopg.errors._WaitTimeout` when it expires. `!None` means no
        timeout.
    :return: whatever `!gen` returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    deadline = monotonic() + timeout if timeout is not None else None
    try:
        fileno, s = next(gen)
        with DefaultSelector() as sel:
            sel.register((last_fileno := fileno), (last_s := s))
            while True:
                t = interval if deadline is None else _wait_time(interval, deadline)
                if not (rlist := sel.select(timeout=t)):
                    gen.send(READY_NONE)
                else:
                    ready = rlist[0][1]
                    fileno, s = gen.send(ready)
                    if fileno != last_fileno or last_s != s:
                        sel.unregister(last_fileno)
                        sel.register((last_fileno := fileno), (last_s := s))

                if deadline is not None and monotonic() >= deadline:
                    raise e._WaitTimeout("wait timeout expired")

    except StopIteration as ex:
        rv: RV = ex.value
        return rv


async def wait_async(
    gen: PQGen[RV], fileno: int, interval: float = 0.0, timeout: float | None = None
) -> RV:
    """
    Coroutine waiting for a generator to complete.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :param interval: interval (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :param timeout: maximum time (in seconds) to wait for `!gen` to complete.
        Raise `~psycopg.errors._WaitTimeout` when it expires. `!None` means no
        timeout.
    :return: whatever `!gen` returns on completion.

    Behave like in `wait()`, but exposing an `asyncio` interface.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    # Don't touch the event loop before we know that we will have to wait: a
    # generator completing without blocking (e.g. every copy() round) pays
    # nothing more than this.
    try:
        s = next(gen)
    except StopIteration as ex:
        rv: RV = ex.value
        return rv

    deadline = monotonic() + timeout if timeout is not None else None
    loop = get_running_loop()
    ready: int = 0
    fut: Future[None] | None = None

    # Block on a future and resolve it from the fd callbacks, or from a timer
    # if the interval expires first. A future is cheaper than an Event plus
    # wait_for(), which would wrap every single wait in a Task.
    def wakeup(state: Ready) -> None:
        nonlocal ready
        ready |= state
        if fut is not None and not fut.done():
            fut.set_result(None)

    def timedout() -> None:
        if fut is not None and not fut.done():
            fut.set_result(None)

    try:
        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")
            ready = 0
            if reader:
                loop.add_reader(fileno, wakeup, READY_R)
            if writer:
                loop.add_writer(fileno, wakeup, READY_W)
            try:
                t = interval if deadline is None else _wait_time(interval, deadline)
                fut = loop.create_future()
                timer = loop.call_later(t, timedout)
                try:
                    await fut
                finally:
                    timer.cancel()
                    fut = None
            finally:
                if reader:
                    loop.remove_reader(fileno)
                if writer:
                    loop.remove_writer(fileno)
            s = gen.send(ready)

            if deadline is not None and monotonic() >= deadline:
                raise e._WaitTimeout("wait timeout expired")

    except OSError as ex:
        # Assume the connection was closed
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv = ex.value
        return rv


async def wait_conn_async(
    gen: PQGenConn[RV], interval: float = 0.0, timeout: float | None = None
) -> RV:
    """
    Coroutine waiting for a connection generator to complete.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param interval: interval (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :param timeout: maximum time (in seconds) to wait for `!gen` to complete.
        Raise `~psycopg.errors._WaitTimeout` when it expires. `!None` means no
        timeout.
    :return: whatever `!gen` returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")

    # Don't touch the event loop before we know that we will have to wait.
    # See `wait_async()` for the rationale of this function's structure.
    try:
        fileno, s = next(gen)
    except StopIteration as ex:
        rv: RV = ex.value
        return rv

    deadline = monotonic() + timeout if timeout is not None else None
    loop = get_running_loop()
    ready: int = 0
    fut: Future[None] | None = None

    def wakeup(state: Ready) -> None:
        nonlocal ready
        ready |= state
        if fut is not None and not fut.done():
            fut.set_result(None)

    def timedout() -> None:
        if fut is not None and not fut.done():
            fut.set_result(None)

    try:
        while True:
            reader = s & WAIT_R
            writer = s & WAIT_W
            if not (reader or writer):
                raise e.InternalError(f"bad poll status: {s}")
            ready = 0
            if reader:
                loop.add_reader(fileno, wakeup, READY_R)
            if writer:
                loop.add_writer(fileno, wakeup, READY_W)
            try:
                t = interval if deadline is None else _wait_time(interval, deadline)
                fut = loop.create_future()
                timer = loop.call_later(t, timedout)
                try:
                    await fut
                finally:
                    timer.cancel()
                    fut = None
            finally:
                if reader:
                    loop.remove_reader(fileno)
                if writer:
                    loop.remove_writer(fileno)
            fileno, s = gen.send(ready)

            if deadline is not None and monotonic() >= deadline:
                raise e._WaitTimeout("wait timeout expired")

    except OSError as ex:
        # Assume the connection was closed
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv = ex.value
        return rv


# Specialised implementation of wait functions.


def wait_select(
    gen: PQGen[RV], fileno: int, interval: float = 0.0, timeout: float | None = None
) -> RV:
    """
    Wait for a generator using select where supported.

    BUG: on Linux, can't select on FD >= 1024. On Windows it's fine.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    deadline = monotonic() + timeout if timeout is not None else None
    try:
        s = next(gen)

        empty = ()
        fnlist = (fileno,)
        while True:
            t = interval if deadline is None else _wait_time(interval, deadline)
            rl, wl, xl = select.select(
                fnlist if s & WAIT_R else empty,
                fnlist if s & WAIT_W else empty,
                fnlist,
                t,
            )
            if xl:
                _check_fd_closed(fileno)
                # Unlikely: the exception should have been raised above
                raise e.OperationalError("connection socket closed")
            ready = 0
            if rl:
                ready = READY_R
            if wl:
                ready |= READY_W

            s = gen.send(ready)

            if deadline is not None and monotonic() >= deadline:
                raise e._WaitTimeout("wait timeout expired")

    except OSError as ex:
        # This happens on macOS but not on Linux (the xl list is set)
        raise e.OperationalError("connection socket closed") from ex
    except StopIteration as ex:
        rv: RV = ex.value
        return rv


if hasattr(selectors, "EpollSelector"):
    _epoll_evmasks = {
        WAIT_R: select.EPOLLONESHOT | select.EPOLLIN,
        WAIT_W: select.EPOLLONESHOT | select.EPOLLOUT,
        WAIT_RW: select.EPOLLONESHOT | select.EPOLLIN | select.EPOLLOUT,
    }
else:
    _epoll_evmasks = {}


def wait_epoll(
    gen: PQGen[RV], fileno: int, interval: float = 0.0, timeout: float | None = None
) -> RV:
    """
    Wait for a generator using epoll where supported.

    Parameters are like for `wait()`. If it is detected that the best selector
    strategy is `epoll` then this function will be used instead of `wait`.

    See also: https://linux.die.net/man/2/epoll_ctl

    BUG: if the connection FD is closed, `epoll.poll()` hangs. Same for
    EpollSelector. For this reason, wait_poll() is currently preferable.
    To reproduce the bug:

        export PSYCOPG_WAIT_FUNC=wait_epoll
        pytest tests/test_concurrency.py::test_concurrent_close
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    deadline = monotonic() + timeout if timeout is not None else None
    try:
        s = next(gen)

        if interval < 0:
            interval = 0.0

        with select.epoll() as epoll:
            evmask = _epoll_evmasks[s]
            epoll.register(fileno, evmask)
            while True:
                t = interval if deadline is None else _wait_time(interval, deadline)
                if not (fileevs := epoll.poll(t)):
                    _check_fd_closed(fileno)
                    gen.send(READY_NONE)
                else:
                    ev = fileevs[0][1]
                    ready = 0
                    if ev & select.EPOLLIN:
                        ready = READY_R
                    if ev & select.EPOLLOUT:
                        ready |= READY_W
                    s = gen.send(ready)
                    evmask = _epoll_evmasks[s]
                    epoll.modify(fileno, evmask)

                if deadline is not None and monotonic() >= deadline:
                    raise e._WaitTimeout("wait timeout expired")

    except StopIteration as ex:
        rv: RV = ex.value
        return rv


if hasattr(selectors, "PollSelector"):
    _poll_evmasks = {
        WAIT_R: select.POLLIN,
        WAIT_W: select.POLLOUT,
        WAIT_RW: select.POLLIN | select.POLLOUT,
    }
    POLL_BAD = ~(select.POLLIN | select.POLLOUT)
else:
    _poll_evmasks = {}


def wait_poll(
    gen: PQGen[RV], fileno: int, interval: float = 0.0, timeout: float | None = None
) -> RV:
    """
    Wait for a generator using poll where supported.

    Parameters are like for `wait()`.
    """
    if interval is None:
        raise ValueError("indefinite wait not supported anymore")
    deadline = monotonic() + timeout if timeout is not None else None
    try:
        s = next(gen)

        if interval < 0:
            interval = 0.0
        interval_ms = int(interval * 1000.0)

        poll = select.poll()
        evmask = _poll_evmasks[s]
        poll.register(fileno, evmask)
        while True:
            if deadline is None:
                t = interval_ms
            else:
                t = int(_wait_time(interval, deadline) * 1000.0)
            if not (fileevs := poll.poll(t)):
                gen.send(READY_NONE)
            else:
                ev = fileevs[0][1]

                ready = 0
                if ev & select.POLLIN:
                    ready = READY_R
                if ev & select.POLLOUT:
                    ready |= READY_W

                if not ready and ev & POLL_BAD:
                    _check_fd_closed(fileno)
                    # Unlikely: the exception should have been raised above
                    raise e.OperationalError("connection socket closed")

                s = gen.send(ready)
                evmask = _poll_evmasks[s]
                poll.modify(fileno, evmask)

            if deadline is not None and monotonic() >= deadline:
                raise e._WaitTimeout("wait timeout expired")

    except StopIteration as ex:
        rv: RV = ex.value
        return rv


def _is_select_patched() -> bool:
    """
    Detect if some greenlet library has patched the select library.

    If this is the case, avoid to use the wait_c function as it doesn't behave
    in a collaborative way.

    Currently supported: gevent.
    """
    # If not imported, don't import it.
    if m := sys.modules.get("gevent.monkey"):
        try:
            if m.is_module_patched("select"):
                return True
        except Exception as ex:
            logger.warning("failed to detect gevent monkey-patching: %s", ex)

    return False


if _psycopg:
    wait_c = _psycopg.wait_c


# Choose the best wait strategy for the platform.
#
# the selectors objects have a generic interface but come with some overhead,
# so we also offer more finely tuned implementations.

wait: WaitFunc

# Allow the user to choose a specific function for testing
if "PSYCOPG_WAIT_FUNC" in os.environ:
    fname = os.environ["PSYCOPG_WAIT_FUNC"]
    if not fname.startswith("wait_") or fname not in globals():
        raise ImportError(
            "PSYCOPG_WAIT_FUNC should be the name of an available wait function;"
            f" got {fname!r}"
        )
    wait = globals()[fname]

# On Windows, for the moment, avoid using wait_c, because it was reported to
# use excessive CPU (see #645).
# TODO: investigate why.
elif _psycopg and sys.platform != "win32" and not _is_select_patched():
    wait = wait_c

elif selectors.DefaultSelector is getattr(selectors, "SelectSelector", None):
    # On Windows, SelectSelector should be the default.
    wait = wait_select

elif hasattr(selectors, "PollSelector"):
    # On linux, EpollSelector is the default. However, it hangs if the fd is
    # closed while polling.
    wait = wait_poll

else:
    wait = wait_selector
