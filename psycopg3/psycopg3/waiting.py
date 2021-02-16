"""
Code concerned with waiting in different contexts (blocking, async, etc).

These functions are designed to consume the generators returned by the
`generators` module function and to return their final value.

"""

# Copyright (C) 2020-2021 The Psycopg Team


import select
import selectors
from enum import IntEnum
from typing import Optional
from asyncio import get_event_loop, Event
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE

from . import errors as e
from .proto import PQGen, PQGenConn, RV


class Wait(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE
    RW = EVENT_READ | EVENT_WRITE


class Ready(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE


def wait_selector(
    gen: PQGen[RV], fileno: int, timeout: Optional[float] = None
) -> RV:
    """
    Wait for a generator using the best strategy available.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :type timeout: float
    :return: whatever *gen* returns on completion.

    Consume *gen*, scheduling `fileno` for completion when it is reported to
    block. Once ready again send the ready state back to *gen*.
    """
    try:
        s = next(gen)
        sel = DefaultSelector()
        while 1:
            sel.register(fileno, s)
            ready = None
            while not ready:
                ready = sel.select(timeout=timeout)
            sel.unregister(fileno)
            s = gen.send(ready[0][1])

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


def wait_conn(gen: PQGenConn[RV], timeout: Optional[float] = None) -> RV:
    """
    Wait for a connection generator using the best strategy available.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :type timeout: float
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    try:
        fileno, s = next(gen)
        sel = DefaultSelector()
        while 1:
            sel.register(fileno, s)
            ready = None
            while not ready:
                ready = sel.select(timeout=timeout)
            sel.unregister(fileno)
            fileno, s = gen.send(ready[0][1])

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


async def wait_async(gen: PQGen[RV], fileno: int) -> RV:
    """
    Coroutine waiting for a generator to complete.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but exposing an `asyncio` interface.
    """
    # Use an event to block and restart after the fd state changes.
    # Not sure this is the best implementation but it's a start.
    ev = Event()
    loop = get_event_loop()
    ready: Ready
    s: Wait

    def wakeup(state: Ready) -> None:
        nonlocal ready
        ready = state
        ev.set()

    try:
        s = next(gen)
        while 1:
            ev.clear()
            if s == Wait.R:
                loop.add_reader(fileno, wakeup, Ready.R)
                await ev.wait()
                loop.remove_reader(fileno)
            elif s == Wait.W:
                loop.add_writer(fileno, wakeup, Ready.W)
                await ev.wait()
                loop.remove_writer(fileno)
            elif s == Wait.RW:
                loop.add_reader(fileno, wakeup, Ready.R)
                loop.add_writer(fileno, wakeup, Ready.W)
                await ev.wait()
                loop.remove_reader(fileno)
                loop.remove_writer(fileno)
            else:
                raise e.InternalError("bad poll status: %s")
            s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


async def wait_conn_async(gen: PQGenConn[RV]) -> RV:
    """
    Coroutine waiting for a connection generator to complete.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :return: whatever *gen* returns on completion.

    Behave like in `wait()`, but take the fileno to wait from the generator
    itself, which might change during processing.
    """
    # Use an event to block and restart after the fd state changes.
    # Not sure this is the best implementation but it's a start.
    ev = Event()
    loop = get_event_loop()
    ready: Ready
    s: Wait

    def wakeup(state: Ready) -> None:
        nonlocal ready
        ready = state
        ev.set()

    try:
        fileno, s = next(gen)
        while 1:
            ev.clear()
            if s == Wait.R:
                loop.add_reader(fileno, wakeup, Ready.R)
                await ev.wait()
                loop.remove_reader(fileno)
            elif s == Wait.W:
                loop.add_writer(fileno, wakeup, Ready.W)
                await ev.wait()
                loop.remove_writer(fileno)
            elif s == Wait.RW:
                loop.add_reader(fileno, wakeup, Ready.R)
                loop.add_writer(fileno, wakeup, Ready.W)
                await ev.wait()
                loop.remove_reader(fileno)
                loop.remove_writer(fileno)
            else:
                raise e.InternalError("bad poll status: %s")
            fileno, s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


def wait_epoll(
    gen: PQGen[RV], fileno: int, timeout: Optional[float] = None
) -> RV:
    """
    Wait for a generator using epoll where supported.

    Parameters are like for `wait()`. If it is detected that the best selector
    strategy is `epoll` then this function will be used instead of `wait`.

    See also: https://linux.die.net/man/2/epoll_ctl
    """
    if timeout is None or timeout < 0:
        timeout = 0
    else:
        timeout = int(timeout * 1000.0)

    try:
        s = next(gen)
        epoll = select.epoll()
        evmask = poll_evmasks[s]
        epoll.register(fileno, evmask)
        while 1:
            fileevs = None
            while not fileevs:
                fileevs = epoll.poll(timeout)
            ev = fileevs[0][1]
            if ev & ~select.EPOLLOUT:
                s = Ready.R
            else:
                s = Ready.W
            s = gen.send(s)
            evmask = poll_evmasks[s]
            epoll.modify(fileno, evmask)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv


if selectors.DefaultSelector is getattr(selectors, "EpollSelector", None):
    wait = wait_epoll

    poll_evmasks = {
        Wait.R: select.EPOLLONESHOT | select.EPOLLIN,
        Wait.W: select.EPOLLONESHOT | select.EPOLLOUT,
        Wait.RW: select.EPOLLONESHOT | select.EPOLLIN | select.EPOLLOUT,
    }

else:
    wait = wait_selector
