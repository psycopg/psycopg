"""
Code concerned with waiting in different contexts (blocking, async, etc).
"""

# Copyright (C) 2020 The Psycopg Team


from enum import IntEnum
from typing import Generator, Optional, Tuple, TypeVar
from asyncio import get_event_loop, Event
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE

from . import exceptions as exc


class Wait(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE
    RW = EVENT_READ | EVENT_WRITE


class Ready(IntEnum):
    R = EVENT_READ
    W = EVENT_WRITE


RV = TypeVar("RV")


def wait(
    gen: Generator[Tuple[int, Wait], Ready, RV],
    timeout: Optional[float] = None,
) -> RV:
    """
    Wait for a generator using the best option available on the platform.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C.
    :type timeout: float
    :return: whatever *gen* returns on completion.
    """
    sel = DefaultSelector()
    try:
        fd, s = next(gen)
        while 1:
            sel.register(fd, s)
            ready = None
            while not ready:
                ready = sel.select(timeout=timeout)
            sel.unregister(fd)

            assert len(ready) == 1
            fd, s = gen.send(ready[0][1])

    except StopIteration as e:
        rv: RV = e.args[0]
        return rv


async def wait_async(gen: Generator[Tuple[int, Wait], Ready, RV]) -> RV:
    """
    Coroutine waiting for a generator to complete.

    *gen* is expected to generate tuples (fd, status). consume it and block
    according to the status until fd is ready. Send back the ready state
    to the generator.

    Return what the generator eventually returned.
    """
    # Use an event to block and restart after the fd state changes.
    # Not sure this is the best implementation but it's a start.
    ev = Event()
    loop = get_event_loop()
    ready = Ready.R

    def wakeup(state: Ready) -> None:
        nonlocal ready
        ready = state
        ev.set()

    try:
        fd, s = next(gen)
        while 1:
            ev.clear()
            if s is Wait.R:
                loop.add_reader(fd, wakeup, Ready.R)
                await ev.wait()
                loop.remove_reader(fd)
            elif s is Wait.W:
                loop.add_writer(fd, wakeup, Ready.W)
                await ev.wait()
                loop.remove_writer(fd)
            elif s is Wait.RW:
                loop.add_reader(fd, wakeup, Ready.R)
                loop.add_writer(fd, wakeup, Ready.W)
                await ev.wait()
                loop.remove_reader(fd)
                loop.remove_writer(fd)
            else:
                raise exc.InternalError("bad poll status: %s")
            fd, s = gen.send(ready)

    except StopIteration as e:
        rv: RV = e.args[0]
        return rv
