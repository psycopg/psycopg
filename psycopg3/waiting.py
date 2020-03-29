"""
Code concerned with waiting in different contexts (blocking, async, etc).
"""

# Copyright (C) 2020 The Psycopg Team


from enum import Enum
from select import select
from typing import Generator, Tuple, TypeVar
from asyncio import get_event_loop, Event

from . import exceptions as exc


Wait = Enum("Wait", "R W RW")
Ready = Enum("Ready", "R W")

RV = TypeVar("RV")


def wait_select(gen: Generator[Tuple[int, Wait], Ready, RV]) -> RV:
    """
    Wait on the behalf of a generator using select().

    *gen* is expected to generate tuples (fd, status). consume it and block
    according to the status until fd is ready. Send back the ready state
    to the generator.

    Return what the generator eventually returned.
    """
    try:
        while 1:
            fd, s = next(gen)
            if s is Wait.R:
                rf, wf, xf = select([fd], [], [])
                assert rf
                gen.send(Ready.R)
            elif s is Wait.W:
                rf, wf, xf = select([], [fd], [])
                assert wf
                gen.send(Ready.W)
            elif s is Wait.RW:
                rf, wf, xf = select([fd], [fd], [])
                assert rf or wf
                assert not (rf and wf)
                if rf:
                    gen.send(Ready.R)
                else:
                    gen.send(Ready.W)
            else:
                raise exc.InternalError("bad poll status: %s")
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
        while 1:
            fd, s = next(gen)
            ev.clear()
            if s is Wait.R:
                loop.add_reader(fd, wakeup, Ready.R)
                await ev.wait()
                loop.remove_reader(fd)
                gen.send(ready)
            elif s is Wait.W:
                loop.add_writer(fd, wakeup, Ready.W)
                await ev.wait()
                loop.remove_writer(fd)
                gen.send(ready)
            elif s is Wait.RW:
                loop.add_reader(fd, wakeup, Ready.R)
                loop.add_writer(fd, wakeup, Ready.W)
                await ev.wait()
                loop.remove_reader(fd)
                loop.remove_writer(fd)
                gen.send(ready)
            else:
                raise exc.InternalError("bad poll status: %s")
    except StopIteration as e:
        rv: RV = e.args[0]
        return rv
