"""
Code concerned with waiting in different contexts (blocking, async, etc).
"""

# Copyright (C) 2020 The Psycopg Team


from enum import Enum
from select import select
from asyncio import get_event_loop, Event

from . import exceptions as exc


Wait = Enum("Wait", "R W RW")
Ready = Enum("Ready", "R W")


def wait_select(gen):
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
        return e.args[0]


async def wait_async(gen):
    """
    Coroutine waiting for a generator to complete.

    *gen* is expected to generate tuples (fd, status). consume it and block
    according to the status until fd is ready. Send back the ready state
    to the generator.

    Return what the generator eventually returned.
    """
    # Use a queue to block and restart after the fd state changes.
    # Not sure this is the best implementation but it's a start.
    e = Event()
    loop = get_event_loop()
    ready = None

    def wakeup(state):
        nonlocal ready
        ready = state
        e.set()

    try:
        while 1:
            fd, s = next(gen)
            e.clear()
            if s is Wait.R:
                loop.add_reader(fd, wakeup, Ready.R)
                await e.wait()
                loop.remove_reader(fd)
                gen.send(ready)
            elif s is Wait.W:
                loop.add_writer(fd, wakeup, Ready.W)
                await e.wait()
                loop.remove_writer(fd)
                gen.send(ready)
            elif s is Wait.RW:
                loop.add_reader(fd, wakeup, Ready.R)
                loop.add_writer(fd, wakeup, Ready.W)
                await e.wait()
                loop.remove_reader(fd)
                loop.remove_writer(fd)
                gen.send(ready)
            else:
                raise exc.InternalError("bad poll status: %s")
    except StopIteration as e:
        return e.args[0]
