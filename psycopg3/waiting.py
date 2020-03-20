"""
Code concerned with waiting in different contexts (blocking, async, etc).
"""

# Copyright (C) 2020 The Psycopg Team


from select import select
from asyncio import get_event_loop
from asyncio.queues import Queue

from . import exceptions as exc


WAIT_R = "WAIT_R"
WAIT_W = "WAIT_W"
WAIT_RW = "WAIT_RW"
READY_R = "READY_R"
READY_W = "READY_W"


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
            if s == WAIT_R:
                rf, wf, xf = select([fd], [], [])
                assert rf
                gen.send(READY_R)
            elif s == WAIT_W:
                rf, wf, xf = select([], [fd], [])
                assert wf
                gen.send(READY_W)
            elif s == WAIT_RW:
                rf, wf, xf = select([fd], [fd], [])
                assert rf or wf
                assert not (rf and wf)
                if rf:
                    gen.send(READY_R)
                else:
                    gen.send(READY_W)
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
    q = Queue()
    loop = get_event_loop()
    try:
        while 1:
            fd, s = next(gen)
            if s == WAIT_R:
                loop.add_reader(fd, q.put_nowait, READY_R)
                ready = await q.get()
                loop.remove_reader(fd)
                gen.send(ready)
            elif s == WAIT_W:
                loop.add_writer(fd, q.put_nowait, READY_W)
                ready = await q.get()
                loop.remove_writer(fd)
                gen.send(ready)
            elif s == WAIT_RW:
                loop.add_reader(fd, q.put_nowait, READY_R)
                loop.add_writer(fd, q.put_nowait, READY_W)
                ready = await q.get()
                loop.remove_reader(fd)
                loop.remove_writer(fd)
                gen.send(ready)
            else:
                raise exc.InternalError("bad poll status: %s")
    except StopIteration as e:
        return e.args[0]
