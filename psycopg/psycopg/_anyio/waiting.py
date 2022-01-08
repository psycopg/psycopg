"""
Async waiting functions using AnyIO.
"""

# Copyright (C) 2022 The Psycopg Team


import socket
from typing import Optional

import anyio

from .. import errors as e
from ..abc import PQGen, PQGenConn, RV
from ..waiting import Ready, Wait


def _fromfd(fileno: int) -> socket.socket:
    # AnyIO's wait_socket_readable() and wait_socket_writable() functions work
    # with socket object (despite the underlying async libraries -- asyncio and
    # trio -- accept integer 'fileno' values):
    # https://github.com/agronholm/anyio/issues/386
    try:
        return socket.fromfd(fileno, socket.AF_INET, socket.SOCK_STREAM)
    except OSError as exc:
        raise e.OperationalError(
            f"failed to build a socket from connection file descriptor: {exc}"
        )


async def wait(gen: PQGen[RV], fileno: int) -> RV:
    """
    Coroutine waiting for a generator to complete.

    :param gen: a generator performing database operations and yielding
        `Ready` values when it would block.
    :param fileno: the file descriptor to wait on.
    :return: whatever *gen* returns on completion.

    Behave like in `waiting.wait()`, but exposing an `anyio` interface.
    """
    s: Wait
    ready: Ready
    sock = _fromfd(fileno)

    async def readable(ev: anyio.Event) -> None:
        await anyio.wait_socket_readable(sock)
        nonlocal ready
        ready |= Ready.R  # type: ignore[assignment]
        ev.set()

    async def writable(ev: anyio.Event) -> None:
        await anyio.wait_socket_writable(sock)
        nonlocal ready
        ready |= Ready.W  # type: ignore[assignment]
        ev.set()

    try:
        s = next(gen)
        while True:
            reader = s & Wait.R
            writer = s & Wait.W
            if not reader and not writer:
                raise e.InternalError(f"bad poll status: {s}")
            ev = anyio.Event()
            ready = 0  # type: ignore[assignment]
            async with anyio.create_task_group() as tg:
                if reader:
                    tg.start_soon(readable, ev)
                if writer:
                    tg.start_soon(writable, ev)
                await ev.wait()
                tg.cancel_scope.cancel()  # Move on upon first task done.

            s = gen.send(ready)

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv

    finally:
        sock.close()


async def wait_conn(gen: PQGenConn[RV], timeout: Optional[float] = None) -> RV:
    """
    Coroutine waiting for a connection generator to complete.

    :param gen: a generator performing database operations and yielding
        (fd, `Ready`) pairs when it would block.
    :param timeout: timeout (in seconds) to check for other interrupt, e.g.
        to allow Ctrl-C. If zero or None, wait indefinitely.
    :return: whatever *gen* returns on completion.

    Behave like in `waiting.wait()`, but take the fileno to wait from the
    generator itself, which might change during processing.
    """
    s: Wait
    ready: Ready

    async def readable(sock: socket.socket, ev: anyio.Event) -> None:
        await anyio.wait_socket_readable(sock)
        nonlocal ready
        ready = Ready.R
        ev.set()

    async def writable(sock: socket.socket, ev: anyio.Event) -> None:
        await anyio.wait_socket_writable(sock)
        nonlocal ready
        ready = Ready.W
        ev.set()

    timeout = timeout or None
    try:
        fileno, s = next(gen)

        while True:
            reader = s & Wait.R
            writer = s & Wait.W
            if not reader and not writer:
                raise e.InternalError(f"bad poll status: {s}")
            ev = anyio.Event()
            ready = 0  # type: ignore[assignment]
            with _fromfd(fileno) as sock:
                async with anyio.create_task_group() as tg:
                    if reader:
                        tg.start_soon(readable, sock, ev)
                    if writer:
                        tg.start_soon(writable, sock, ev)
                    with anyio.fail_after(timeout):
                        await ev.wait()

            fileno, s = gen.send(ready)

    except TimeoutError:
        raise e.OperationalError("timeout expired")

    except StopIteration as ex:
        rv: RV = ex.args[0] if ex.args else None
        return rv
