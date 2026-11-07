#!/usr/bin/env python
"""Micro-benchmarks for the async wait functions.

The cost of `psycopg.waiting.wait_async()` is paid once per query round trip,
and several times during a copy(), so it shows up in the throughput of small
queries. This script measures it in three ways:

- `bench`: wall clock and CPU time of workloads with a different mix of waits.
- `idle`: CPU burned while *not* doing anything, to catch a wait function
  polling the socket instead of sleeping on it.
- `calls`: how many times the wait function is entered per workload, and how
  often it actually has to block.

Alternative implementations can be measured without patching the library:

    ./waittest.py bench --wait-async mymodule:my_wait_async

Passing more than one `--wait-async` compares them all with the builtin one:
each workload is run once per implementation per round, rotating the order
every round so that a machine slowing down doesn't favour whichever runs
first, and the median is reported.

BEWARE: these numbers are only comparable within a single run on a single
machine. They depend on the event loop, on the Python version, and very much
on whether the server is reached through a Unix socket, localhost or a real
network: the faster the connection, the more the wait function weighs. Don't
compare them across machines and don't read much into a single round.

Not every workload is sensitive to the wait function: `fetch-all` usually
isn't, because parsing the rows dwarfs the waiting, so expect its numbers to
wander between runs.
"""

from __future__ import annotations

import sys
import socket
import asyncio
import logging
import importlib
import selectors
import statistics
from time import perf_counter, process_time
from typing import Any, Protocol
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from collections.abc import Coroutine

import psycopg
from psycopg import waiting
from psycopg.abc import RV, PQGen

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# The name the builtin function is reported under, and what the alternative
# implementations are compared against.
BUILTIN = "builtin"


class AsyncWaitFunc(Protocol):
    """What `--wait-async` must point to: an async `psycopg.abc.WaitFunc`."""

    def __call__(
        self,
        gen: PQGen[RV],
        fileno: int,
        interval: float = ...,
        timeout: float | None = ...,
    ) -> Coroutine[Any, Any, RV]: ...


def main() -> int:
    args = parse_cmdline()
    logger.setLevel(args.loglevel)

    # Psycopg doesn't work with the ProactorEventLoop, the default on Windows,
    # because it doesn't implement loop.add_reader().
    # Loop policies are deprecated from Python 3.14, loop_factory was
    # introduced in Python 3.12.
    kwargs: dict[str, Any] = {}
    if sys.platform == "win32":
        if sys.version_info >= (3, 12):
            kwargs["loop_factory"] = lambda: asyncio.SelectorEventLoop(
                selectors.SelectSelector()
            )
        else:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    rv: int = asyncio.run(args.func(args), **kwargs)
    return rv


async def cmd_bench(args: Namespace) -> int:
    """Time the workloads, once per implementation, and report the medians."""
    impls = get_impls(args)

    for workload in get_workloads(args):
        results: dict[str, list[tuple[float, float]]] = {name: [] for name in impls}
        for round_ in range(args.rounds):
            # Rotate the order, so that a drift in the machine's performance
            # doesn't systematically favour the implementation running first.
            names = list(impls)
            shift = round_ % len(names)
            for name in names[shift:] + names[:shift]:
                set_wait_async(impls[name])
                try:
                    results[name].append(await run_workload(args, workload))
                finally:
                    set_wait_async(impls[BUILTIN])

        report_bench(workload, results, args)

    return 0


async def cmd_idle(args: Namespace) -> int:
    """Measure the CPU used while the connection has nothing to say.

    A wait function sleeping on the socket uses almost nothing. One polling it
    in a loop uses a whole core, which no benchmark of a busy connection would
    reveal: this is the check that a faster implementation didn't buy its speed
    with a spin.
    """
    impls = get_impls(args)

    logger.info("waiting %s sec on a socket which never becomes readable", args.idle)
    for name, impl in impls.items():
        report_idle(name, await idle_socket(impl, args.idle), args)

    logger.info("waiting for a pg_sleep(%s) to return", args.idle)
    for name, impl in impls.items():
        set_wait_async(impl)
        try:
            cpu = await idle_query(args)
        finally:
            set_wait_async(impls[BUILTIN])
        report_idle(name, cpu, args)

    return 0


async def cmd_calls(args: Namespace) -> int:
    """Count how often the wait function is entered, and how often it blocks.

    This tells which workloads are sensitive to the wait function at all. A
    `select 1` enters it once and blocks once; a copy() enters it once per row
    and almost never blocks, so what matters there is the cost of *entering*
    it, not the cost of waiting.
    """
    counter = CallCounter(waiting.wait_async)
    set_wait_async(counter)
    try:
        for workload in get_workloads(args):
            counter.reset()
            await run_workload(args, workload)
            report_calls(workload, counter)
    finally:
        set_wait_async(counter.wrapped)

    return 0


def set_wait_async(impl: AsyncWaitFunc) -> None:
    """Replace the wait function used by the async connections."""
    waiting.wait_async = impl


class CallCounter:
    """Wrap a wait function, counting the calls and the waits within each."""

    def __init__(self, wrapped: AsyncWaitFunc):
        self.wrapped = wrapped
        self.reset()

    def reset(self) -> None:
        self.calls = 0
        self.waits: list[int] = []

    async def __call__(
        self,
        gen: PQGen[RV],
        fileno: int,
        interval: float = 0.0,
        timeout: float | None = None,
    ) -> RV:
        nwaits = 0

        def counting() -> PQGen[RV]:
            # Relay whatever the wrapped generator yields, counting the blocks.
            nonlocal nwaits
            try:
                s = next(gen)
                while True:
                    nwaits += 1
                    ready = yield s
                    s = gen.send(ready)
            except StopIteration as ex:
                rv: RV = ex.value
                return rv

        self.calls += 1
        try:
            rv: RV = await self.wrapped(counting(), fileno, interval, timeout)
            return rv
        finally:
            self.waits.append(nwaits)


class Workload:
    """A unit of work with a characteristic mix of waits."""

    name = "-"
    doc = "-"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        raise NotImplementedError


class RoundTrips(Workload):
    name = "roundtrips"
    doc = "small queries, one blocking wait each"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        for _ in range(args.nqueries):
            await cur.execute("select 1")


class CopyOut(Workload):
    name = "copy-out"
    doc = "rows copied out, the wait function entered once per row"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        stmt = "copy (select i, i::text from generate_series(1, %s) i) to stdout"
        async with conn.cursor().copy(stmt, (args.nrows,)) as copy:
            async for _ in copy:
                pass


class FetchAll(Workload):
    name = "fetch-all"
    doc = "one large result set, usually dwarfed by parsing the rows"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        await cur.execute(
            "select i, i::text from generate_series(1, %s) i", (args.nrows,)
        )
        await cur.fetchall()


WORKLOADS: list[Workload] = [RoundTrips(), CopyOut(), FetchAll()]


def get_workloads(args: Namespace) -> list[Workload]:
    return [w for w in WORKLOADS if not args.workload or w.name in args.workload]


async def run_workload(args: Namespace, workload: Workload) -> tuple[float, float]:
    """Run a workload on a fresh connection, returning (wall, cpu) seconds."""
    async with await psycopg.AsyncConnection.connect(args.dsn, autocommit=True) as conn:
        # Warm up: the first queries pay for the type lookups and the caches.
        cur = conn.cursor()
        for _ in range(5):
            await cur.execute("select 1")

        # Note: perf_counter() and not monotonic(), which until Python 3.13
        # has a 15.625 ms resolution on Windows.
        wall0, cpu0 = perf_counter(), cpu_time()
        await workload.run(conn, args)
        return perf_counter() - wall0, cpu_time() - cpu0


async def idle_socket(impl: AsyncWaitFunc, idle: float) -> float:
    """Return the CPU seconds used waiting on a socket with nothing to read.

    Note that the socket *is* write-ready, as a connection's socket is after
    flushing a query, so a wait function registering both directions
    unconditionally will be woken up by it.
    """
    rs, ws = socket.socketpair()
    rs.setblocking(False)
    try:
        task = asyncio.ensure_future(impl(waiting_forever(), rs.fileno(), 0.1))
        try:
            cpu0 = cpu_time()
            await asyncio.sleep(idle)
            return cpu_time() - cpu0
        finally:
            task.cancel()
            try:
                await task
            except BaseException:
                pass
    finally:
        rs.close()
        ws.close()


def waiting_forever() -> PQGen[None]:
    """A generator waiting to read, for as long as it is resumed."""
    while True:
        yield waiting.Wait.R


async def idle_query(args: Namespace) -> float:
    """Return the CPU seconds used waiting for a slow query to return."""
    async with await psycopg.AsyncConnection.connect(args.dsn, autocommit=True) as conn:
        cur = conn.cursor()
        await cur.execute("select 1")  # warm up
        cpu0 = cpu_time()
        await cur.execute("select pg_sleep(%s)", (args.idle,))
        return cpu_time() - cpu0


def cpu_time() -> float:
    """Return the CPU time used by this process, in seconds."""
    # Note: not resource.getrusage(), which is not available on Windows.
    return process_time()


def report_bench(
    workload: Workload,
    results: dict[str, list[tuple[float, float]]],
    args: Namespace,
) -> None:
    """Print the median of the measures, relative to the builtin function."""
    logger.info(
        "%s: %s (median of %s rounds)", workload.name, workload.doc, args.rounds
    )
    walls = {name: [w for w, _ in ms] for name, ms in results.items()}
    base = statistics.median(walls[BUILTIN])
    for name, measures in results.items():
        wall = statistics.median(walls[name])
        cpu = statistics.median(c for _, c in measures)
        logger.info(
            "  %-40s %9.2f ms %+7.1f%% cpu %9.2f ms min %9.2f ms",
            name,
            wall * 1000.0,
            (wall / base - 1.0) * 100.0,
            cpu * 1000.0,
            min(walls[name]) * 1000.0,
        )


def report_idle(name: str, cpu: float, args: Namespace) -> None:
    logger.info(
        "  %-40s %9.1f ms CPU (%.1f%% of a core)",
        name,
        cpu * 1000.0,
        cpu / args.idle * 100.0,
    )


def report_calls(workload: Workload, counter: CallCounter) -> None:
    nwaits = sum(counter.waits)
    blocked = sum(1 for w in counter.waits if w)
    logger.info(
        "%-12s %7d calls, %7d waits, %.3f waits/call,"
        " blocked in %.1f%% of the calls, max %d",
        workload.name,
        counter.calls,
        nwaits,
        nwaits / counter.calls if counter.calls else 0.0,
        100.0 * blocked / counter.calls if counter.calls else 0.0,
        max(counter.waits, default=0),
    )


def get_impls(args: Namespace) -> dict[str, AsyncWaitFunc]:
    """Return the wait functions to measure, the builtin one first."""
    impls: dict[str, AsyncWaitFunc] = {BUILTIN: waiting.wait_async}
    for spec in args.wait_async:
        impls[spec] = import_wait_async(spec)
    return impls


def import_wait_async(spec: str) -> AsyncWaitFunc:
    """Import a wait function from a "module:function" specification."""
    modname, _, funcname = spec.partition(":")
    if not (modname and funcname):
        raise ValueError(f"not a 'module:function' specification: {spec!r}")

    mod = importlib.import_module(modname)
    try:
        func: AsyncWaitFunc = getattr(mod, funcname)
    except AttributeError:
        raise ValueError(f"no function {funcname!r} in module {modname!r}") from None

    return func


def parse_cmdline() -> Namespace:
    # The options are on the subcommands, so that they can be passed after
    # them, which is the order people expect.
    common = ArgumentParser(add_help=False)
    common.add_argument("--dsn", default="", help="database connection string")
    common.add_argument(
        "--wait-async",
        metavar="MODULE:FUNCTION",
        action="append",
        default=[],
        help="an alternative wait function to measure (can be repeated)",
    )
    common.add_argument(
        "--rounds",
        type=int,
        default=11,
        help="how many times to repeat each measure [default: %(default)s]",
    )
    common.add_argument(
        "--nqueries",
        type=int,
        default=2000,
        help="queries in the roundtrips workload [default: %(default)s]",
    )
    common.add_argument(
        "--nrows",
        type=int,
        default=100_000,
        help="rows in the copy-out and fetch-all workloads [default: %(default)s]",
    )
    common.add_argument(
        "--idle",
        type=float,
        default=1.0,
        help="seconds to stay idle in the idle command [default: %(default)s]",
    )
    common.add_argument(
        "--workload",
        action="append",
        default=[],
        choices=[w.name for w in WORKLOADS],
        help="only measure this workload (can be repeated)",
    )

    g = common.add_mutually_exclusive_group()
    g.add_argument(
        "-q",
        "--quiet",
        help="Talk less",
        dest="loglevel",
        action="store_const",
        const=logging.WARN,
        default=logging.INFO,
    )
    g.add_argument(
        "-v",
        "--verbose",
        help="Talk more",
        dest="loglevel",
        action="store_const",
        const=logging.DEBUG,
        default=logging.INFO,
    )

    parser = ArgumentParser(
        description=__doc__, formatter_class=RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name, func in [
        ("bench", cmd_bench),
        ("idle", cmd_idle),
        ("calls", cmd_calls),
    ]:
        doc = func.__doc__ or ""
        p = sub.add_parser(
            name,
            parents=[common],
            help=doc.split("\n")[0],
            description=doc,
            formatter_class=RawDescriptionHelpFormatter,
        )
        p.set_defaults(func=func)

    args = parser.parse_args()
    for spec in args.wait_async:
        try:
            import_wait_async(spec)
        except (ImportError, ValueError) as ex:
            parser.error(str(ex))

    return args


if __name__ == "__main__":
    sys.exit(main())
