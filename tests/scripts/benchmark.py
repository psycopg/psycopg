#!/usr/bin/env python
"""Micro-benchmarks for psycopg, focused on the wait functions and the copy.

The cost of the wait functions (`psycopg.waiting.wait_async()` for the async
connections, `psycopg.waiting.wait()` for the sync ones) is paid once per
query round trip, and once per row in a copy(), so it shows up in the
throughput of small queries. This script measures it in different ways:

- `time`: wall clock and CPU time of workloads with a different mix of waits.
- `stall`: the longest time a workload keeps the event loop from running other
  tasks, to catch a wait function which never yields when it doesn't block.
- `idle`: CPU burned while *not* doing anything, to catch a wait function
  polling the socket instead of sleeping on it.
- `calls`: how many times the wait function is entered per workload, and how
  often it actually has to block.
- `copy`: time and memory of a large copy from the client, optionally to a
  server slower than the client.

The `time`, `idle` and `copy` commands measure sync connections instead with
`--sync`, using the wait function chosen by psycopg, which can be changed by
setting PSYCOPG_WAIT_FUNC.

Alternative implementations can be measured without patching the library:

    ./benchmark.py time --wait-async mymodule:my_wait_async

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
import json
import socket
import asyncio
import logging
import importlib
import selectors
import statistics
from time import perf_counter, process_time
from typing import Any, Protocol
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from dataclasses import dataclass
from collections.abc import Coroutine

import psycopg
from psycopg import sql, waiting
from psycopg.abc import RV, PQGen, Query

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


async def cmd_time(args: Namespace) -> int:
    """Time the workloads, once per implementation, and report the medians."""
    for workload in get_workloads(args):
        results = await run_rounds(args, workload, ticker=False)
        if args.json:
            report_json("time", workload.name, results)
        else:
            report_time(workload, results, args)

    return 0


async def cmd_stall(args: Namespace) -> int:
    """Measure the longest time the workloads keep the event loop busy.

    A task ticking on the loop runs next to the workload: the longest interval
    between two ticks is how long any other task, a timeout, or a cancellation
    would have had to wait. A wait function serving the socket without going
    through the loop must still yield to it now and then.

    The ticker competes with the workload for the loop, so the times measured
    here are not comparable with the ones of the `time` command.
    """
    if args.sync:
        raise SystemExit("the stall command doesn't support --sync")
    for workload in get_workloads(args):
        results = await run_rounds(args, workload, ticker=True)
        if args.json:
            report_json("stall", workload.name, results)
        else:
            report_stall(workload, results, args)

    return 0


async def run_rounds(
    args: Namespace, workload: Workload, ticker: bool
) -> dict[str, list[Measure]]:
    """Run a workload once per implementation per round, return the measures."""
    impls = get_impls(args)
    results: dict[str, list[Measure]] = {name: [] for name in impls}
    for round_ in range(args.rounds):
        # Rotate the order, so that a drift in the machine's performance
        # doesn't systematically favour the implementation running first.
        names = list(impls)
        shift = round_ % len(names)
        for name in names[shift:] + names[:shift]:
            set_wait_async(impls[name])
            try:
                if args.sync:
                    m = run_workload_sync(args, workload)
                else:
                    m = await run_workload(args, workload, ticker)
                results[name].append(m)
            finally:
                set_wait_async(impls[BUILTIN])

    return results


async def cmd_idle(args: Namespace) -> int:
    """Measure the CPU used while the connection has nothing to say.

    A wait function sleeping on the socket uses almost nothing. One polling it
    in a loop uses a whole core, which no benchmark of a busy connection would
    reveal: this is the check that a faster implementation didn't buy its speed
    with a spin.
    """
    impls = get_impls(args)

    if not args.json:
        logger.info(
            "waiting %s sec on a socket which never becomes readable", args.idle
        )
    results: dict[str, list[Measure]] = {}
    for name, impl in impls.items():
        if args.sync:
            cpu = idle_socket_sync(args.idle)
        else:
            cpu = await idle_socket(impl, args.idle)
        results[name] = [Measure(args.idle, cpu)]
    report_idle_results("socket", results, args)

    if not args.json:
        logger.info("waiting for a pg_sleep(%s) to return", args.idle)
    results = {}
    for name, impl in impls.items():
        set_wait_async(impl)
        try:
            cpu = idle_query_sync(args) if args.sync else await idle_query(args)
        finally:
            set_wait_async(impls[BUILTIN])
        results[name] = [Measure(args.idle, cpu)]
    report_idle_results("query", results, args)

    return 0


async def cmd_copy(args: Namespace) -> int:
    """Measure a large copy from the client, and the memory it uses.

    Copy `--nrows` records of `--ncols` fields, `--colsize` characters each,
    using `write_row()`. `--server-delay` makes the server slower than the
    client, sleeping every 100 records, which shows how much data the client
    buffers.

    The max rss reported is the peak of the whole process: it is only
    meaningful running a single copy (`--rounds 1`, no `--wait-async`).
    """
    results = await run_rounds(args, CopyPut(), ticker=False)
    rss = get_maxrss()
    for measures in results.values():
        for m in measures:
            m.maxrss = rss
    if args.json:
        report_json("copy", CopyPut.name, results)
    else:
        report_time(CopyPut(), results, args)
        if rss is not None:
            logger.info("  max rss: %.1f MB", rss)

    return 0


async def cmd_calls(args: Namespace) -> int:
    """Count how often the wait function is entered, and how often it blocks.

    This tells which workloads are sensitive to the wait function at all. A
    `select 1` enters it once and blocks once; a copy() enters it once per row
    and almost never blocks, so what matters there is the cost of *entering*
    it, not the cost of waiting.
    """
    if args.json:
        raise SystemExit("the calls command doesn't support --json")
    if args.sync:
        raise SystemExit("the calls command doesn't support --sync")
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

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        raise NotImplementedError


class RoundTrips(Workload):
    name = "roundtrips"
    doc = "small queries, one blocking wait each"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        for _ in range(args.nqueries):
            await cur.execute("select 1")

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        for _ in range(args.nqueries):
            cur.execute("select 1")


class CopyOut(Workload):
    name = "copy-out"
    doc = "rows copied out, the wait function entered once per row"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        stmt = "copy (select i, i::text from generate_series(1, %s) i) to stdout"
        async with conn.cursor().copy(stmt, (args.nrows,)) as copy:
            async for _ in copy:
                pass

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        stmt = "copy (select i, i::text from generate_series(1, %s) i) to stdout"
        with conn.cursor().copy(stmt, (args.nrows,)) as copy:
            for _ in copy:
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

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        cur.execute("select i, i::text from generate_series(1, %s) i", (args.nrows,))
        cur.fetchall()


class CopyFrom(Workload):
    name = "copy-in"
    doc = "rows copied in one write each, the wait function entered once per row"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        await conn.execute("create temp table benchtest (i int, s text)")
        async with conn.cursor().copy("copy benchtest from stdin") as copy:
            for i in range(args.nrows):
                await copy.write(f"{i}\t{i}\n")

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        conn.execute("create temp table benchtest (i int, s text)")
        with conn.cursor().copy("copy benchtest from stdin") as copy:
            for i in range(args.nrows):
                copy.write(f"{i}\t{i}\n")


class CopyRows(Workload):
    name = "copy-rows"
    doc = "rows copied in with write_row(), buffered before calling the wait"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        await conn.execute("create temp table benchtest (i int, s text)")
        async with conn.cursor().copy("copy benchtest from stdin") as copy:
            for i in range(args.nrows):
                await copy.write_row((i, str(i)))

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        conn.execute("create temp table benchtest (i int, s text)")
        with conn.cursor().copy("copy benchtest from stdin") as copy:
            for i in range(args.nrows):
                copy.write_row((i, str(i)))


class Pipeline(Workload):
    name = "pipeline"
    doc = "small queries in pipeline mode, the wait function entered at each"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        async with conn.pipeline():
            for _ in range(args.nqueries):
                await cur.execute("select 1")

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        with conn.pipeline():
            for _ in range(args.nqueries):
                cur.execute("select 1")


class ExecuteMany(Workload):
    name = "executemany"
    doc = "small inserts in one executemany(), the wait function entered once"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        await conn.execute("create temp table benchtest (i int, s text)")
        await conn.cursor().executemany(
            "insert into benchtest values (%s, %s)",
            ((i, str(i)) for i in range(args.nmany)),
        )

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        conn.execute("create temp table benchtest (i int, s text)")
        conn.cursor().executemany(
            "insert into benchtest values (%s, %s)",
            ((i, str(i)) for i in range(args.nmany)),
        )


class CopyPut(Workload):
    """A large copy, used by the copy command, not part of the benchmarks."""

    name = "copy"
    doc = "a large copy from the client, with write_row()"

    async def run(self, conn: psycopg.AsyncConnection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        for stmt in copy_setup_stmts(args):
            await cur.execute(stmt)
        writer = make_writer(args, cur, is_async=True)
        record = copy_record(args)
        async with cur.copy(copy_stmt(args), writer=writer) as copy:
            for _ in range(args.nrows):
                await copy.write_row(record)

    def run_sync(self, conn: psycopg.Connection[Any], args: Namespace) -> None:
        cur = conn.cursor()
        for stmt in copy_setup_stmts(args):
            cur.execute(stmt)
        writer = make_writer(args, cur)
        record = copy_record(args)
        with cur.copy(copy_stmt(args), writer=writer) as copy:
            for _ in range(args.nrows):
                copy.write_row(record)


def copy_setup_stmts(args: Namespace) -> list[Query]:
    fields = sql.SQL(", ").join(sql.SQL(f"f{i} text") for i in range(args.ncols))
    rv: list[Query] = [
        sql.SQL("create temp table benchcopy (id serial primary key, {})").format(
            fields
        )
    ]
    if args.server_delay:
        # Make the server slower than the client
        rv.append(sql.SQL("""\
create function pg_temp.benchcopy_delay() returns trigger language plpgsql as $$
begin
    if new.id % 100 = 0 then
        perform pg_sleep({});
    end if;
    return new;
end
$$
""").format(args.server_delay))
        rv.append(sql.SQL("""\
create trigger benchcopy_delay before insert on benchcopy
for each row execute function pg_temp.benchcopy_delay()
"""))
    return rv


def copy_stmt(args: Namespace) -> Query:
    fields = sql.SQL(", ").join(sql.Identifier(f"f{i}") for i in range(args.ncols))
    return sql.SQL("copy benchcopy ({}) from stdin").format(fields)


def copy_record(args: Namespace) -> tuple[Any, ...]:
    return tuple("x" * args.colsize for _ in range(args.ncols))


def make_writer(args: Namespace, cur: Any, is_async: bool = False) -> Any:
    name = args.writer or "LibpqWriter"
    if is_async and not name.startswith("Async"):
        # Use the async version of the writer requested
        name = f"Async{name}"
    cls = getattr(psycopg.copy, name)
    if args.flush_threshold is None:
        return cls(cur)
    else:
        return cls(cur, flush_threshold=args.flush_threshold)


WORKLOADS: list[Workload] = [
    RoundTrips(),
    CopyOut(),
    FetchAll(),
    CopyFrom(),
    CopyRows(),
    Pipeline(),
    ExecuteMany(),
]


def get_workloads(args: Namespace) -> list[Workload]:
    return [w for w in WORKLOADS if not args.workload or w.name in args.workload]


@dataclass
class Measure:
    """The result of running a workload once. Times are in seconds."""

    wall: float
    cpu: float
    # The longest time the ticker couldn't run and how many times it did,
    # if it was running.
    stall: float | None = None
    ticks: int | None = None
    # The peak memory of the process in MB, measured by the copy command.
    maxrss: float | None = None


async def run_workload(
    args: Namespace, workload: Workload, ticker: bool = False
) -> Measure:
    """Run a workload on a fresh connection and measure it."""
    async with await psycopg.AsyncConnection.connect(args.dsn, autocommit=True) as conn:
        # Warm up: the first queries pay for the type lookups and the caches.
        cur = conn.cursor()
        for _ in range(5):
            await cur.execute("select 1")

        if not ticker:
            # Note: perf_counter() and not monotonic(), which until Python 3.13
            # has a 15.625 ms resolution on Windows.
            wall0, cpu0 = perf_counter(), cpu_time()
            await workload.run(conn, args)
            return Measure(perf_counter() - wall0, cpu_time() - cpu0)

        stall = 0.0
        ticks = 0
        running = True

        async def tick() -> None:
            nonlocal stall, ticks
            last = perf_counter()
            while running:
                await asyncio.sleep(0)
                now = perf_counter()
                stall = max(stall, now - last)
                ticks += 1
                last = now

        task = asyncio.ensure_future(tick())
        await asyncio.sleep(0)  # let the ticker start
        try:
            wall0, cpu0 = perf_counter(), cpu_time()
            await workload.run(conn, args)
            wall, cpu = perf_counter() - wall0, cpu_time() - cpu0
        finally:
            running = False
            await task

        return Measure(wall, cpu, stall, ticks)


def run_workload_sync(args: Namespace, workload: Workload) -> Measure:
    """Run a workload on a fresh sync connection and measure it."""
    with psycopg.Connection.connect(args.dsn, autocommit=True) as conn:
        cur = conn.cursor()
        for _ in range(5):
            cur.execute("select 1")

        wall0, cpu0 = perf_counter(), cpu_time()
        workload.run_sync(conn, args)
        return Measure(perf_counter() - wall0, cpu_time() - cpu0)


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


def idle_socket_sync(idle: float) -> float:
    """Return the CPU seconds used by `wait()` on a socket with nothing to read."""
    rs, ws = socket.socketpair()
    rs.setblocking(False)
    try:
        cpu0 = cpu_time()
        try:
            waiting.wait(waiting_forever(), rs.fileno(), 0.1, timeout=idle)
        except psycopg.errors._WaitTimeout:
            pass
        return cpu_time() - cpu0
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


def idle_query_sync(args: Namespace) -> float:
    """Return the CPU seconds used waiting for a slow query, on a sync connection."""
    with psycopg.Connection.connect(args.dsn, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("select 1")  # warm up
        cpu0 = cpu_time()
        cur.execute("select pg_sleep(%s)", (args.idle,))
        return cpu_time() - cpu0


def get_maxrss() -> float | None:
    """Return the peak memory used by the process in MB, if available."""
    try:
        import resource
    except ImportError:  # Windows
        try:
            import psutil  # type: ignore[import-untyped,unused-ignore]
        except ImportError:
            return None
        rss: int = psutil.Process().memory_info().peak_wset // 1024
    else:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Reported in KB on Linux, bytes on macOS
        if sys.platform == "darwin":
            rss //= 1024
    return rss / 1024


def cpu_time() -> float:
    """Return the CPU time used by this process, in seconds."""
    # Note: not resource.getrusage(), which is not available on Windows.
    return process_time()


def report_time(
    workload: Workload, results: dict[str, list[Measure]], args: Namespace
) -> None:
    """Print the median of the measures, relative to the builtin function."""
    logger.info(
        "%s: %s (median of %s rounds)", workload.name, workload.doc, args.rounds
    )
    walls = {name: [m.wall for m in ms] for name, ms in results.items()}
    base = statistics.median(walls[BUILTIN])
    for name, measures in results.items():
        wall = statistics.median(walls[name])
        cpu = statistics.median(m.cpu for m in measures)
        logger.info(
            "  %-40s %9.2f ms %+7.1f%% cpu %9.2f ms min %9.2f ms",
            name,
            wall * 1000.0,
            (wall / base - 1.0) * 100.0,
            cpu * 1000.0,
            min(walls[name]) * 1000.0,
        )


def report_stall(
    workload: Workload, results: dict[str, list[Measure]], args: Namespace
) -> None:
    """Print the longest stall of the loop, median and worst of the rounds."""
    logger.info(
        "%s: %s (median of %s rounds)", workload.name, workload.doc, args.rounds
    )
    for name, measures in results.items():
        stalls = [m.stall or 0.0 for m in measures]
        logger.info(
            "  %-40s max stall %9.2f ms worst %9.2f ms ticks %9d in %9.2f ms",
            name,
            statistics.median(stalls) * 1000.0,
            max(stalls) * 1000.0,
            statistics.median(m.ticks or 0 for m in measures),
            statistics.median(m.wall for m in measures) * 1000.0,
        )


def report_json(command: str, workload: str, results: dict[str, list[Measure]]) -> None:
    """Print all the measures as a line of JSON, for other scripts to parse."""
    for name, measures in results.items():
        print(
            json.dumps(
                {
                    "command": command,
                    "workload": workload,
                    "impl": name,
                    "measures": [m.__dict__ for m in measures],
                }
            ),
            flush=True,
        )


def report_idle_results(
    what: str, results: dict[str, list[Measure]], args: Namespace
) -> None:
    if args.json:
        report_json("idle", what, results)
    else:
        for name, (m,) in results.items():
            report_idle(name, m.cpu, args)


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
        help="queries in the roundtrips and pipeline workloads [default: %(default)s]",
    )
    common.add_argument(
        "--nrows",
        type=int,
        default=100_000,
        help="rows in the copy and fetch-all workloads [default: %(default)s]",
    )
    common.add_argument(
        "--ncols",
        type=int,
        default=10,
        help="columns copied by the copy command [default: %(default)s]",
    )
    common.add_argument(
        "--colsize",
        type=int,
        default=10,
        help="width of the columns copied by the copy command [default: %(default)s]",
    )
    common.add_argument(
        "--server-delay",
        type=float,
        default=0.0,
        help="seconds the server sleeps every 100 rows in the copy command"
        " [default: %(default)s]",
    )
    common.add_argument(
        "--writer",
        help="the copy writer used by the copy command [default: LibpqWriter]",
    )
    common.add_argument(
        "--flush-threshold",
        type=int,
        help="bytes the copy command sends before waiting for a flush"
        " [default: the writer default]",
    )
    common.add_argument(
        "--nmany",
        type=int,
        default=20_000,
        help="rows in the executemany workload [default: %(default)s]",
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
    common.add_argument(
        "--sync",
        action="store_true",
        help="measure sync connections instead of async ones (time, idle and copy)",
    )
    common.add_argument(
        "--json",
        action="store_true",
        help="print the measures as JSON lines on stdout, for other scripts",
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
        ("time", cmd_time),
        ("stall", cmd_stall),
        ("idle", cmd_idle),
        ("calls", cmd_calls),
        ("copy", cmd_copy),
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
    if args.sync and args.wait_async:
        parser.error("--wait-async can't be used with --sync")
    if args.writer and not hasattr(psycopg.copy, args.writer):
        parser.error(f"unknown writer: {args.writer!r}")
    for spec in args.wait_async:
        try:
            import_wait_async(spec)
        except (ImportError, ValueError) as ex:
            parser.error(str(ex))

    return args


if __name__ == "__main__":
    sys.exit(main())
