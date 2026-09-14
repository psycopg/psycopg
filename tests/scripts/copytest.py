#!/usr/bin/env python
"""Copy operation micro-benchmarks."""

from __future__ import annotations

import sys
import pstats
import asyncio
import logging
import cProfile
import statistics
from time import perf_counter
from typing import Any
from argparse import ArgumentParser, BooleanOptionalAction, Namespace
from contextlib import nullcontext

import psycopg
from psycopg import sql
from psycopg.abc import Query

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

pr = cProfile.Profile()


def main():
    args = parse_cmdline()
    logger.setLevel(args.loglevel)

    if getattr(args, "async"):
        t_ins, t_outs, t_inserts, t_selects = asyncio.run(main_async(args))
    else:
        t_ins, t_outs, t_inserts, t_selects = main_sync(args)

    if args.executemany:
        execute_descr = "executemany"
    else:
        execute_descr = "execute"
        if args.pipeline:
            execute_descr = "pipelined execute"
        else:
            execute_descr = "execute"

    if t_ins:
        output_timings(args, t_ins, "copy in")
    if t_outs:
        output_timings(args, t_outs, "copy out")
    if t_inserts:
        output_timings(args, t_inserts, f"{execute_descr} insert")
    if t_selects:
        output_timings(args, t_selects, f"{execute_descr} select")

    if args.cprofile:
        pr.dump_stats("output.prof")
        stats = pstats.Stats(pr)
        stats.sort_stats("cumulative")
        stats.print_stats(10)

    with psycopg.Connection.connect(args.dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS testcopy")


def main_sync(args: Namespace) -> tuple[list[float], ...]:
    test = CopyPutTest(args)
    t_ins, t_outs, t_inserts, t_selects = [], [], [], []
    if args.executemany:
        insert = executemany_insert_sync
        select = executemany_select_sync
    else:
        insert = execute_insert_sync
        select = execute_select_sync
    with psycopg.Connection.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(test.get_table_stmt())
            n_in, n_out, n_insert, n_select = get_effective_repeats(args)
            for i in range(n_insert):
                t_insert = insert(args, test, cur)
                t_inserts.append(t_insert)
            for i in range(n_in):
                t_in = copy_in_sync(args, test, cur)
                if args.copy_in:
                    t_ins.append(t_in)
                if i != n_in - 1:
                    cur.execute(test.get_truncate_stmt())
            for i in range(n_select):
                t_select = select(args, test, cur)
                t_selects.append(t_select)
            for i in range(n_out):
                t_out = copy_out_sync(args, test, cur)
                t_outs.append(t_out)
    return t_ins, t_outs, t_inserts, t_selects


async def main_async(args: Namespace) -> tuple[list[float], ...]:
    test = CopyPutTest(args)
    t_ins, t_outs, t_inserts, t_selects = [], [], [], []
    if args.executemany:
        insert = executemany_insert_async
        select = executemany_select_async
    else:
        insert = execute_insert_async
        select = execute_select_async
    async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(test.get_table_stmt())
            n_in, n_out, n_insert, n_select = get_effective_repeats(args)
            for i in range(n_insert):
                t_insert = await insert(args, test, cur)
                t_inserts.append(t_insert)
                await cur.execute(test.get_truncate_stmt())
            for i in range(n_in):
                t_in = await copy_in_async(args, test, cur)
                if args.copy_in:
                    t_ins.append(t_in)
                if not i == n_in - 1:
                    await cur.execute(test.get_truncate_stmt())
            for i in range(n_select):
                t_select = await select(args, test, cur)
                t_selects.append(t_select)
            for i in range(n_out):
                t_out = await copy_out_async(args, test, cur)
                t_outs.append(t_out)
    return t_ins, t_outs, t_inserts, t_selects


def get_effective_repeats(args):
    # copy in always runs once or we have nothing to copy out
    n_in = args.repeat if args.copy_in else 1 if args.copy_out or args.select else 0
    n_out = args.repeat if args.copy_out else 0
    n_insert = args.repeat if args.insert else 0
    n_select = args.repeat if args.select else 0

    if all(r == 0 for r in (n_in, n_out, n_insert, n_select)):
        print("Nothing to do.")
    return n_in, n_out, n_insert, n_select


def output_timings(args, timings, description):
    if args.repeat > 1:
        logger.info(
            f"time to {description}: %.6f sec [%.6f +/- %.6f from %i results]",
            *get_statistics(timings),
        )
    else:
        logger.info(f"time to {description}: %.6f sec", timings[0])


def get_statistics(timings):
    orig_timings = timings
    min_val = min(timings)
    mean_val = statistics.mean(timings)
    stddev = statistics.stdev(timings)

    if len(timings) <= 4:
        return (min_val, mean_val, stddev, len(timings))

    # trim upper outliers
    robust_timings = [t for t in timings if t <= statistics.quantiles(timings)[-1]]
    threshold = statistics.mean(robust_timings) + 3 * statistics.stdev(robust_timings)
    while max(timings) > threshold:
        timings = [t for t in timings if t <= threshold]
        if len(timings) < 4:
            logger.warning(
                "Not trimming outliers as it would result in less than 4 timings"
            )
            timings = orig_timings
            break
        mean_val = statistics.mean(timings)
        stddev = statistics.stdev(timings)
        threshold = mean_val + 3 * stddev

    return (min_val, mean_val, stddev, len(timings))


def copy_in_sync(args, test, cur):
    writer = getattr(psycopg.copy, args.writer)(cur) if args.writer else None
    with cur.copy(test.get_copy_stmt(), writer=writer) as copy:
        if args.set_types:
            copy.set_types(["text"] * args.nfields)
        records = [test.get_record() for _ in range(args.nrecs)]
        t0 = perf_counter()
        if args.copy_in and args.cprofile:
            pr.enable()
        for record in records:
            copy.write_row(record)
        if args.copy_in and args.cprofile:
            pr.disable()
        tf = perf_counter()

    return tf - t0


def copy_out_sync(args, test, cur):
    with cur.copy(test.get_copy_out_stmt()) as copy:
        if args.set_types:
            copy.set_types(["int4"] + ["text"] * args.nfields)
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()
        while copy.read_row():
            pass
        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


def executemany_insert_sync(args, test, cur):
    insert = test.get_insert_stmt()
    params = [test.get_record() for _ in range(args.nrecs)]
    t0 = perf_counter()
    if args.cprofile:
        pr.enable()
    cur.executemany(insert, params)
    if args.cprofile:
        pr.disable()
    tf = perf_counter()
    return tf - t0


def executemany_select_sync(args, test, cur):
    select = test.get_select_stmt()
    params = test.get_record_ids(cur)
    t0 = perf_counter()
    if args.cprofile:
        pr.enable()
    cur.executemany(select, params)
    if args.cprofile:
        pr.disable()
    tf = perf_counter()
    return tf - t0


def execute_insert_sync(args, test, cur):
    if args.pipeline:
        context = cur.connection.pipeline()
    else:
        context = nullcontext()
    insert = test.get_insert_stmt()
    params = [test.get_record() for _ in range(args.nrecs)]
    with context as pipeline:
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()

        for param in params:
            cur.execute(insert, param)
        if pipeline is not None:
            pipeline.sync()

        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


def execute_select_sync(args, test, cur):
    if args.pipeline:
        context = cur.connection.pipeline()
    else:
        context = nullcontext()
    select = test.get_select_stmt()
    params = test.get_record_ids(cur)
    with context as pipeline:
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()

        for param in params:
            cur.execute(select, param)
        if pipeline is not None:
            pipeline.sync()

        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def copy_in_async(args, test, cur):
    writer = getattr(psycopg.copy, args.writer)(cur) if args.writer else None
    async with cur.copy(test.get_copy_stmt(), writer=writer) as copy:
        if args.set_types:
            copy.set_types(["text"] * args.nfields)
        records = [test.get_record() for _ in range(args.nrecs)]
        t0 = perf_counter()
        if args.copy_in and args.cprofile:
            pr.enable()
        for record in records:
            await copy.write_row(record)
        if args.copy_in and args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def copy_out_async(args, test, cur):
    async with cur.copy(test.get_copy_out_stmt()) as copy:
        if args.set_types:
            copy.set_types(["int4"] + ["text"] * args.nfields)
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()
        while await copy.read_row():
            pass
        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def executemany_insert_async(args, test, cur):
    insert = test.get_insert_stmt()
    params = [test.get_record() for _ in range(args.nrecs)]
    t0 = perf_counter()
    if args.cprofile:
        pr.enable()
    await cur.executemany(insert, params)
    if args.cprofile:
        pr.disable()
    tf = perf_counter()
    return tf - t0


async def executemany_select_async(args, test, cur):
    select = test.get_select_stmt()
    params = await test.get_record_ids_async(cur)
    t0 = perf_counter()
    if args.cprofile:
        pr.enable()
    await cur.executemany(select, params)
    if args.cprofile:
        pr.disable()
    tf = perf_counter()
    return tf - t0


async def execute_insert_async(args, test, cur):
    if args.pipeline:
        context = cur.connection.pipeline()
    else:
        context = nullcontext()
    insert = test.get_insert_stmt()
    params = [test.get_record() for _ in range(args.nrecs)]
    async with context as pipeline:
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()

        for param in params:
            await cur.execute(insert, param)
        if pipeline is not None:
            await pipeline.sync()

        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


async def execute_select_async(args, test, cur):
    if args.pipeline:
        context = cur.connection.pipeline()
    else:
        context = nullcontext()
    select = test.get_select_stmt()
    params = await test.get_record_ids_async(cur)
    async with context as pipeline:
        t0 = perf_counter()
        if args.cprofile:
            pr.enable()

        for param in params:
            await cur.execute(select, param)
        if pipeline is not None:
            await pipeline.sync()

        if args.cprofile:
            pr.disable()
        tf = perf_counter()
    return tf - t0


class CopyPutTest:
    def __init__(self, args: Namespace):
        self.args = args

    def get_table_stmt(self) -> Query:
        fields = sql.SQL(", ").join(
            [sql.SQL(f"f{i} text") for i in range(self.args.nfields)]
        )
        stmt = sql.SQL("""\
create temp table testcopy (id serial primary key, {})
""").format(fields)
        return stmt

    def get_copy_stmt(self) -> Query:
        fields = sql.SQL(", ").join(
            [sql.Identifier(f"f{i}") for i in range(self.args.nfields)]
        )
        stmt = sql.SQL("""\
copy testcopy ({}) from stdin{}
""").format(fields, sql.SQL(" WITH (FORMAT BINARY)" if self.args.binary else ""))
        return stmt

    def get_select_stmt(self) -> Query:
        stmt = sql.SQL("""\
SELECT * FROM testcopy WHERE id = {}
""").format(sql.SQL("%b" if self.args.binary else "%t"))
        return stmt

    def get_insert_stmt(self) -> Query:
        fields = sql.SQL(", ").join(
            [sql.Identifier(f"f{i}") for i in range(self.args.nfields)]
        )
        formatter = sql.SQL("%b" if self.args.binary else "%t")
        stmt = sql.SQL("""\
INSERT INTO testcopy ({}) VALUES ({})
""").format(
            fields,
            sql.SQL(", ").join(formatter for _ in range(self.args.nfields)),
        )
        return stmt

    def get_truncate_stmt(self) -> Query:
        return sql.SQL("TRUNCATE testcopy")

    def get_copy_out_stmt(self) -> Query:
        stmt = sql.SQL("""\
COPY testcopy TO STDOUT{}
""").format(sql.SQL(" WITH (FORMAT BINARY)" if self.args.binary else ""))
        return stmt

    def get_record(self) -> tuple[Any, ...]:
        return tuple("x" * self.args.colsize for _ in range(self.args.nfields))

    def get_record_ids(self, cur: psycopg.Cursor) -> list[tuple[int]]:
        cur.execute("SELECT id FROM testcopy")
        return cur.fetchall()

    async def get_record_ids_async(self, cur: psycopg.AsyncCursor) -> list[tuple[int]]:
        await cur.execute("SELECT id FROM testcopy")
        return await cur.fetchall()


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="", help="database connection string")
    parser.add_argument(
        "--async", action="store_true", default=False, help="test async objects"
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="number of repeats [default: %(default)s]",
    )
    parser.add_argument(
        "--cprofile",
        action=BooleanOptionalAction,
        default=False,
        help="output cProfile information and save profile to output.prof",
    )
    parser.add_argument(
        "--copy-in",
        action=BooleanOptionalAction,
        default=True,
        help="output copy in timings and profile",
    )
    parser.add_argument(
        "--copy-out",
        action=BooleanOptionalAction,
        default=False,
        help="output copy out timings and profile",
    )
    parser.add_argument(
        "--insert",
        action=BooleanOptionalAction,
        default=False,
        help="output insert timings and profile",
    )
    parser.add_argument(
        "--select",
        action=BooleanOptionalAction,
        default=False,
        help="output select timings and profile",
    )
    parser.add_argument(
        "--pipeline",
        action=BooleanOptionalAction,
        default=False,
        help="use pipeline mode for --select and --insert with --no-executemany",
    )
    parser.add_argument(
        "--executemany",
        action=BooleanOptionalAction,
        default=True,
        help="use executemany instead of execute",
    )
    parser.add_argument(
        "--binary",
        action=BooleanOptionalAction,
        default=False,
        help="binary or text output format",
    )
    parser.add_argument(
        "--set-types",
        action=BooleanOptionalAction,
        default=False,
        help="binary or text output format",
    )
    parser.add_argument(
        "--nrecs",
        type=int,
        default=1000,
        help="number of records to write [default: %(default)s]",
    )
    parser.add_argument(
        "--nfields",
        type=int,
        default=10,
        help="number of columns to write [default: %(default)s]",
    )
    parser.add_argument(
        "--colsize",
        type=int,
        default=10,
        help="width of each column to write [default: %(default)s]",
    )
    parser.add_argument("--writer", help="test alternative writer")

    g = parser.add_mutually_exclusive_group()
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

    if (args := parser.parse_args()).writer:
        try:
            getattr(psycopg.copy, args.writer)
        except AttributeError:
            parser.error(f"unknown writer: {args.writer!r}")

    return args


if __name__ == "__main__":
    sys.exit(main())
