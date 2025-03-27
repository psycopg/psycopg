#!/usr/bin/env python
"""Copy operation micro-benchmarks."""
from __future__ import annotations

import sys
import asyncio
import logging
from time import time
from typing import Any
from argparse import ArgumentParser, Namespace

import psycopg
from psycopg import sql
from psycopg.abc import Query

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def main():
    args = parse_cmdline()
    logger.setLevel(args.loglevel)

    if getattr(args, "async"):
        asyncio.run(main_async(args))
    else:
        main_sync(args)


def main_sync(args: Namespace) -> None:
    test = CopyPutTest(args)
    with psycopg.Connection.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            writer = getattr(psycopg.copy, args.writer)(cur) if args.writer else None
            cur.execute(test.get_table_stmt())
            t0 = time()
            with cur.copy(test.get_copy_stmt(), writer=writer) as copy:
                for i in range(args.nrecs):
                    copy.write_row(test.get_record())
            tf = time()

    logger.info("time to copy: %.6f sec", tf - t0)


async def main_async(args: Namespace) -> None:
    test = CopyPutTest(args)
    async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(test.get_table_stmt())
            writer = getattr(psycopg.copy, args.writer)(cur) if args.writer else None
            t0 = time()
            async with cur.copy(test.get_copy_stmt(), writer=writer) as copy:
                for i in range(args.nrecs):
                    await copy.write_row(test.get_record())
            tf = time()

    logger.info("time to copy: %.6f sec", tf - t0)


class CopyPutTest:
    def __init__(self, args: Namespace):
        self.args = args

    def get_table_stmt(self) -> Query:
        fields = sql.SQL(", ").join(
            [sql.SQL(f"f{i} text") for i in range(self.args.nfields)]
        )
        stmt = sql.SQL(
            """\
create temp table testcopy (id serial primary key, {})
"""
        ).format(fields)
        return stmt

    def get_copy_stmt(self) -> Query:
        fields = sql.SQL(", ").join(
            [sql.Identifier(f"f{i}") for i in range(self.args.nfields)]
        )
        stmt = sql.SQL(
            """\
copy testcopy ({}) from stdin
"""
        ).format(fields)
        return stmt

    def get_record(self) -> tuple[Any, ...]:
        return tuple("x" * self.args.colsize for _ in range(self.args.nfields))


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="", help="database connection string")
    parser.add_argument(
        "--async", action="store_true", default=False, help="test async objects"
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
