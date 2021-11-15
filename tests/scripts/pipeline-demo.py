"""Pipeline mode demo

This reproduces libpq_pipeline::pipelined_insert PostgreSQL test at
src/test/modules/libpq_pipeline/libpq_pipeline.c::test_pipelined_insert().

We do not fetch results explicitly (using cursor.fetch*()), this is
handled by execute() calls when pgconn socket is read-ready, which
happens when the output buffer is full.
"""
import argparse
import asyncio
import logging

from psycopg import AsyncConnection, Connection


def pipeline_demo(rows_to_send: int) -> None:
    conn = Connection.connect()
    conn.autocommit = True
    with conn.pipeline() as pipeline:
        with conn.transaction():
            conn.execute("DROP TABLE IF EXISTS pq_pipeline_demo")
            conn.execute(
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            )
            for r in range(rows_to_send, 0, -1):
                conn.execute(
                    "INSERT INTO pq_pipeline_demo(itemno, int8filler)"
                    " VALUES (%s, %s)",
                    (r, 1 << 62),
                )
        pipeline.sync()


async def pipeline_demo_async(rows_to_send: int) -> None:
    aconn = await AsyncConnection.connect()
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        async with aconn.transaction():
            await aconn.execute("DROP TABLE IF EXISTS pq_pipeline_demo")
            await aconn.execute(
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            )
            for r in range(rows_to_send, 0, -1):
                await aconn.execute(
                    "INSERT INTO pq_pipeline_demo(itemno, int8filler)"
                    " VALUES (%s, %s)",
                    (r, 1 << 62),
                )
        await pipeline.sync()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        dest="nrows",
        metavar="ROWS",
        default=10_000,
        type=int,
        help="number of rows to insert",
    )
    parser.add_argument(
        "--async", dest="async_", action="store_true", help="use async API"
    )
    parser.add_argument("-l", "--log", help="log file (stderr by default)")
    args = parser.parse_args()
    logger = logging.getLogger("psycopg")
    logger.setLevel(logging.DEBUG)
    if args.log:
        logger.addHandler(logging.FileHandler(args.log))
    else:
        logger.addHandler(logging.StreamHandler())
    if args.async_:
        asyncio.run(pipeline_demo_async(args.nrows))
    else:
        pipeline_demo(args.nrows)


if __name__ == "__main__":
    main()
