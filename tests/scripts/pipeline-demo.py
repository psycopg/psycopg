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
from collections import OrderedDict, deque
from select import select
from typing import Any, Deque, Iterable

from psycopg import AsyncConnection, Connection
from psycopg import pq, waiting
from psycopg import errors as e

# Import from 'connection' to get selected implementation.
from psycopg.connection import pipeline_communicate


def pipeline_demo(rows_to_send: int) -> None:
    """Pipeline demo using sync API."""
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
    """Pipeline demo using async API."""
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


class DemoPipeline:
    """Handle for pipeline demo using 'pq' API."""

    _queries = OrderedDict(
        {
            "begin": "BEGIN TRANSACTION",
            "drop table": "DROP TABLE IF EXISTS pq_pipeline_demo",
            "create table": (
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            ),
            "prepare": (
                "INSERT INTO pq_pipeline_demo(itemno, int8filler)"
                " VALUES ($1, $2)"
            ),
        }
    )

    def __init__(
        self, pgconn: pq.abc.PGconn, logger: logging.Logger, rows_to_send: int
    ) -> None:
        self.pgconn = pgconn
        self.logger = logger
        self.queue: Deque[str] = deque()
        self.rows_to_send = rows_to_send
        self.committed = False
        self.synced = False

    def __enter__(self) -> "DemoPipeline":
        logger = self.logger
        pgconn = self.pgconn

        logger.debug("enter pipeline")
        pgconn.enter_pipeline_mode()

        for qname, query in self._queries.items():
            self.queue.append(qname)
            if qname == "prepare":
                insert_name = qname.encode()
                pgconn.send_prepare(insert_name, query.encode())
            else:
                pgconn.send_query(query.encode())
            logger.info(f"sent {qname}: {query}")

        pgconn.nonblocking = 1
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self.logger.debug("exit pipeline")
        self.pgconn.exit_pipeline_mode()

    def __iter__(self) -> "DemoPipeline":
        return self

    def __next__(self) -> None:
        pgconn = self.pgconn
        logger = self.logger
        queue = self.queue
        if self.rows_to_send:
            params = [f"{self.rows_to_send}".encode(), f"{1 << 62}".encode()]
            pgconn.send_query_prepared(b"prepare", params)
            self.rows_to_send -= 1
            queue.append(f"insert {self.rows_to_send}")
            logger.info(f"sent row {self.rows_to_send}")
        elif not self.committed:
            pgconn.send_query(b"COMMIT")
            self.committed = True
            queue.append("commit")
            logger.info("sent COMMIT")
        elif not self.synced:
            pgconn.pipeline_sync()
            queue.append("pipeline sync")
            logger.info("pipeline sync sent")
            self.synced = True
        else:
            raise StopIteration()

    def process_results(
        self, fetched: Iterable[Iterable[pq.abc.PGresult]]
    ) -> None:
        for results in fetched:
            queued = self.queue.popleft()
            statuses = [pq.ExecStatus(r.status).name for r in results]
            self.logger.info(
                f"got {', '.join(statuses)} results for '{queued}' command"
            )
            for r in results:
                if r.status in (
                    pq.ExecStatus.FATAL_ERROR,
                    pq.ExecStatus.PIPELINE_ABORTED,
                ):
                    raise e.error_from_result(r)


def pipeline_demo_pq(rows_to_send: int, logger: logging.Logger) -> None:
    pgconn = Connection.connect().pgconn
    handler = DemoPipeline(pgconn, logger, rows_to_send)

    socket = pgconn.socket
    wait = waiting.wait

    with handler:
        while handler.queue:
            gen = pipeline_communicate(pgconn)
            fetched = wait(gen, socket)
            handler.process_results(fetched)
            rl, wl, xl = select([], [socket], [], 0.1)
            if wl:
                next(handler, None)


async def pipeline_demo_pq_async(
    rows_to_send: int, logger: logging.Logger
) -> None:
    pgconn = (await AsyncConnection.connect()).pgconn
    handler = DemoPipeline(pgconn, logger, rows_to_send)

    loop = asyncio.get_event_loop()
    socket = pgconn.socket
    wait = waiting.wait_async

    with handler:
        while handler.queue:
            gen = pipeline_communicate(pgconn)
            fetched = await wait(gen, socket)
            handler.process_results(fetched)
            fut = loop.create_future()
            loop.add_writer(socket, fut.set_result, True)
            fut.add_done_callback(lambda f: loop.remove_writer(socket))
            try:
                await asyncio.wait_for(fut, timeout=0.1)
            except asyncio.TimeoutError:
                continue
            else:
                next(handler, None)


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
        "--pq", action="store_true", help="use low-level psycopg.pq API"
    )
    parser.add_argument(
        "--async", dest="async_", action="store_true", help="use async API"
    )
    parser.add_argument("-l", "--log", help="log file (stderr by default)")
    args = parser.parse_args()
    logger = logging.getLogger("psycopg")
    logger.setLevel(logging.DEBUG)
    pipeline_logger = logging.getLogger("pipeline")
    pipeline_logger.setLevel(logging.DEBUG)
    if args.log:
        logger.addHandler(logging.FileHandler(args.log))
        pipeline_logger.addHandler(logging.FileHandler(args.log))
    else:
        logger.addHandler(logging.StreamHandler())
        pipeline_logger.addHandler(logging.StreamHandler())
    if args.async_:
        if args.pq:
            asyncio.run(pipeline_demo_pq_async(args.nrows, pipeline_logger))
        else:
            asyncio.run(pipeline_demo_async(args.nrows))
    else:
        if args.pq:
            pipeline_demo_pq(args.nrows, pipeline_logger)
        else:
            pipeline_demo(args.nrows)


if __name__ == "__main__":
    main()
