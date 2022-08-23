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
from contextlib import contextmanager
from functools import partial
from typing import Any, Iterator, Optional, Sequence, Tuple

from psycopg import AsyncConnection, Connection
from psycopg import pq, waiting
from psycopg import errors as e
from psycopg.abc import PipelineCommand
from psycopg.generators import pipeline_communicate
from psycopg.pq import Format, DiagnosticField
from psycopg._compat import Deque

psycopg_logger = logging.getLogger("psycopg")
pipeline_logger = logging.getLogger("pipeline")
args: argparse.Namespace


class LoggingPGconn:
    """Wrapper for PGconn that logs fetched results."""

    def __init__(self, pgconn: pq.abc.PGconn, logger: logging.Logger):
        self._pgconn = pgconn
        self._logger = logger

        def log_notice(result: pq.abc.PGresult) -> None:
            def get_field(field: DiagnosticField) -> Optional[str]:
                value = result.error_field(field)
                return value.decode("utf-8", "replace") if value else None

            logger.info(
                "notice %s %s",
                get_field(DiagnosticField.SEVERITY),
                get_field(DiagnosticField.MESSAGE_PRIMARY),
            )

        pgconn.notice_handler = log_notice

        if args.trace:
            self._trace_file = open(args.trace, "w")
            pgconn.trace(self._trace_file.fileno())

    def __del__(self) -> None:
        if hasattr(self, "_trace_file"):
            self._pgconn.untrace()
            self._trace_file.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pgconn, name)

    def send_query(self, command: bytes) -> None:
        self._logger.warning("PQsendQuery broken in libpq 14.5")
        self._pgconn.send_query(command)
        self._logger.info("sent %s", command.decode())

    def send_query_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> None:
        self._pgconn.send_query_params(
            command, param_values, param_types, param_formats, result_format
        )
        self._logger.info("sent %s", command.decode())

    def send_query_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> None:
        self._pgconn.send_query_prepared(
            name, param_values, param_formats, result_format
        )
        self._logger.info("sent prepared '%s' with %s", name.decode(), param_values)

    def send_prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> None:
        self._pgconn.send_prepare(name, command, param_types)
        self._logger.info("prepare %s as '%s'", command.decode(), name.decode())

    def get_result(self) -> Optional[pq.abc.PGresult]:
        r = self._pgconn.get_result()
        if r is not None:
            self._logger.info("got %s result", pq.ExecStatus(r.status).name)
        return r


@contextmanager
def prepare_pipeline_demo_pq(
    pgconn: LoggingPGconn, rows_to_send: int, logger: logging.Logger
) -> Iterator[Tuple[Deque[PipelineCommand], Deque[str]]]:
    """Set up pipeline demo with initial queries and yield commands and
    results queue for pipeline_communicate().
    """
    logger.debug("enter pipeline")
    pgconn.enter_pipeline_mode()

    setup_queries = [
        ("begin", "BEGIN TRANSACTION"),
        ("drop table", "DROP TABLE IF EXISTS pq_pipeline_demo"),
        (
            "create table",
            (
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            ),
        ),
        (
            "prepare",
            ("INSERT INTO pq_pipeline_demo(itemno, int8filler)" " VALUES ($1, $2)"),
        ),
    ]

    commands = Deque[PipelineCommand]()
    results_queue = Deque[str]()

    for qname, query in setup_queries:
        if qname == "prepare":
            pgconn.send_prepare(qname.encode(), query.encode())
        else:
            pgconn.send_query_params(query.encode(), None)
        results_queue.append(qname)

    committed = False
    synced = False

    while True:
        if rows_to_send:
            params = [f"{rows_to_send}".encode(), f"{1 << 62}".encode()]
            commands.append(partial(pgconn.send_query_prepared, b"prepare", params))
            results_queue.append(f"row {rows_to_send}")
            rows_to_send -= 1

        elif not committed:
            committed = True
            commands.append(partial(pgconn.send_query_params, b"COMMIT", None))
            results_queue.append("commit")

        elif not synced:

            def sync() -> None:
                pgconn.pipeline_sync()
                logger.info("pipeline sync sent")

            synced = True
            commands.append(sync)
            results_queue.append("sync")

        else:
            break

    try:
        yield commands, results_queue
    finally:
        logger.debug("exit pipeline")
        pgconn.exit_pipeline_mode()


def pipeline_demo_pq(rows_to_send: int, logger: logging.Logger) -> None:
    pgconn = LoggingPGconn(Connection.connect().pgconn, logger)
    with prepare_pipeline_demo_pq(pgconn, rows_to_send, logger) as (
        commands,
        results_queue,
    ):
        while results_queue:
            fetched = waiting.wait(
                pipeline_communicate(
                    pgconn,  # type: ignore[arg-type]
                    commands,
                ),
                pgconn.socket,
            )
            assert not commands, commands
            for results in fetched:
                results_queue.popleft()
                for r in results:
                    if r.status in (
                        pq.ExecStatus.FATAL_ERROR,
                        pq.ExecStatus.PIPELINE_ABORTED,
                    ):
                        raise e.error_from_result(r)


async def pipeline_demo_pq_async(rows_to_send: int, logger: logging.Logger) -> None:
    pgconn = LoggingPGconn((await AsyncConnection.connect()).pgconn, logger)

    with prepare_pipeline_demo_pq(pgconn, rows_to_send, logger) as (
        commands,
        results_queue,
    ):
        while results_queue:
            fetched = await waiting.wait_async(
                pipeline_communicate(
                    pgconn,  # type: ignore[arg-type]
                    commands,
                ),
                pgconn.socket,
            )
            assert not commands, commands
            for results in fetched:
                results_queue.popleft()
                for r in results:
                    if r.status in (
                        pq.ExecStatus.FATAL_ERROR,
                        pq.ExecStatus.PIPELINE_ABORTED,
                    ):
                        raise e.error_from_result(r)


def pipeline_demo(rows_to_send: int, many: bool, logger: logging.Logger) -> None:
    """Pipeline demo using sync API."""
    conn = Connection.connect()
    conn.autocommit = True
    conn.pgconn = LoggingPGconn(conn.pgconn, logger)  # type: ignore[assignment]
    with conn.pipeline():
        with conn.transaction():
            conn.execute("DROP TABLE IF EXISTS pq_pipeline_demo")
            conn.execute(
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            )
            query = "INSERT INTO pq_pipeline_demo(itemno, int8filler) VALUES (%s, %s)"
            params = ((r, 1 << 62) for r in range(rows_to_send, 0, -1))
            if many:
                cur = conn.cursor()
                cur.executemany(query, list(params))
            else:
                for p in params:
                    conn.execute(query, p)


async def pipeline_demo_async(
    rows_to_send: int, many: bool, logger: logging.Logger
) -> None:
    """Pipeline demo using async API."""
    aconn = await AsyncConnection.connect()
    await aconn.set_autocommit(True)
    aconn.pgconn = LoggingPGconn(aconn.pgconn, logger)  # type: ignore[assignment]
    async with aconn.pipeline():
        async with aconn.transaction():
            await aconn.execute("DROP TABLE IF EXISTS pq_pipeline_demo")
            await aconn.execute(
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            )
            query = "INSERT INTO pq_pipeline_demo(itemno, int8filler) VALUES (%s, %s)"
            params = ((r, 1 << 62) for r in range(rows_to_send, 0, -1))
            if many:
                cur = aconn.cursor()
                await cur.executemany(query, list(params))
            else:
                for p in params:
                    await aconn.execute(query, p)


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
    parser.add_argument(
        "--many",
        action="store_true",
        help="use executemany() (not applicable for --pq)",
    )
    parser.add_argument("--trace", help="write trace info into TRACE file")
    parser.add_argument("-l", "--log", help="log file (stderr by default)")

    global args
    args = parser.parse_args()

    psycopg_logger.setLevel(logging.DEBUG)
    pipeline_logger.setLevel(logging.DEBUG)
    if args.log:
        psycopg_logger.addHandler(logging.FileHandler(args.log))
        pipeline_logger.addHandler(logging.FileHandler(args.log))
    else:
        psycopg_logger.addHandler(logging.StreamHandler())
        pipeline_logger.addHandler(logging.StreamHandler())

    if args.pq:
        if args.many:
            parser.error("--many cannot be used with --pq")
        if args.async_:
            asyncio.run(pipeline_demo_pq_async(args.nrows, pipeline_logger))
        else:
            pipeline_demo_pq(args.nrows, pipeline_logger)
    else:
        if pq.__impl__ != "python":
            parser.error(
                "only supported for Python implementation (set PSYCOPG_IMPL=python)"
            )
        if args.async_:
            asyncio.run(pipeline_demo_async(args.nrows, args.many, pipeline_logger))
        else:
            pipeline_demo(args.nrows, args.many, pipeline_logger)


if __name__ == "__main__":
    main()
