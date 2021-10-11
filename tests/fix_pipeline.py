import logging
from collections import deque, OrderedDict
from typing import Deque

import pytest

from psycopg import pq
from psycopg import errors as e


class DemoPipeline:
    """Handler for pipeline demo as found in PostgreSQL test
    libpq_pipeline::pipelined_insert at
    src/test/modules/libpq_pipeline/libpq_pipeline.c::test_pipelined_insert()
    """

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

    def __init__(self, pgconn, logger, rows_to_send=10_000):
        self.pgconn = pgconn
        self.logger = logger
        self.queue: Deque[str] = deque()
        self.rows_to_send = rows_to_send
        self.committed = False
        self.synced = False

    def __enter__(self):
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

    def __exit__(self, *args, **kwargs):
        self.logger.debug("exit pipeline")
        self.pgconn.exit_pipeline_mode()

    def __iter__(self):
        return self

    def __next__(self):
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

    def process_results(self, fetched):
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


@pytest.fixture
def pipeline_logger(caplog):
    """Configure a logger and log capture for pipeline tests.

    Log messages will be displayed as part of "Captured log call" block in
    pytest output in case of failure.

    Use --log-file=<name> to get log messages in a file, even in case of
    success.
    """
    logger = logging.getLogger("pipeline")
    caplog.set_level(logging.INFO, logger=logger.name)
    caplog.set_level(logging.DEBUG, logger="psycopg")
    return logger


@pytest.fixture
def demo_pipeline(pgconn, pipeline_logger):
    return DemoPipeline(pgconn, pipeline_logger)
