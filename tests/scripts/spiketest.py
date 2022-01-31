#!/usr/bin/env python
"""
Run a connection pool spike test.

The test is inspired to the `spike analysis`__ illustrated by HikariCP

.. __: https://github.com/brettwooldridge/HikariCP/blob/dev/documents/
       Welcome-To-The-Jungle.md

"""
# mypy: allow-untyped-defs
# mypy: allow-untyped-calls

import time
import threading

import psycopg
import psycopg_pool
from psycopg.rows import Row

import logging


def main() -> None:
    opt = parse_cmdline()
    if opt.loglevel:
        loglevel = getattr(logging, opt.loglevel.upper())
        logging.basicConfig(
            level=loglevel, format="%(asctime)s %(levelname)s %(message)s"
        )

        logging.getLogger("psycopg2.pool").setLevel(loglevel)

    with psycopg_pool.ConnectionPool(
        opt.dsn,
        min_size=opt.min_size,
        max_size=opt.max_size,
        connection_class=DelayedConnection,
        kwargs={"conn_delay": 0.150},
    ) as pool:
        pool.wait()
        measurer = Measurer(pool)

        # Create and start all the thread: they will get stuck on the event
        ev = threading.Event()
        threads = [
            threading.Thread(target=worker, args=(pool, 0.002, ev), daemon=True)
            for i in range(opt.num_clients)
        ]
        for t in threads:
            t.start()
        time.sleep(0.2)

        # Release the threads!
        measurer.start(0.00025)
        t0 = time.time()
        ev.set()

        # Wait for the threads to finish
        for t in threads:
            t.join()
        t1 = time.time()
        measurer.stop()

    print(f"time: {(t1 - t0) * 1000} msec")
    print("active,idle,total,waiting")
    recs = [
        f'{m["pool_size"] - m["pool_available"]}'
        f',{m["pool_available"]}'
        f',{m["pool_size"]}'
        f',{m["requests_waiting"]}'
        for m in measurer.measures
    ]
    print("\n".join(recs))


def worker(p, t, ev):
    ev.wait()
    with p.connection():
        time.sleep(t)


class Measurer:
    def __init__(self, pool):
        self.pool = pool
        self.worker = None
        self.stopped = False
        self.measures = []

    def start(self, interval):
        self.worker = threading.Thread(target=self._run, args=(interval,), daemon=True)
        self.worker.start()

    def stop(self):
        self.stopped = True
        if self.worker:
            self.worker.join()
            self.worker = None

    def _run(self, interval):
        while not self.stopped:
            self.measures.append(self.pool.get_stats())
            time.sleep(interval)


class DelayedConnection(psycopg.Connection[Row]):
    """A connection adding a delay to the connection time."""

    @classmethod
    def connect(cls, conninfo, conn_delay=0, **kwargs):
        t0 = time.time()
        conn = super().connect(conninfo, **kwargs)
        t1 = time.time()
        wait = max(0.0, conn_delay - (t1 - t0))
        if wait:
            time.sleep(wait)
        return conn


def parse_cmdline():
    from argparse import ArgumentParser

    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="", help="connection string to the database")
    parser.add_argument(
        "--min_size",
        default=5,
        type=int,
        help="minimum number of connections in the pool",
    )
    parser.add_argument(
        "--max_size",
        default=20,
        type=int,
        help="maximum number of connections in the pool",
    )
    parser.add_argument(
        "--num-clients",
        default=50,
        type=int,
        help="number of threads making a request",
    )
    parser.add_argument(
        "--loglevel",
        default=None,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="level to log at [default: no log]",
    )

    opt = parser.parse_args()

    return opt


if __name__ == "__main__":
    main()
