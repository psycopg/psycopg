import os
import pytest
import logging
from contextlib import contextmanager
from typing import List

import psycopg
from psycopg import pq
from .utils import check_server_version


def pytest_addoption(parser):
    parser.addoption(
        "--test-dsn",
        metavar="DSN",
        default=os.environ.get("PSYCOPG_TEST_DSN"),
        help=(
            "Connection string to run database tests requiring a connection"
            " [you can also use the PSYCOPG_TEST_DSN env var]."
        ),
    )
    parser.addoption(
        "--pq-tracefile",
        metavar="TRACEFILE",
        default=None,
        help="Generate a libpq trace to TRACEFILE.",
    )


def pytest_report_header(config):
    dsn = config.getoption("--test-dsn")
    if dsn is None:
        return []

    try:
        with psycopg.connect(dsn, connect_timeout=10) as conn:
            server_version = conn.execute("select version()").fetchall()[0][0]
    except Exception as ex:
        server_version = f"unknown ({ex})"

    return [
        f"Server version: {server_version}",
    ]


def pytest_configure(config):
    # register pg marker
    config.addinivalue_line(
        "markers",
        "pg(version_expr): run the test only with matching server version"
        " (e.g. '>= 10', '< 9.6')",
    )


def pytest_runtest_setup(item):
    # Copy the want marker on the function so we can check the version
    # after the connection has been created.
    want_ver = [m.args[0] for m in item.iter_markers() if m.name == "pg"]
    if want_ver:
        item.function.want_pg_version = want_ver[0]


@pytest.fixture(scope="session")
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if dsn is None:
        pytest.skip("skipping test as no --test-dsn")

    try:
        warm_up_database(dsn)
    except Exception:
        # This is a session fixture, so, in case of error, the exception would
        # be cached and nothing would run.
        # Let the caller fail instead.
        logging.exception("error warming up database")

    return dsn


@pytest.fixture(scope="session")
def tracefile(request):
    """Open and yield a file for libpq client/server communication traces if
    --pq-tracefile option is set.
    """
    tracefile = request.config.getoption("--pq-tracefile")
    if not tracefile:
        yield None
        return

    with open(tracefile, "w") as f:
        yield f


@contextmanager
def maybe_trace(pgconn, tracefile, function):
    """Handle libpq client/server communication traces for a single test
    function.
    """
    if tracefile is None:
        yield None
        return

    title = f" {function.__module__}::{function.__qualname__} ".center(80, "=")
    tracefile.write(title + "\n")
    tracefile.flush()

    pgconn.trace(tracefile.fileno())
    try:
        pgconn.set_trace_flags(pq.Trace.SUPPRESS_TIMESTAMPS | pq.Trace.REGRESS_MODE)
    except psycopg.NotSupportedError:
        pass
    try:
        yield None
    finally:
        pgconn.untrace()


@pytest.fixture
def pgconn(dsn, request, tracefile):
    """Return a PGconn connection open to `--test-dsn`."""
    from psycopg import pq

    conn = pq.PGconn.connect(dsn.encode())
    if conn.status != pq.ConnStatus.OK:
        pytest.fail(f"bad connection: {conn.error_message.decode('utf8', 'replace')}")
    msg = check_connection_version(conn.server_version, request.function)
    if msg:
        conn.finish()
        pytest.skip(msg)

    with maybe_trace(conn, tracefile, request.function):
        yield conn

    conn.finish()


@pytest.fixture
def conn(dsn, request, tracefile):
    """Return a `Connection` connected to the ``--test-dsn`` database."""
    from psycopg import Connection

    conn = Connection.connect(dsn)
    msg = check_connection_version(conn.info.server_version, request.function)
    if msg:
        conn.close()
        pytest.skip(msg)
    with maybe_trace(conn.pgconn, tracefile, request.function):
        yield conn
    conn.close()


@pytest.fixture
async def aconn(dsn, request, tracefile):
    """Return an `AsyncConnection` connected to the ``--test-dsn`` database."""
    from psycopg import AsyncConnection

    conn = await AsyncConnection.connect(dsn)
    msg = check_connection_version(conn.info.server_version, request.function)
    if msg:
        await conn.close()
        pytest.skip(msg)
    with maybe_trace(conn.pgconn, tracefile, request.function):
        yield conn
    await conn.close()


@pytest.fixture(scope="session")
def svcconn(dsn):
    """
    Return a session `Connection` connected to the ``--test-dsn`` database.
    """
    from psycopg import Connection

    conn = Connection.connect(dsn, autocommit=True)
    yield conn
    conn.close()


@pytest.fixture
def commands(conn, monkeypatch):
    """The list of commands issued internally by the test connection."""
    yield patch_exec(conn, monkeypatch)


@pytest.fixture
def acommands(aconn, monkeypatch):
    """The list of commands issued internally by the test async connection."""
    yield patch_exec(aconn, monkeypatch)


def patch_exec(conn, monkeypatch):
    """Helper to implement the commands fixture both sync and async."""
    from psycopg import sql

    _orig_exec_command = conn._exec_command
    L = ListPopAll()

    def _exec_command(command, *args, **kwargs):
        cmdcopy = command
        if isinstance(cmdcopy, bytes):
            cmdcopy = cmdcopy.decode(conn.info.encoding)
        elif isinstance(cmdcopy, sql.Composable):
            cmdcopy = cmdcopy.as_string(conn)

        L.append(cmdcopy)
        return _orig_exec_command(command, *args, **kwargs)

    monkeypatch.setattr(conn, "_exec_command", _exec_command)
    return L


class ListPopAll(list):  # type: ignore[type-arg]
    """A list, with a popall() method."""

    def popall(self):
        out = self[:]
        del self[:]
        return out


def check_connection_version(got, function):
    if not hasattr(function, "want_pg_version"):
        return
    return check_server_version(got, function.want_pg_version)


@pytest.fixture
def hstore(svcconn):
    from psycopg import Error

    try:
        with svcconn.transaction():
            svcconn.execute("create extension if not exists hstore")
    except Error as e:
        pytest.skip(str(e))


def warm_up_database(dsn: str, __first_connection: List[bool] = [True]) -> None:
    """Connect to the database before returning a connection.

    In the CI sometimes, the first test fails with a timeout, probably because
    the server hasn't started yet. Absorb the delay before the test.
    """
    # Do it only once, even in case of failure, otherwise, in case of bad
    # configuration, with every test timing out, the entire test run would take
    # forever.
    if not __first_connection:
        return
    del __first_connection[:]

    import psycopg

    with psycopg.connect(dsn, connect_timeout=10) as conn:
        conn.execute("select 1")
