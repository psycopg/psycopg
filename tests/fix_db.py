import os
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--test-dsn",
        metavar="DSN",
        default=os.environ.get("PSYCOPG3_TEST_DSN") or None,
        help="Connection string to run database tests requiring a connection"
        " [you can also use the PSYCOPG3_TEST_DSN env var].",
    )


@pytest.fixture(scope="session")
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if not dsn:
        pytest.skip("skipping test as no --test-dsn")
    return dsn


@pytest.fixture
def pgconn(dsn):
    """Return a PGconn connection open to `--test-dsn`."""
    from psycopg3 import pq

    conn = pq.PGconn.connect(dsn.encode("utf8"))
    if conn.status != pq.ConnStatus.OK:
        pytest.fail(
            f"bad connection: {conn.error_message.decode('utf8', 'replace')}"
        )
    yield conn
    conn.finish()


@pytest.fixture
def conn(dsn):
    """Return a `Connection` connected to the ``--test-dsn`` database."""
    from psycopg3 import Connection

    conn = Connection.connect(dsn)
    yield conn
    conn.close()


@pytest.fixture
async def aconn(dsn):
    """Return an `AsyncConnection` connected to the ``--test-dsn`` database."""
    from psycopg3 import AsyncConnection

    conn = await AsyncConnection.connect(dsn)
    yield conn
    await conn.close()


@pytest.fixture(scope="session")
def svcconn(dsn):
    """
    Return a session `Connection` connected to the ``--test-dsn`` database.
    """
    from psycopg3 import Connection

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
    from psycopg3 import sql

    _orig_exec_command = conn._exec_command
    L = ListPopAll()

    def _exec_command(command):
        cmdcopy = command
        if isinstance(cmdcopy, bytes):
            cmdcopy = cmdcopy.decode(conn.client_encoding)
        elif isinstance(cmdcopy, sql.Composable):
            cmdcopy = cmdcopy.as_string(conn)

        L.insert(0, cmdcopy)
        return _orig_exec_command(command)

    monkeypatch.setattr(conn, "_exec_command", _exec_command)
    return L


class ListPopAll(list):
    """A list, with a popall() method."""

    def popall(self):
        out = self[:]
        del self[:]
        return out
