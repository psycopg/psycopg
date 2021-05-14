import os
import pytest

from .utils import check_server_version


def pytest_addoption(parser):
    parser.addoption(
        "--test-dsn",
        metavar="DSN",
        default=os.environ.get("PSYCOPG_TEST_DSN") or None,
        help="Connection string to run database tests requiring a connection"
        " [you can also use the PSYCOPG_TEST_DSN env var].",
    )


def pytest_configure(config):
    # register pg marker
    config.addinivalue_line(
        "markers",
        "pg(version_expr): run the test only with matching server version"
        " (e.g. '>= 10', '< 9.6')",
    )


@pytest.fixture(scope="session")
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if not dsn:
        pytest.skip("skipping test as no --test-dsn")
    return dsn


@pytest.fixture
def pgconn(dsn, request):
    """Return a PGconn connection open to `--test-dsn`."""
    from psycopg import pq

    conn = pq.PGconn.connect(dsn.encode())
    if conn.status != pq.ConnStatus.OK:
        pytest.fail(
            f"bad connection: {conn.error_message.decode('utf8', 'replace')}"
        )
    msg = check_connection_version(conn.server_version, request.function)
    if msg:
        conn.finish()
        pytest.skip(msg)
    yield conn
    conn.finish()


@pytest.fixture
def conn(dsn, request):
    """Return a `Connection` connected to the ``--test-dsn`` database."""
    from psycopg import Connection

    conn = Connection.connect(dsn)
    msg = check_connection_version(conn.info.server_version, request.function)
    if msg:
        conn.close()
        pytest.skip(msg)
    yield conn
    conn.close()


@pytest.fixture
async def aconn(dsn, request):
    """Return an `AsyncConnection` connected to the ``--test-dsn`` database."""
    from psycopg import AsyncConnection

    conn = await AsyncConnection.connect(dsn)
    msg = check_connection_version(conn.info.server_version, request.function)
    if msg:
        await conn.close()
        pytest.skip(msg)
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

        L.insert(0, cmdcopy)
        return _orig_exec_command(command, *args, **kwargs)

    monkeypatch.setattr(conn, "_exec_command", _exec_command)
    return L


class ListPopAll(list):
    """A list, with a popall() method."""

    def popall(self):
        out = self[:]
        del self[:]
        return out


def check_connection_version(got, function):
    if not hasattr(function, "pytestmark"):
        return
    want = [m.args[0] for m in function.pytestmark if m.name == "pg"]
    if want:
        return check_server_version(got, want[0])


@pytest.fixture
def hstore(svcconn):
    from psycopg import Error

    try:
        with svcconn.transaction():
            svcconn.execute("create extension if not exists hstore")
    except Error as e:
        pytest.skip(str(e))
