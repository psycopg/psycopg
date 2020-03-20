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


@pytest.fixture
def pq():
    """The libpq module wrapper to test."""
    from psycopg3 import pq

    return pq


@pytest.fixture
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if not dsn:
        pytest.skip("skipping test as no --test-dsn")
    return dsn


@pytest.fixture
def pgconn(pq, dsn):
    """Return a PGconn connection open to `--test-dsn`."""
    return pq.PGconn.connect(dsn.encode("utf8"))
