import pytest

pytestmark = pytest.mark.crdb


def test_vendor(conn):
    assert conn.info.vendor == "CockroachDB"


def test_crdb_version(conn):
    assert conn.info.crdb_version > 200000


def test_backend_pid(conn):
    assert conn.info.backend_pid == 0
