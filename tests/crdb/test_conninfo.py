import pytest

pytestmark = pytest.mark.crdb


def test_vendor(conn):
    assert conn.info.vendor == "CockroachDB"


def test_server_version(conn):
    assert conn.info.server_version > 200000


@pytest.mark.crdb("< 22")
def test_backend_pid_pre_22(conn):
    assert conn.info.backend_pid == 0


@pytest.mark.crdb(">= 22")
def test_backend_pid(conn):
    assert conn.info.backend_pid > 0
