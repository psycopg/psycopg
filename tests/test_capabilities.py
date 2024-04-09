import re

import pytest

from psycopg import pq, _cmodule
from psycopg import capabilities, NotSupportedError

caps = [
    ("has_encrypt_password", "encrypt_password", 10),
    ("has_hostaddr", "PGconn.hostaddr", 12),
    ("has_pipeline", "Connection.pipeline()", 14),
    ("has_set_trace_flag", "PGconn.set_trace_flag()", 14),
    ("has_cancel_safe", "Connection.cancel_safe()", 17),
    ("has_pgbouncer_prepared", "PgBouncer prepared statements compatibility", 17),
]


@pytest.mark.parametrize(
    "method_name",
    [
        pytest.param(method_name, marks=pytest.mark.libpq(f">= {min_ver}"))
        for method_name, _, min_ver in caps
    ],
)
def test_has_capability(method_name):
    method = getattr(capabilities, method_name)
    assert method()
    assert method(check=True)


@pytest.mark.parametrize(
    "method_name, label",
    [
        pytest.param(method_name, label, marks=pytest.mark.libpq(f"< {min_ver}"))
        for method_name, label, min_ver in caps
    ],
)
def test_no_capability(method_name, label):
    method = getattr(capabilities, method_name)
    assert not method()
    with pytest.raises(NotSupportedError, match=f"'{re.escape(label)}'"):
        method(check=True)


def test_build_or_import_msg(monkeypatch):
    monkeypatch.setattr(pq, "version", lambda: 140000)
    monkeypatch.setattr(pq, "__build_version__", 139999)
    with pytest.raises(NotSupportedError, match=r"built with libpq version 13\.99\.99"):
        capabilities.has_pipeline(check=True)

    monkeypatch.setattr(pq, "version", lambda: 139999)
    with pytest.raises(
        NotSupportedError, match=r"client libpq version \(.*\) is 13\.99\.99"
    ):
        capabilities.has_pipeline(check=True)


def test_impl_build_error(monkeypatch):
    monkeypatch.setattr(pq, "__build_version__", 139999)
    monkeypatch.setattr(pq, "version", lambda: 139999)
    if pq.__impl__ == "binary":
        ver = _cmodule.__version__
        assert ver
        msg = "(imported from the psycopg[binary] package version {ver})"
    else:
        msg = "(imported from system libraries)"
        with pytest.raises(NotSupportedError, match=re.escape(msg)):
            capabilities.has_pipeline(check=True)
