import re

import pytest

from psycopg import pq, _cmodule

try:
    from psycopg import Capabilities, capabilities, NotSupportedError
except ImportError:
    # Allow to import the module with Psycopg 3.1
    pass

caps = [
    ("has_encrypt_password", "pq.PGconn.encrypt_password()", 10),
    ("has_hostaddr", "Connection.info.hostaddr", 12),
    ("has_pipeline", "Connection.pipeline()", 14),
    ("has_set_trace_flags", "PGconn.set_trace_flags()", 14),
    ("has_cancel_safe", "Connection.cancel_safe()", 17),
    ("has_stream_chunked", "Cursor.stream() with 'size' parameter greater than 1", 17),
    ("has_send_close_prepared", "PGconn.send_close_prepared()", 17),
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
        Capabilities().has_pipeline(check=True)

    monkeypatch.setattr(pq, "version", lambda: 139999)
    with pytest.raises(
        NotSupportedError, match=r"client libpq version \(.*\) is 13\.99\.99"
    ):
        Capabilities().has_pipeline(check=True)


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
            Capabilities().has_pipeline(check=True)


def test_caching(monkeypatch):

    version = 150000

    caps = Capabilities()
    called = 0

    def ver():
        nonlocal called
        called += 1
        return version

    monkeypatch.setattr(pq, "version", ver)
    monkeypatch.setattr(pq, "__build_version__", version)

    caps.has_pipeline()
    assert called == 1
    caps.has_pipeline()
    assert called == 1
    caps.has_hostaddr()
    assert called == 2
