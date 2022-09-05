import os
import sys
import ctypes
from typing import Iterator, List, NamedTuple
from tempfile import TemporaryFile

import pytest

from .utils import check_libpq_version

try:
    from psycopg import pq
except ImportError:
    pq = None  # type: ignore


def pytest_report_header(config):
    try:
        from psycopg import pq
    except ImportError:
        return []

    return [
        f"libpq wrapper implementation: {pq.__impl__}",
        f"libpq used: {pq.version()}",
        f"libpq compiled: {pq.__build_version__}",
    ]


def pytest_configure(config):
    # register libpq marker
    config.addinivalue_line(
        "markers",
        "libpq(version_expr): run the test only with matching libpq"
        " (e.g. '>= 10', '< 9.6')",
    )


def pytest_runtest_setup(item):
    for m in item.iter_markers(name="libpq"):
        assert len(m.args) == 1
        msg = check_libpq_version(pq.version(), m.args[0])
        if msg:
            pytest.skip(msg)


@pytest.fixture
def libpq():
    """Return a ctypes wrapper to access the libpq."""
    try:
        from psycopg.pq.misc import find_libpq_full_path

        # Not available when testing the binary package
        libname = find_libpq_full_path()
        assert libname, "libpq libname not found"
        return ctypes.pydll.LoadLibrary(libname)
    except Exception as e:
        if pq.__impl__ == "binary":
            pytest.skip(f"can't load libpq for testing: {e}")
        else:
            raise


@pytest.fixture
def setpgenv(monkeypatch):
    """Replace the PG* env vars with the vars provided."""

    def setpgenv_(env):
        ks = [k for k in os.environ if k.startswith("PG")]
        for k in ks:
            monkeypatch.delenv(k)

        if env:
            for k, v in env.items():
                monkeypatch.setenv(k, v)

    return setpgenv_


@pytest.fixture
def trace(libpq):
    pqver = pq.__build_version__
    if pqver < 140000:
        pytest.skip(f"trace not available on libpq {pqver}")
    if sys.platform != "linux":
        pytest.skip(f"trace not available on {sys.platform}")

    yield Tracer()


class Tracer:
    def trace(self, conn):
        pgconn: "pq.abc.PGconn"

        if hasattr(conn, "exec_"):
            pgconn = conn
        elif hasattr(conn, "cursor"):
            pgconn = conn.pgconn
        else:
            raise Exception()

        return TraceLog(pgconn)


class TraceLog:
    def __init__(self, pgconn: "pq.abc.PGconn"):
        self.pgconn = pgconn
        self.tempfile = TemporaryFile(buffering=0)
        pgconn.trace(self.tempfile.fileno())
        pgconn.set_trace_flags(pq.Trace.SUPPRESS_TIMESTAMPS)

    def __del__(self):
        if self.pgconn.status == pq.ConnStatus.OK:
            self.pgconn.untrace()
        self.tempfile.close()

    def __iter__(self) -> "Iterator[TraceEntry]":
        self.tempfile.seek(0)
        data = self.tempfile.read()
        for entry in self._parse_entries(data):
            yield entry

    def _parse_entries(self, data: bytes) -> "Iterator[TraceEntry]":
        for line in data.splitlines():
            direction, length, type, *content = line.split(b"\t")
            yield TraceEntry(
                direction=direction.decode(),
                length=int(length.decode()),
                type=type.decode(),
                # Note: the items encoding is not very solid: no escaped
                # backslash, no escaped quotes.
                # At the moment we don't need a proper parser.
                content=[content[0]] if content else [],
            )


class TraceEntry(NamedTuple):
    direction: str
    length: int
    type: str
    content: List[bytes]
