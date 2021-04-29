import re
import sys
import subprocess as sp

import pytest


@pytest.mark.slow
@pytest.mark.skipif(sys.version_info < (3, 7), reason="no future annotations")
def test_typing_example(mypy):
    cp = mypy.run("tests/typing_example.py")
    errors = cp.stdout.decode("utf8", "replace").splitlines()
    assert not errors
    assert cp.returncode == 0


@pytest.mark.slow
@pytest.mark.parametrize(
    "conn, type",
    [
        (
            "psycopg3.connect()",
            "psycopg3.Connection[Tuple[Any, ...]]",
        ),
        (
            "psycopg3.connect(row_factory=rows.tuple_row)",
            "psycopg3.Connection[Tuple[Any, ...]]",
        ),
        (
            "psycopg3.connect(row_factory=rows.dict_row)",
            "psycopg3.Connection[Dict[str, Any]]",
        ),
        (
            "psycopg3.connect(row_factory=rows.namedtuple_row)",
            "psycopg3.Connection[NamedTuple]",
        ),
        (
            "psycopg3.connect(row_factory=thing_row)",
            "psycopg3.Connection[Thing]",
        ),
        (
            "psycopg3.Connection.connect()",
            "psycopg3.Connection[Tuple[Any, ...]]",
        ),
        (
            "psycopg3.Connection.connect(row_factory=rows.dict_row)",
            "psycopg3.Connection[Dict[str, Any]]",
        ),
        (
            "await psycopg3.AsyncConnection.connect()",
            "psycopg3.AsyncConnection[Tuple[Any, ...]]",
        ),
        (
            "await psycopg3.AsyncConnection.connect(row_factory=rows.dict_row)",
            "psycopg3.AsyncConnection[Dict[str, Any]]",
        ),
    ],
)
def test_connection_type(conn, type, mypy, tmpdir):
    stmts = f"obj = {conn}"
    _test_reveal(stmts, type, mypy, tmpdir)


@pytest.mark.slow
@pytest.mark.parametrize(
    "conn, curs, type",
    [
        (
            "psycopg3.connect()",
            "conn.cursor()",
            "psycopg3.Cursor[Tuple[Any, ...]]",
        ),
        (
            "psycopg3.connect(row_factory=rows.dict_row)",
            "conn.cursor()",
            "psycopg3.Cursor[Dict[str, Any]]",
        ),
        (
            "psycopg3.connect(row_factory=rows.dict_row)",
            "conn.cursor(row_factory=rows.namedtuple_row)",
            "psycopg3.Cursor[NamedTuple]",
        ),
        (
            "psycopg3.connect(row_factory=thing_row)",
            "conn.cursor()",
            "psycopg3.Cursor[Thing]",
        ),
        (
            "psycopg3.connect()",
            "conn.cursor(row_factory=thing_row)",
            "psycopg3.Cursor[Thing]",
        ),
        # Async cursors
        (
            "await psycopg3.AsyncConnection.connect()",
            "conn.cursor()",
            "psycopg3.AsyncCursor[Tuple[Any, ...]]",
        ),
        (
            "await psycopg3.AsyncConnection.connect()",
            "conn.cursor(row_factory=thing_row)",
            "psycopg3.AsyncCursor[Thing]",
        ),
        # Server-side cursors
        (
            "psycopg3.connect()",
            "conn.cursor(name='foo')",
            "psycopg3.ServerCursor[Tuple[Any, ...]]",
        ),
        (
            "psycopg3.connect(row_factory=rows.dict_row)",
            "conn.cursor(name='foo')",
            "psycopg3.ServerCursor[Dict[str, Any]]",
        ),
        (
            "psycopg3.connect()",
            "conn.cursor(name='foo', row_factory=rows.dict_row)",
            "psycopg3.ServerCursor[Dict[str, Any]]",
        ),
        # Async server-side cursors
        (
            "await psycopg3.AsyncConnection.connect()",
            "conn.cursor(name='foo')",
            "psycopg3.AsyncServerCursor[Tuple[Any, ...]]",
        ),
        (
            "await psycopg3.AsyncConnection.connect(row_factory=rows.dict_row)",
            "conn.cursor(name='foo')",
            "psycopg3.AsyncServerCursor[Dict[str, Any]]",
        ),
        (
            "psycopg3.connect()",
            "conn.cursor(name='foo', row_factory=rows.dict_row)",
            "psycopg3.ServerCursor[Dict[str, Any]]",
        ),
    ],
)
def test_cursor_type(conn, curs, type, mypy, tmpdir):
    stmts = f"""\
conn = {conn}
obj = {curs}
"""
    _test_reveal(stmts, type, mypy, tmpdir)


@pytest.mark.slow
@pytest.mark.parametrize(
    "curs, type",
    [
        (
            "conn.cursor()",
            "Optional[Tuple[Any, ...]]",
        ),
        (
            "conn.cursor(row_factory=rows.dict_row)",
            "Optional[Dict[str, Any]]",
        ),
        (
            "conn.cursor(row_factory=thing_row)",
            "Optional[Thing]",
        ),
    ],
)
@pytest.mark.parametrize("server_side", [False, True])
@pytest.mark.parametrize("conn_class", ["Connection", "AsyncConnection"])
def test_fetchone_type(conn_class, server_side, curs, type, mypy, tmpdir):
    await_ = "await" if "Async" in conn_class else ""
    if server_side:
        curs = curs.replace("(", "(name='foo',", 1)
    stmts = f"""\
conn = {await_} psycopg3.{conn_class}.connect()
curs = {curs}
obj = {await_} curs.fetchone()
"""
    _test_reveal(stmts, type, mypy, tmpdir)


@pytest.fixture(scope="session")
def mypy(tmp_path_factory):
    cache_dir = tmp_path_factory.mktemp(basename="mypy_cache")

    class MypyRunner:
        def run(self, filename):
            cmdline = f"""
                mypy
                --strict
                --show-error-codes --no-color-output --no-error-summary
                --config-file= --cache-dir={cache_dir}
                """.split()
            cmdline.append(filename)
            return sp.run(cmdline, stdout=sp.PIPE, stderr=sp.STDOUT)

    return MypyRunner()


def _test_reveal(stmts, type, mypy, tmpdir):
    ignore = (
        "" if type.startswith("Optional") else "# type: ignore[assignment]"
    )
    stmts = "\n".join(f"    {line}" for line in stmts.splitlines())

    src = f"""\
from typing import Any, Callable, Dict, NamedTuple, Optional, Sequence, Tuple
import psycopg3
from psycopg3 import rows

class Thing:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

def thing_row(
    cur: psycopg3.BaseCursor[Any, Thing],
) -> Callable[[Sequence[Any]], Thing]:
    assert cur.description
    names = [d.name for d in cur.description]

    def make_row(t: Sequence[Any]) -> Thing:
        return Thing(**dict(zip(names, t)))

    return make_row

async def tmp() -> None:
{stmts}
    reveal_type(obj)

ref: {type} = None  {ignore}
reveal_type(ref)
"""
    fn = tmpdir / "tmp.py"
    with fn.open("w") as f:
        f.write(src)

    cp = mypy.run(str(fn))
    out = cp.stdout.decode("utf8", "replace").splitlines()
    assert len(out) == 2, "\n".join(out)
    got, want = [
        re.sub(r".*Revealed type is '([^']+)'.*", r"\1", line).replace("*", "")
        for line in out
    ]
    assert got == want
