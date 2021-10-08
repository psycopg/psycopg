import re
import sys
import subprocess as sp

import pytest


@pytest.mark.slow
@pytest.mark.parametrize(
    "filename",
    [
        "tests/adapters_example.py",
        pytest.param(
            "tests/typing_example.py",
            marks=pytest.mark.skipif(
                sys.version_info < (3, 7), reason="no future annotations"
            ),
        ),
    ],
)
def test_typing_example(mypy, filename):
    cp = mypy.run_on_file(filename)
    errors = cp.stdout.decode("utf8", "replace").splitlines()
    assert not errors
    assert cp.returncode == 0


@pytest.mark.slow
@pytest.mark.parametrize(
    "conn, type",
    [
        (
            "psycopg.connect()",
            "psycopg.Connection[Tuple[Any, ...]]",
        ),
        (
            "psycopg.connect(row_factory=rows.tuple_row)",
            "psycopg.Connection[Tuple[Any, ...]]",
        ),
        (
            "psycopg.connect(row_factory=rows.dict_row)",
            "psycopg.Connection[Dict[str, Any]]",
        ),
        (
            "psycopg.connect(row_factory=rows.namedtuple_row)",
            "psycopg.Connection[NamedTuple]",
        ),
        (
            "psycopg.connect(row_factory=rows.class_row(Thing))",
            "psycopg.Connection[Thing]",
        ),
        (
            "psycopg.connect(row_factory=thing_row)",
            "psycopg.Connection[Thing]",
        ),
        (
            "psycopg.Connection.connect()",
            "psycopg.Connection[Tuple[Any, ...]]",
        ),
        (
            "psycopg.Connection.connect(row_factory=rows.dict_row)",
            "psycopg.Connection[Dict[str, Any]]",
        ),
        (
            "await psycopg.AsyncConnection.connect()",
            "psycopg.AsyncConnection[Tuple[Any, ...]]",
        ),
        (
            "await psycopg.AsyncConnection.connect(row_factory=rows.dict_row)",
            "psycopg.AsyncConnection[Dict[str, Any]]",
        ),
    ],
)
def test_connection_type(conn, type, mypy):
    stmts = f"obj = {conn}"
    _test_reveal(stmts, type, mypy)


@pytest.mark.slow
@pytest.mark.parametrize(
    "conn, curs, type",
    [
        (
            "psycopg.connect()",
            "conn.cursor()",
            "psycopg.Cursor[Tuple[Any, ...]]",
        ),
        (
            "psycopg.connect(row_factory=rows.dict_row)",
            "conn.cursor()",
            "psycopg.Cursor[Dict[str, Any]]",
        ),
        (
            "psycopg.connect(row_factory=rows.dict_row)",
            "conn.cursor(row_factory=rows.namedtuple_row)",
            "psycopg.Cursor[NamedTuple]",
        ),
        (
            "psycopg.connect(row_factory=rows.class_row(Thing))",
            "conn.cursor()",
            "psycopg.Cursor[Thing]",
        ),
        (
            "psycopg.connect(row_factory=thing_row)",
            "conn.cursor()",
            "psycopg.Cursor[Thing]",
        ),
        (
            "psycopg.connect()",
            "conn.cursor(row_factory=thing_row)",
            "psycopg.Cursor[Thing]",
        ),
        # Async cursors
        (
            "await psycopg.AsyncConnection.connect()",
            "conn.cursor()",
            "psycopg.AsyncCursor[Tuple[Any, ...]]",
        ),
        (
            "await psycopg.AsyncConnection.connect()",
            "conn.cursor(row_factory=thing_row)",
            "psycopg.AsyncCursor[Thing]",
        ),
        # Server-side cursors
        (
            "psycopg.connect()",
            "conn.cursor(name='foo')",
            "psycopg.ServerCursor[Tuple[Any, ...]]",
        ),
        (
            "psycopg.connect(row_factory=rows.dict_row)",
            "conn.cursor(name='foo')",
            "psycopg.ServerCursor[Dict[str, Any]]",
        ),
        (
            "psycopg.connect()",
            "conn.cursor(name='foo', row_factory=rows.dict_row)",
            "psycopg.ServerCursor[Dict[str, Any]]",
        ),
        # Async server-side cursors
        (
            "await psycopg.AsyncConnection.connect()",
            "conn.cursor(name='foo')",
            "psycopg.AsyncServerCursor[Tuple[Any, ...]]",
        ),
        (
            "await psycopg.AsyncConnection.connect(row_factory=rows.dict_row)",
            "conn.cursor(name='foo')",
            "psycopg.AsyncServerCursor[Dict[str, Any]]",
        ),
        (
            "psycopg.connect()",
            "conn.cursor(name='foo', row_factory=rows.dict_row)",
            "psycopg.ServerCursor[Dict[str, Any]]",
        ),
    ],
)
def test_cursor_type(conn, curs, type, mypy):
    stmts = f"""\
conn = {conn}
obj = {curs}
"""
    _test_reveal(stmts, type, mypy)


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
def test_fetchone_type(conn_class, server_side, curs, type, mypy):
    await_ = "await" if "Async" in conn_class else ""
    if server_side:
        curs = curs.replace("(", "(name='foo',", 1)
    stmts = f"""\
conn = {await_} psycopg.{conn_class}.connect()
curs = {curs}
obj = {await_} curs.fetchone()
"""
    _test_reveal(stmts, type, mypy)


@pytest.mark.slow
@pytest.mark.parametrize(
    "curs, type",
    [
        (
            "conn.cursor()",
            "Tuple[Any, ...]",
        ),
        (
            "conn.cursor(row_factory=rows.dict_row)",
            "Dict[str, Any]",
        ),
        (
            "conn.cursor(row_factory=thing_row)",
            "Thing",
        ),
    ],
)
@pytest.mark.parametrize("server_side", [False, True])
@pytest.mark.parametrize("conn_class", ["Connection", "AsyncConnection"])
def test_iter_type(conn_class, server_side, curs, type, mypy):
    if "Async" in conn_class:
        async_ = "async "
        await_ = "await "
    else:
        async_ = await_ = ""

    if server_side:
        curs = curs.replace("(", "(name='foo',", 1)
    stmts = f"""\
conn = {await_}psycopg.{conn_class}.connect()
curs = {curs}
{async_}for obj in curs:
    pass
"""
    _test_reveal(stmts, type, mypy)


@pytest.mark.slow
@pytest.mark.parametrize("method", ["fetchmany", "fetchall"])
@pytest.mark.parametrize(
    "curs, type",
    [
        (
            "conn.cursor()",
            "List[Tuple[Any, ...]]",
        ),
        (
            "conn.cursor(row_factory=rows.dict_row)",
            "List[Dict[str, Any]]",
        ),
        (
            "conn.cursor(row_factory=thing_row)",
            "List[Thing]",
        ),
    ],
)
@pytest.mark.parametrize("server_side", [False, True])
@pytest.mark.parametrize("conn_class", ["Connection", "AsyncConnection"])
def test_fetchsome_type(conn_class, server_side, curs, type, method, mypy):
    await_ = "await" if "Async" in conn_class else ""
    if server_side:
        curs = curs.replace("(", "(name='foo',", 1)
    stmts = f"""\
conn = {await_} psycopg.{conn_class}.connect()
curs = {curs}
obj = {await_} curs.{method}()
"""
    _test_reveal(stmts, type, mypy)


@pytest.mark.slow
@pytest.mark.parametrize("server_side", [False, True])
@pytest.mark.parametrize("conn_class", ["Connection", "AsyncConnection"])
def test_cur_subclass_execute(mypy, conn_class, server_side):
    async_ = "async " if "Async" in conn_class else ""
    await_ = "await" if "Async" in conn_class else ""
    cur_base_class = "".join(
        [
            "Async" if "Async" in conn_class else "",
            "Server" if server_side else "",
            "Cursor",
        ]
    )
    cur_name = "'foo'" if server_side else ""

    src = f"""\
from typing import Any, cast
import psycopg
from psycopg.rows import Row, TupleRow

class MyCursor(psycopg.{cur_base_class}[Row]):
    pass

{async_}def test() -> None:
    conn = {await_} psycopg.{conn_class}.connect()

    cur: MyCursor[TupleRow]
    reveal_type(cur)

    cur = cast(MyCursor[TupleRow], conn.cursor({cur_name}))
    {async_}with cur as cur2:
        reveal_type(cur2)
        cur3 = {await_} cur2.execute("")
        reveal_type(cur3)
"""
    cp = mypy.run_on_source(src)
    out = cp.stdout.decode("utf8", "replace").splitlines()
    assert len(out) == 3
    types = [mypy.get_revealed(line) for line in out]
    assert types[0] == types[1]
    assert types[0] == types[2]


@pytest.fixture(scope="session")
def mypy(tmp_path_factory):
    cache_dir = tmp_path_factory.mktemp(basename="mypy_cache")
    src_dir = tmp_path_factory.mktemp("source")

    class MypyRunner:
        def run_on_file(self, filename):
            cmdline = f"""
                mypy
                --strict
                --show-error-codes --no-color-output --no-error-summary
                --config-file= --cache-dir={cache_dir}
                """.split()
            cmdline.append(filename)
            return sp.run(cmdline, stdout=sp.PIPE, stderr=sp.STDOUT)

        def run_on_source(self, source):
            fn = src_dir / "tmp.py"
            with fn.open("w") as f:
                f.write(source)

            return self.run_on_file(str(fn))

        def get_revealed(self, line):
            """return the type from an output of reveal_type"""
            return re.sub(
                r".*Revealed type is (['\"])([^']+)\1.*", r"\2", line
            ).replace("*", "")

    return MypyRunner()


def _test_reveal(stmts, type, mypy):
    ignore = (
        "" if type.startswith("Optional") else "# type: ignore[assignment]"
    )
    stmts = "\n".join(f"    {line}" for line in stmts.splitlines())

    src = f"""\
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Sequence
from typing import Tuple, Union
import psycopg
from psycopg import rows

class Thing:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

def thing_row(
    cur: Union[psycopg.Cursor[Thing], psycopg.AsyncCursor[Thing]],
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
    cp = mypy.run_on_source(src)
    out = cp.stdout.decode("utf8", "replace").splitlines()
    assert len(out) == 2, "\n".join(out)
    got, want = [mypy.get_revealed(line) for line in out]
    assert got == want
