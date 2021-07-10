# flake8: builtins=reveal_type

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence, Tuple, Union

from psycopg import AnyCursor, Connection, Cursor, ServerCursor, connect
from psycopg import pq
from psycopg.proto import Dumper, Loader, AdaptContext, PyFormat, Buffer


def int_row_factory(cursor: AnyCursor[int]) -> Callable[[Sequence[int]], int]:
    return lambda values: values[0] if values else 42


@dataclass
class Person:
    name: str
    address: str

    @classmethod
    def row_factory(
        cls, cursor: AnyCursor[Person]
    ) -> Callable[[Sequence[str]], Person]:
        def mkrow(values: Sequence[str]) -> Person:
            name, address = values
            return cls(name, address)

        return mkrow


def check_row_factory_cursor() -> None:
    """Type-check connection.cursor(..., row_factory=<MyRowFactory>) case."""
    conn = connect()

    cur1: Cursor[Any]
    cur1 = conn.cursor()
    r1: Optional[Any]
    r1 = cur1.fetchone()
    r1 is not None

    cur2: Cursor[int]
    r2: Optional[int]
    with conn.cursor(row_factory=int_row_factory) as cur2:
        cur2.execute("select 1")
        r2 = cur2.fetchone()
        r2 and r2 > 0

    cur3: ServerCursor[Person]
    persons: Sequence[Person]
    with conn.cursor(name="s", row_factory=Person.row_factory) as cur3:
        cur3.execute("select * from persons where name like 'al%'")
        persons = cur3.fetchall()
        persons[0].address


def check_row_factory_connection() -> None:
    """Type-check connect(..., row_factory=<MyRowFactory>) or
    Connection.row_factory cases.
    """
    conn1: Connection[int]
    cur1: Cursor[int]
    r1: Optional[int]
    conn1 = connect(row_factory=int_row_factory)
    cur1 = conn1.execute("select 1")
    r1 = cur1.fetchone()
    r1 != 0
    with conn1.cursor() as cur1:
        cur1.execute("select 2")

    conn2: Connection[Person]
    cur2: Cursor[Person]
    r2: Optional[Person]
    conn2 = connect(row_factory=Person.row_factory)
    cur2 = conn2.execute("select * from persons")
    r2 = cur2.fetchone()
    r2 and r2.name
    with conn2.cursor() as cur2:
        cur2.execute("select 2")

    cur3: Cursor[Tuple[Any, ...]]
    r3: Optional[Tuple[Any, ...]]
    conn3 = connect()
    cur3 = conn3.execute("select 3")
    with conn3.cursor() as cur3:
        cur3.execute("select 42")
        r3 = cur3.fetchone()
        r3 and len(r3)


def f() -> None:
    d: Dumper = MyStrDumper(str, None)
    assert d.dump("abc") == b"abcabc"
    assert d.quote("abc") == b"'abcabc'"

    lo: Loader = MyTextLoader(0, None)
    assert lo.load(b"abc") == "abcabc"


class MyStrDumper:
    format = pq.Format.TEXT

    def __init__(self, cls: type, context: Optional[AdaptContext] = None):
        self._cls = cls
        self.oid = 25  # text

    def dump(self, obj: str) -> bytes:
        return (obj * 2).encode("utf-8")

    def quote(self, obj: str) -> bytes:
        value = self.dump(obj)
        esc = pq.Escaping()
        return b"'%s'" % esc.escape_string(value.replace(b"h", b"q"))

    def get_key(self, obj: str, format: PyFormat) -> type:
        return self._cls

    def upgrade(self, obj: str, format: PyFormat) -> "MyStrDumper":
        return self


class MyTextLoader:
    format = pq.Format.TEXT

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        pass

    def load(self, data: Buffer) -> str:
        return (bytes(data) * 2).decode("utf-8")


# This should be the definition of psycopg.adapt.DumperKey, but mypy doesn't
# support recursive types. When it will, this statement will give an error
# (unused type: ignore) so we can fix our definition.
_DumperKey = Union[type, Tuple[Union[type, "_DumperKey"]]]  # type: ignore
