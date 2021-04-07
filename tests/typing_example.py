# flake8: builtins=reveal_type

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence

from psycopg3 import BaseCursor, Cursor, ServerCursor, connect


def int_row_factory(
    cursor: BaseCursor[Any, int]
) -> Callable[[Sequence[int]], int]:
    return lambda values: values[0] if values else 42


@dataclass
class Person:
    name: str
    address: str

    @classmethod
    def row_factory(
        cls, cursor: BaseCursor[Any, Person]
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

    This example is incomplete because Connection is not generic on Row, hence
    all the Any, which we aim at getting rid of.
    """
    cur1: Cursor[Any]
    r1: Any
    conn1 = connect(row_factory=int_row_factory)
    cur1 = conn1.execute("select 1")
    r1 = cur1.fetchone()
    r1 != 0
    with conn1.cursor() as cur1:
        cur1.execute("select 2")

    cur2: Cursor[Any]
    r2: Any
    conn2 = connect(row_factory=Person.row_factory)
    cur2 = conn2.execute("select * from persons")
    r2 = cur2.fetchone()
    r2 and r2.name
    with conn2.cursor() as cur2:
        cur2.execute("select 2")

    cur3: Cursor[Any]
    r3: Optional[Any]
    conn3 = connect()
    cur3 = conn3.execute("select 3")
    with conn3.cursor() as cur3:
        cur3.execute("select 42")
        r3 = cur3.fetchone()
        r3 and len(r3)
