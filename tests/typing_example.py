# flake8: builtins=reveal_type

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union

from psycopg import Connection, Cursor, ServerCursor, connect, rows
from psycopg import AsyncConnection, AsyncCursor, AsyncServerCursor


def int_row_factory(
    cursor: Union[Cursor[Any], AsyncCursor[Any]]
) -> Callable[[Sequence[int]], int]:
    return lambda values: values[0] if values else 42


@dataclass
class Person:
    name: str
    address: str

    @classmethod
    def row_factory(
        cls, cursor: Union[Cursor[Any], AsyncCursor[Any]]
    ) -> Callable[[Sequence[str]], Person]:
        def mkrow(values: Sequence[str]) -> Person:
            name, address = values
            return cls(name, address)

        return mkrow


def kwargsf(*, foo: int, bar: int, baz: int) -> int:
    return 42


def argsf(foo: int, bar: int, baz: int) -> float:
    return 42.0


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


async def async_check_row_factory_cursor() -> None:
    """Type-check connection.cursor(..., row_factory=<MyRowFactory>) case."""
    conn = await AsyncConnection.connect()

    cur1: AsyncCursor[Any]
    cur1 = conn.cursor()
    r1: Optional[Any]
    r1 = await cur1.fetchone()
    r1 is not None

    cur2: AsyncCursor[int]
    r2: Optional[int]
    async with conn.cursor(row_factory=int_row_factory) as cur2:
        await cur2.execute("select 1")
        r2 = await cur2.fetchone()
        r2 and r2 > 0

    cur3: AsyncServerCursor[Person]
    persons: Sequence[Person]
    async with conn.cursor(name="s", row_factory=Person.row_factory) as cur3:
        await cur3.execute("select * from persons where name like 'al%'")
        persons = await cur3.fetchall()
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


async def async_check_row_factory_connection() -> None:
    """Type-check connect(..., row_factory=<MyRowFactory>) or
    Connection.row_factory cases.
    """
    conn1: AsyncConnection[int]
    cur1: AsyncCursor[int]
    r1: Optional[int]
    conn1 = await AsyncConnection.connect(row_factory=int_row_factory)
    cur1 = await conn1.execute("select 1")
    r1 = await cur1.fetchone()
    r1 != 0
    async with conn1.cursor() as cur1:
        await cur1.execute("select 2")

    conn2: AsyncConnection[Person]
    cur2: AsyncCursor[Person]
    r2: Optional[Person]
    conn2 = await AsyncConnection.connect(row_factory=Person.row_factory)
    cur2 = await conn2.execute("select * from persons")
    r2 = await cur2.fetchone()
    r2 and r2.name
    async with conn2.cursor() as cur2:
        await cur2.execute("select 2")

    cur3: AsyncCursor[Tuple[Any, ...]]
    r3: Optional[Tuple[Any, ...]]
    conn3 = await AsyncConnection.connect()
    cur3 = await conn3.execute("select 3")
    async with conn3.cursor() as cur3:
        await cur3.execute("select 42")
        r3 = await cur3.fetchone()
        r3 and len(r3)


def check_row_factories() -> None:
    conn1 = connect(row_factory=rows.tuple_row)
    v1: Tuple[Any, ...] = conn1.execute("").fetchall()[0]

    conn2 = connect(row_factory=rows.dict_row)
    v2: Dict[str, Any] = conn2.execute("").fetchall()[0]

    conn3 = connect(row_factory=rows.class_row(Person))
    v3: Person = conn3.execute("").fetchall()[0]

    conn4 = connect(row_factory=rows.args_row(argsf))
    v4: float = conn4.execute("").fetchall()[0]

    conn5 = connect(row_factory=rows.kwargs_row(kwargsf))
    v5: int = conn5.execute("").fetchall()[0]

    v1, v2, v3, v4, v5
