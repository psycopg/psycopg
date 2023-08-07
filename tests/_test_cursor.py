"""
Support module for test_cursor[_async].py
"""

import re
from typing import Any, List, Match, Union

import pytest
import psycopg
from psycopg.rows import RowMaker


@pytest.fixture(scope="session")
def _execmany(svcconn):
    cur = svcconn.cursor()
    cur.execute(
        """
        drop table if exists execmany;
        create table execmany (id serial primary key, num integer, data text)
        """
    )


@pytest.fixture(scope="function")
def execmany(svcconn, _execmany):
    cur = svcconn.cursor()
    cur.execute("truncate table execmany")


def ph(cur: Any, query: str) -> str:
    """Change placeholders in a query from %s to $n if testing  a raw cursor"""
    if not isinstance(cur, (psycopg.RawCursor, psycopg.AsyncRawCursor)):
        return query

    if "%(" in query:
        raise pytest.skip("RawCursor only supports positional placeholders")

    n = 1

    def s(m: Match[str]) -> str:
        nonlocal n
        rv = f"${n}"
        n += 1
        return rv

    return re.sub(r"(?<!%)(%[bst])", s, query)


def my_row_factory(
    cursor: Union[psycopg.Cursor[List[str]], psycopg.AsyncCursor[List[str]]]
) -> RowMaker[List[str]]:
    if cursor.description is not None:
        titles = [c.name for c in cursor.description]

        def mkrow(values):
            return [f"{value.upper()}{title}" for title, value in zip(titles, values)]

        return mkrow
    else:
        return psycopg.rows.no_result
