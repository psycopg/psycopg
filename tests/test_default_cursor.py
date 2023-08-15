"""
Tests for psycopg.Cursor that are not supposed to pass for subclasses.
"""

import pytest
import psycopg
from psycopg import pq, rows, errors as e
from psycopg.adapt import PyFormat

from .utils import gc_collect, gc_count


def test_default_cursor(conn):
    cur = conn.cursor()
    assert type(cur) is psycopg.Cursor


def test_from_cursor_factory(conn_cls, dsn):
    with conn_cls.connect(dsn, cursor_factory=psycopg.ClientCursor) as conn:
        cur = conn.cursor()
        assert type(cur) is psycopg.ClientCursor


def test_str(conn):
    cur = conn.cursor()
    assert "psycopg.Cursor" in str(cur)


def test_execute_many_results_param(conn):
    cur = conn.cursor()
    # Postgres raises SyntaxError, CRDB raises InvalidPreparedStatementDefinition
    with pytest.raises((e.SyntaxError, e.InvalidPreparedStatementDefinition)):
        cur.execute("select %s; select generate_series(1, %s)", ("foo", 3))


def test_query_params_execute(conn):
    cur = conn.cursor()
    assert cur._query is None

    cur.execute("select %t, %s::text", [1, None])
    assert cur._query is not None
    assert cur._query.query == b"select $1, $2::text"
    assert cur._query.params == [b"1", None]

    cur.execute("select 1")
    assert cur._query.query == b"select 1"
    assert not cur._query.params

    with pytest.raises(psycopg.DataError):
        cur.execute("select %t::int", ["wat"])

    assert cur._query.query == b"select $1::int"
    assert cur._query.params == [b"wat"]


def test_query_params_executemany(conn):
    cur = conn.cursor()

    cur.executemany("select %t, %t", [[1, 2], [3, 4]])
    assert cur._query.query == b"select $1, $2"
    assert cur._query.params == [b"3", b"4"]


@pytest.mark.slow
@pytest.mark.parametrize("fmt", PyFormat)
@pytest.mark.parametrize("fmt_out", pq.Format)
@pytest.mark.parametrize("fetch", ["one", "many", "all", "iter"])
@pytest.mark.parametrize("row_factory", ["tuple_row", "dict_row", "namedtuple_row"])
def test_leak(conn_cls, dsn, faker, fmt, fmt_out, fetch, row_factory):
    faker.format = fmt
    faker.choose_schema(ncols=5)
    faker.make_records(10)
    row_factory = getattr(rows, row_factory)

    def work():
        with conn_cls.connect(dsn) as conn, conn.transaction(force_rollback=True):
            with conn.cursor(binary=fmt_out, row_factory=row_factory) as cur:
                cur.execute(faker.drop_stmt)
                cur.execute(faker.create_stmt)
                with faker.find_insert_problem(conn):
                    cur.executemany(faker.insert_stmt, faker.records)
                cur.execute(faker.select_stmt)

                if fetch == "one":
                    while True:
                        tmp = cur.fetchone()
                        if tmp is None:
                            break
                elif fetch == "many":
                    while True:
                        tmp = cur.fetchmany(3)
                        if not tmp:
                            break
                elif fetch == "all":
                    cur.fetchall()
                elif fetch == "iter":
                    for rec in cur:
                        pass

    n = []
    gc_collect()
    for i in range(3):
        work()
        gc_collect()
        n.append(gc_count())
    assert n[0] == n[1] == n[2], f"objects leaked: {n[1] - n[0]}, {n[2] - n[1]}"
