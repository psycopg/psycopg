import datetime as dt

import pytest
import psycopg
from psycopg import rows

from .utils import gc_collect, gc_count
from .fix_crdb import crdb_encoding


@pytest.fixture
def conn(conn):
    conn.cursor_factory = psycopg.ClientCursor
    return conn


def test_default_cursor(conn):
    cur = conn.cursor()
    assert type(cur) is psycopg.ClientCursor


def test_from_cursor_factory(conn_cls, dsn):
    with conn_cls.connect(dsn, cursor_factory=psycopg.ClientCursor) as conn:
        cur = conn.cursor()
        assert type(cur) is psycopg.ClientCursor


def test_execute_many_results_param(conn):
    cur = conn.cursor()
    assert cur.nextset() is None

    rv = cur.execute("select %s; select generate_series(1, %s)", ("foo", 3))
    assert rv is cur
    assert cur.fetchall() == [("foo",)]
    assert cur.rowcount == 1
    assert cur.nextset()
    assert cur.fetchall() == [(1,), (2,), (3,)]
    assert cur.nextset() is None

    cur.close()
    assert cur.nextset() is None


def test_query_params_execute(conn):
    cur = conn.cursor()
    assert cur._query is None

    cur.execute("select %t, %s::text", [1, None])
    assert cur._query is not None
    assert cur._query.query == b"select 1, NULL::text"
    assert cur._query.params == (b"1", b"NULL")

    cur.execute("select 1")
    assert cur._query.query == b"select 1"
    assert not cur._query.params

    with pytest.raises(psycopg.DataError):
        cur.execute("select %t::int", ["wat"])

    assert cur._query.query == b"select 'wat'::int"
    assert cur._query.params == (b"'wat'",)


def test_query_params_executemany(conn):
    cur = conn.cursor()

    cur.executemany("select %t, %t", [[1, 2], [3, 4]])
    assert cur._query.query == b"select 3, 4"
    assert cur._query.params == (b"3", b"4")


@pytest.mark.slow
@pytest.mark.parametrize("fetch", ["one", "many", "all", "iter"])
@pytest.mark.parametrize("row_factory", ["tuple_row", "dict_row", "namedtuple_row"])
def test_leak(conn_cls, dsn, faker, fetch, row_factory):
    faker.choose_schema(ncols=5)
    faker.make_records(10)
    row_factory = getattr(rows, row_factory)

    def work():
        with conn_cls.connect(dsn) as conn, conn.transaction(force_rollback=True):
            with psycopg.ClientCursor(conn, row_factory=row_factory) as cur:
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


@pytest.mark.parametrize(
    "query, params, want",
    [
        ("select 'hello'", (), "select 'hello'"),
        ("select %s, %s", ([1, dt.date(2020, 1, 1)],), "select 1, '2020-01-01'::date"),
        ("select %(foo)s, %(foo)s", ({"foo": "x"},), "select 'x', 'x'"),
        ("select %%", (), "select %%"),
        ("select %%, %s", (["a"],), "select %, 'a'"),
        ("select %%, %(foo)s", ({"foo": "x"},), "select %, 'x'"),
        ("select %%s, %(foo)s", ({"foo": "x"},), "select %s, 'x'"),
    ],
)
def test_mogrify(conn, query, params, want):
    cur = conn.cursor()
    got = cur.mogrify(query, *params)
    assert got == want


@pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
def test_mogrify_encoding(conn, encoding):
    conn.execute(f"set client_encoding to {encoding}")
    q = conn.cursor().mogrify("select %(s)s", {"s": "\u20ac"})
    assert q == "select '\u20ac'"


@pytest.mark.parametrize("encoding", [crdb_encoding("latin1")])
def test_mogrify_badenc(conn, encoding):
    conn.execute(f"set client_encoding to {encoding}")
    with pytest.raises(UnicodeEncodeError):
        conn.cursor().mogrify("select %(s)s", {"s": "\u20ac"})
