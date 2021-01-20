"""
Prepared statements tests
"""

import datetime as dt
from decimal import Decimal

import pytest


def test_connection_attributes(conn, monkeypatch):
    assert conn.prepare_threshold == 5
    assert conn.prepared_max == 100

    # They are on the class
    monkeypatch.setattr(conn.__class__, "prepare_threshold", 10)
    assert conn.prepare_threshold == 10

    monkeypatch.setattr(conn.__class__, "prepared_max", 200)
    assert conn.prepared_max == 200


def test_dont_prepare(conn):
    cur = conn.cursor()
    for i in range(10):
        cur.execute("select %s::int", [i], prepare=False)

    cur.execute("select count(*) from pg_prepared_statements")
    assert cur.fetchone() == (0,)


def test_do_prepare(conn):
    cur = conn.cursor()
    cur.execute("select %s::int", [10], prepare=True)
    cur.execute("select count(*) from pg_prepared_statements")
    assert cur.fetchone() == (1,)


def test_auto_prepare(conn):
    cur = conn.cursor()
    res = []
    for i in range(10):
        cur.execute("select count(*) from pg_prepared_statements")
        res.append(cur.fetchone()[0])

    assert res == [0] * 5 + [1] * 5


def test_dont_prepare_conn(conn):
    for i in range(10):
        conn.execute("select %s::int", [i], prepare=False)

    cur = conn.execute("select count(*) from pg_prepared_statements")
    assert cur.fetchone() == (0,)


def test_do_prepare_conn(conn):
    conn.execute("select %s::int", [10], prepare=True)
    cur = conn.execute("select count(*) from pg_prepared_statements")
    assert cur.fetchone() == (1,)


def test_auto_prepare_conn(conn):
    res = []
    for i in range(10):
        cur = conn.execute("select count(*) from pg_prepared_statements")
        res.append(cur.fetchone()[0])

    assert res == [0] * 5 + [1] * 5


def test_prepare_disable(conn):
    conn.prepare_threshold = None
    res = []
    for i in range(10):
        cur = conn.execute("select count(*) from pg_prepared_statements")
        res.append(cur.fetchone()[0])

    assert res == [0] * 10
    assert not conn._prepared._prepared


def test_no_prepare_multi(conn):
    res = []
    for i in range(10):
        cur = conn.execute(
            "select count(*) from pg_prepared_statements; select 1"
        )
        res.append(cur.fetchone()[0])

    assert res == [0] * 10


def test_no_prepare_error(conn):
    conn.autocommit = True
    for i in range(10):
        with pytest.raises(conn.ProgrammingError):
            conn.execute("select wat")

    cur = conn.execute("select count(*) from pg_prepared_statements")
    assert cur.fetchone() == (0,)


@pytest.mark.parametrize(
    "query",
    [
        "create table test_no_prepare ()",
        "notify foo, 'bar'",
        "set timezone = utc",
        "select num from prepared_test",
        "insert into prepared_test (num) values (1)",
        "update prepared_test set num = num * 2",
        "delete from prepared_test where num > 10",
    ],
)
def test_misc_statement(conn, query):
    conn.execute("create table prepared_test (num int)", prepare=False)
    conn.prepare_threshold = 0
    conn.execute(query)
    cur = conn.execute(
        "select count(*) from pg_prepared_statements", prepare=False
    )
    assert cur.fetchone() == (1,)


def test_params_types(conn):
    conn.execute(
        "select %s, %s, %s",
        [dt.date(2020, 12, 10), 42, Decimal(42)],
        prepare=True,
    )
    cur = conn.execute("select parameter_types from pg_prepared_statements")
    (rec,) = cur.fetchall()
    assert rec[0] == ["date", "smallint", "numeric"]


def test_evict_lru(conn):
    conn.prepared_max = 5
    for i in range(10):
        conn.execute("select 'a'")
        conn.execute(f"select {i}")

    assert len(conn._prepared._prepared) == 5
    assert conn._prepared._prepared[b"select 'a'", ()] == b"_pg3_0"
    for i in [9, 8, 7, 6]:
        assert conn._prepared._prepared[f"select {i}".encode("utf8"), ()] == 1

    cur = conn.execute("select statement from pg_prepared_statements")
    assert cur.fetchall() == [("select 'a'",)]


def test_evict_lru_deallocate(conn):
    conn.prepared_max = 5
    conn.prepare_threshold = 0
    for i in range(10):
        conn.execute("select 'a'")
        conn.execute(f"select {i}")

    assert len(conn._prepared._prepared) == 5
    for i in [9, 8, 7, 6, "'a'"]:
        assert conn._prepared._prepared[
            f"select {i}".encode("utf8"), ()
        ].startswith(b"_pg3_")

    cur = conn.execute(
        "select statement from pg_prepared_statements order by prepare_time",
        prepare=False,
    )
    assert cur.fetchall() == [(f"select {i}",) for i in ["'a'", 6, 7, 8, 9]]


def test_different_types(conn):
    conn.prepare_threshold = 0
    conn.execute("select %s", [None])
    conn.execute("select %s", [dt.date(2000, 1, 1)])
    conn.execute("select %s", [42])
    conn.execute("select %s", [41])
    conn.execute("select %s", [dt.date(2000, 1, 2)])
    cur = conn.execute(
        "select parameter_types from pg_prepared_statements order by prepare_time",
        prepare=False,
    )
    assert cur.fetchall() == [(["text"],), (["date"],), (["smallint"],)]


def test_untyped_json(conn):
    conn.prepare_threshold = 1
    conn.execute("create table testjson(data jsonb)")

    for i in range(2):
        conn.execute("insert into testjson (data) values (%s)", ["{}"])

    cur = conn.execute("select parameter_types from pg_prepared_statements")
    assert cur.fetchall() == [(["jsonb"],)]
