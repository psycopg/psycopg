"""
Prepared statements tests
"""

import datetime as dt
from decimal import Decimal

import pytest

from psycopg.rows import namedtuple_row


@pytest.mark.parametrize("value", [None, 0, 3])
def test_prepare_threshold_init(conn_cls, dsn, value):
    with conn_cls.connect(dsn, prepare_threshold=value) as conn:
        assert conn.prepare_threshold == value


def test_dont_prepare(conn):
    cur = conn.cursor()
    for i in range(10):
        cur.execute("select %s::int", [i], prepare=False)

    stmts = get_prepared_statements(conn)
    assert len(stmts) == 0


def test_do_prepare(conn):
    cur = conn.cursor()
    cur.execute("select %s::int", [10], prepare=True)
    stmts = get_prepared_statements(conn)
    assert len(stmts) == 1


def test_auto_prepare(conn):
    res = []
    for i in range(10):
        conn.execute("select %s::int", [0])
        stmts = get_prepared_statements(conn)
        res.append(len(stmts))

    assert res == [0] * 5 + [1] * 5


def test_dont_prepare_conn(conn):
    for i in range(10):
        conn.execute("select %s::int", [i], prepare=False)

    stmts = get_prepared_statements(conn)
    assert len(stmts) == 0


def test_do_prepare_conn(conn):
    conn.execute("select %s::int", [10], prepare=True)
    stmts = get_prepared_statements(conn)
    assert len(stmts) == 1


def test_auto_prepare_conn(conn):
    res = []
    for i in range(10):
        conn.execute("select %s", [0])
        stmts = get_prepared_statements(conn)
        res.append(len(stmts))

    assert res == [0] * 5 + [1] * 5


def test_prepare_disable(conn):
    conn.prepare_threshold = None
    res = []
    for i in range(10):
        conn.execute("select %s", [0])
        stmts = get_prepared_statements(conn)
        res.append(len(stmts))

    assert res == [0] * 10
    assert not conn._prepared._names
    assert not conn._prepared._counts


def test_no_prepare_multi(conn):
    res = []
    for i in range(10):
        conn.execute("select 1; select 2")
        stmts = get_prepared_statements(conn)
        res.append(len(stmts))

    assert res == [0] * 10


def test_no_prepare_multi_with_drop(conn):
    conn.execute("select 1", prepare=True)

    for i in range(10):
        conn.execute("drop table if exists noprep; create table noprep()")

    stmts = get_prepared_statements(conn)
    assert len(stmts) == 0


def test_no_prepare_error(conn):
    conn.autocommit = True
    for i in range(10):
        with pytest.raises(conn.ProgrammingError):
            conn.execute("select wat")

    stmts = get_prepared_statements(conn)
    assert len(stmts) == 0


@pytest.mark.parametrize(
    "query",
    [
        "create table test_no_prepare ()",
        pytest.param("notify foo, 'bar'", marks=pytest.mark.crdb_skip("notify")),
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
    stmts = get_prepared_statements(conn)
    assert len(stmts) == 1


def test_params_types(conn):
    conn.execute(
        "select %s, %s, %s",
        [dt.date(2020, 12, 10), 42, Decimal(42)],
        prepare=True,
    )
    stmts = get_prepared_statements(conn)
    want = [stmt.parameter_types for stmt in stmts]
    assert want == [["date", "smallint", "numeric"]]


def test_evict_lru(conn):
    conn.prepared_max = 5
    for i in range(10):
        conn.execute("select 'a'")
        conn.execute(f"select {i}")

    assert len(conn._prepared._names) == 1
    assert conn._prepared._names[b"select 'a'", ()] == b"_pg3_0"
    for i in [9, 8, 7, 6]:
        assert conn._prepared._counts[f"select {i}".encode(), ()] == 1

    stmts = get_prepared_statements(conn)
    assert len(stmts) == 1
    assert stmts[0].statement == "select 'a'"


def test_evict_lru_deallocate(conn):
    conn.prepared_max = 5
    conn.prepare_threshold = 0
    for i in range(10):
        conn.execute("select 'a'")
        conn.execute(f"select {i}")

    assert len(conn._prepared._names) == 5
    for j in [9, 8, 7, 6, "'a'"]:
        name = conn._prepared._names[f"select {j}".encode(), ()]
        assert name.startswith(b"_pg3_")

    stmts = get_prepared_statements(conn)
    stmts.sort(key=lambda rec: rec.prepare_time)
    got = [stmt.statement for stmt in stmts]
    assert got == [f"select {i}" for i in ["'a'", 6, 7, 8, 9]]


def test_different_types(conn):
    conn.prepare_threshold = 0
    conn.execute("select %s", [None])
    conn.execute("select %s", [dt.date(2000, 1, 1)])
    conn.execute("select %s", [42])
    conn.execute("select %s", [41])
    conn.execute("select %s", [dt.date(2000, 1, 2)])

    stmts = get_prepared_statements(conn)
    stmts.sort(key=lambda rec: rec.prepare_time)
    got = [stmt.parameter_types for stmt in stmts]
    assert got == [["text"], ["date"], ["smallint"]]


def test_untyped_json(conn):
    conn.prepare_threshold = 1
    conn.execute("create table testjson(data jsonb)")

    for i in range(2):
        conn.execute("insert into testjson (data) values (%s)", ["{}"])

    stmts = get_prepared_statements(conn)
    got = [stmt.parameter_types for stmt in stmts]
    assert got == [["jsonb"]]


def test_change_type_execute(conn):
    conn.prepare_threshold = 0
    for i in range(3):
        conn.execute("CREATE TYPE prepenum AS ENUM ('foo', 'bar', 'baz')")
        conn.execute("CREATE TABLE preptable(id integer, bar prepenum[])")
        conn.cursor().execute(
            "INSERT INTO preptable (bar) VALUES (%(enum_col)s::prepenum[])",
            {"enum_col": ["foo"]},
        )
        conn.rollback()


def test_change_type_executemany(conn):
    for i in range(3):
        conn.execute("CREATE TYPE prepenum AS ENUM ('foo', 'bar', 'baz')")
        conn.execute("CREATE TABLE preptable(id integer, bar prepenum[])")
        conn.cursor().executemany(
            "INSERT INTO preptable (bar) VALUES (%(enum_col)s::prepenum[])",
            [{"enum_col": ["foo"]}, {"enum_col": ["foo", "bar"]}],
        )
        conn.rollback()


@pytest.mark.crdb("skip", reason="can't re-create a type")
def test_change_type(conn):
    conn.prepare_threshold = 0
    conn.execute("CREATE TYPE prepenum AS ENUM ('foo', 'bar', 'baz')")
    conn.execute("CREATE TABLE preptable(id integer, bar prepenum[])")
    conn.cursor().execute(
        "INSERT INTO preptable (bar) VALUES (%(enum_col)s::prepenum[])",
        {"enum_col": ["foo"]},
    )
    conn.execute("DROP TABLE preptable")
    conn.execute("DROP TYPE prepenum")
    conn.execute("CREATE TYPE prepenum AS ENUM ('foo', 'bar', 'baz')")
    conn.execute("CREATE TABLE preptable(id integer, bar prepenum[])")
    conn.cursor().execute(
        "INSERT INTO preptable (bar) VALUES (%(enum_col)s::prepenum[])",
        {"enum_col": ["foo"]},
    )

    stmts = get_prepared_statements(conn)
    assert len(stmts) == 3


def test_change_type_savepoint(conn):
    conn.prepare_threshold = 0
    with conn.transaction():
        for i in range(3):
            with pytest.raises(ZeroDivisionError):
                with conn.transaction():
                    conn.execute("CREATE TYPE prepenum AS ENUM ('foo', 'bar', 'baz')")
                    conn.execute("CREATE TABLE preptable(id integer, bar prepenum[])")
                    conn.cursor().execute(
                        "INSERT INTO preptable (bar) VALUES (%(enum_col)s::prepenum[])",
                        {"enum_col": ["foo"]},
                    )
                    raise ZeroDivisionError()


def get_prepared_statements(conn):
    cur = conn.cursor(row_factory=namedtuple_row)
    cur.execute(
        # CRDB has 'PREPARE name AS' in the statement.
        r"""
select name,
    regexp_replace(statement, 'prepare _pg3_\d+ as ', '', 'i') as statement,
    prepare_time,
    parameter_types
from pg_prepared_statements
where name != ''
        """,
        prepare=False,
    )
    return cur.fetchall()
