import pytest
import psycopg
from psycopg import pq, rows, errors as e
from psycopg.adapt import PyFormat

from .test_cursor import ph
from .utils import gc_collect, gc_count


@pytest.fixture
def conn(conn):
    conn.cursor_factory = psycopg.RawCursor
    return conn


def test_default_cursor(conn):
    cur = conn.cursor()
    assert type(cur) is psycopg.RawCursor


def test_str(conn):
    cur = conn.cursor()
    assert "psycopg.RawCursor" in str(cur)


def test_sequence_only(conn):
    cur = conn.cursor()
    cur.execute("select 1", ())
    assert cur.fetchone() == (1,)

    with pytest.raises(TypeError, match="sequence"):
        cur.execute("select 1", {})


def test_execute_many_results_param(conn):
    cur = conn.cursor()
    # Postgres raises SyntaxError, CRDB raises InvalidPreparedStatementDefinition
    with pytest.raises((e.SyntaxError, e.InvalidPreparedStatementDefinition)):
        cur.execute("select $1; select generate_series(1, $2)", ("foo", 3))


def test_query_params_execute(conn):
    cur = conn.cursor()
    assert cur._query is None

    cur.execute("select $1, $2::text", [1, None])
    assert cur._query is not None
    assert cur._query.query == b"select $1, $2::text"
    assert cur._query.params == [b"\x00\x01", None]

    cur.execute("select 1")
    assert cur._query.query == b"select 1"
    assert not cur._query.params

    with pytest.raises(psycopg.DataError):
        cur.execute("select $1::int", ["wat"])

    assert cur._query.query == b"select $1::int"
    assert cur._query.params == [b"wat"]


def test_query_params_executemany(conn):
    cur = conn.cursor()

    cur.executemany("select $1, $2", [[1, 2], [3, 4]])
    assert cur._query.query == b"select $1, $2"
    assert cur._query.params == [b"\x00\x03", b"\x00\x04"]


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
                cur.execute(ph(cur, faker.select_stmt))

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
