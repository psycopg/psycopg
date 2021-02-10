import gc
import pickle
from weakref import ref

import pytest

from psycopg3 import pq
from psycopg3 import errors as e

eur = "\u20ac"


def test_error_diag(conn):
    cur = conn.cursor()
    with pytest.raises(e.DatabaseError) as excinfo:
        cur.execute("select 1 from wat")

    exc = excinfo.value
    diag = exc.diag
    assert diag.sqlstate == "42P01"
    assert diag.severity == "ERROR"


def test_diag_all_attrs(pgconn):
    res = pgconn.make_empty_result(pq.ExecStatus.NONFATAL_ERROR)
    diag = e.Diagnostic(res)
    for d in pq.DiagnosticField:
        val = getattr(diag, d.name.lower())
        assert val is None or isinstance(val, str)


def test_diag_right_attr(pgconn, monkeypatch):
    res = pgconn.make_empty_result(pq.ExecStatus.NONFATAL_ERROR)
    diag = e.Diagnostic(res)

    checked = []

    def check_val(self, v):
        nonlocal to_check
        assert to_check == v
        checked.append(v)
        return None

    monkeypatch.setattr(e.Diagnostic, "_error_message", check_val)

    for to_check in pq.DiagnosticField:
        getattr(diag, to_check.name.lower())

    assert len(checked) == len(pq.DiagnosticField)


def test_diag_attr_values(conn):
    cur = conn.cursor()
    cur.execute(
        """
        create temp table test_exc (
            data int constraint chk_eq1 check (data = 1)
        )"""
    )
    with pytest.raises(e.Error) as exc:
        cur.execute("insert into test_exc values(2)")
    diag = exc.value.diag
    assert diag.sqlstate == "23514"
    assert diag.schema_name[:7] == "pg_temp"
    assert diag.table_name == "test_exc"
    assert diag.constraint_name == "chk_eq1"
    if conn.pgconn.server_version >= 90600:
        assert diag.severity_nonlocalized == "ERROR"


@pytest.mark.parametrize("enc", ["utf8", "latin9"])
def test_diag_encoding(conn, enc):
    msgs = []
    conn.pgconn.exec_(b"set client_min_messages to notice")
    conn.add_notice_handler(lambda diag: msgs.append(diag.message_primary))
    conn.client_encoding = enc
    cur = conn.cursor()
    cur.execute(
        "do $$begin raise notice 'hello %', chr(8364); end$$ language plpgsql"
    )
    assert msgs == [f"hello {eur}"]


@pytest.mark.parametrize("enc", ["utf8", "latin9"])
def test_error_encoding(conn, enc):
    conn.client_encoding = enc
    cur = conn.cursor()
    with pytest.raises(e.DatabaseError) as excinfo:
        cur.execute(
            """
            do $$begin
                execute format('insert into "%s" values (1)', chr(8364));
            end$$ language plpgsql;
            """
        )

    diag = excinfo.value.diag
    assert f'"{eur}"' in diag.message_primary
    assert diag.sqlstate == "42P01"


def test_exception_class(conn):
    cur = conn.cursor()

    with pytest.raises(e.DatabaseError) as excinfo:
        cur.execute("select * from nonexist")

    assert isinstance(excinfo.value, e.UndefinedTable)
    assert isinstance(excinfo.value, conn.ProgrammingError)


def test_exception_class_fallback(conn):
    cur = conn.cursor()

    x = e._sqlcodes.pop("42P01")
    try:
        with pytest.raises(e.Error) as excinfo:
            cur.execute("select * from nonexist")
    finally:
        e._sqlcodes["42P01"] = x

    assert type(excinfo.value) is conn.ProgrammingError


def test_lookup():
    assert e.lookup("42P01") is e.UndefinedTable

    with pytest.raises(KeyError):
        e.lookup("XXXXX")


def test_error_pickle(conn):
    cur = conn.cursor()
    with pytest.raises(e.DatabaseError) as excinfo:
        cur.execute("select 1 from wat")

    exc = pickle.loads(pickle.dumps(excinfo.value))
    assert isinstance(exc, e.UndefinedTable)
    assert exc.diag.sqlstate == "42P01"


def test_diag_pickle(conn):
    cur = conn.cursor()
    with pytest.raises(e.DatabaseError) as excinfo:
        cur.execute("select 1 from wat")

    diag1 = excinfo.value.diag
    diag2 = pickle.loads(pickle.dumps(diag1))

    assert isinstance(diag2, type(diag1))
    for f in pq.DiagnosticField:
        assert getattr(diag1, f.name.lower()) == getattr(diag2, f.name.lower())

    assert diag2.sqlstate == "42P01"


def test_diag_survives_cursor(conn):
    cur = conn.cursor()
    with pytest.raises(e.Error) as exc:
        cur.execute("select * from nosuchtable")

    diag = exc.value.diag
    del exc
    w = ref(cur)
    del cur
    gc.collect()
    assert w() is None
    assert diag.sqlstate == "42P01"


def test_diag_independent(conn):
    conn.autocommit = True
    cur = conn.cursor()

    with pytest.raises(e.Error) as exc1:
        cur.execute("l'acqua e' poca e 'a papera nun galleggia")

    with pytest.raises(e.Error) as exc2:
        cur.execute("select level from water where ducks > 1")

    assert exc1.value.diag.sqlstate == "42601"
    assert exc2.value.diag.sqlstate == "42P01"


def test_diag_from_commit(conn):
    cur = conn.cursor()
    cur.execute(
        """
        create temp table test_deferred (
           data int primary key,
           ref int references test_deferred (data)
               deferrable initially deferred)
    """
    )
    cur.execute("insert into test_deferred values (1,2)")
    with pytest.raises(e.Error) as exc:
        conn.commit()

    assert exc.value.diag.sqlstate == "23503"


@pytest.mark.asyncio
async def test_diag_from_commit_async(aconn):
    cur = aconn.cursor()
    await cur.execute(
        """
        create temp table test_deferred (
           data int primary key,
           ref int references test_deferred (data)
               deferrable initially deferred)
    """
    )
    await cur.execute("insert into test_deferred values (1,2)")
    with pytest.raises(e.Error) as exc:
        await aconn.commit()

    assert exc.value.diag.sqlstate == "23503"
