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
