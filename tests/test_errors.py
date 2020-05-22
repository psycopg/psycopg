import pytest
from psycopg3 import errors as e

eur = "\u20ac"


def test_error_diag(conn):
    cur = conn.cursor()
    with pytest.raises(e.DatabaseError) as excinfo:
        cur.execute("select 1 from wat")

    exc = excinfo.value
    diag = exc.diag
    assert diag.sqlstate == "42P01"
    assert diag.severity_nonlocalized == "ERROR"


def test_diag_all_attrs(pgconn, pq):
    res = pgconn.make_empty_result(pq.ExecStatus.NONFATAL_ERROR)
    diag = e.Diagnostic(res)
    for d in pq.DiagnosticField:
        val = getattr(diag, d.name.lower())
        assert val is None or isinstance(val, str)


def test_diag_right_attr(pgconn, pq, monkeypatch):
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
    conn.set_client_encoding(enc)
    cur = conn.cursor()
    cur.execute(
        "do $$begin raise notice 'hello %', chr(8364); end$$ language plpgsql"
    )
    assert msgs == [f"hello {eur}"]


@pytest.mark.parametrize("enc", ["utf8", "latin9"])
def test_error_encoding(conn, enc):
    conn.set_client_encoding(enc)
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
