import pytest


def test_execute_many(conn):
    cur = conn.cursor()
    rv = cur.execute("select 'foo'; select 'bar'")
    assert rv is cur
    assert len(cur._results) == 2
    assert cur.pgresult.get_value(0, 0) == b"foo"
    assert cur.nextset()
    assert cur.pgresult.get_value(0, 0) == b"bar"
    assert cur.nextset() is None


def test_execute_sequence(conn):
    cur = conn.cursor()
    rv = cur.execute("select %s, %s, %s", [1, "foo", None])
    assert rv is cur
    assert len(cur._results) == 1
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert cur.pgresult.get_value(0, 1) == b"foo"
    assert cur.pgresult.get_value(0, 2) is None
    assert cur.nextset() is None


def test_fetchone(conn):
    cur = conn.cursor()
    cur.execute("select %s, %s, %s", [1, "foo", None])
    assert cur.pgresult.fformat(0) == 0

    row = cur.fetchone()
    assert row[0] == 1
    assert row[1] == "foo"
    assert row[2] is None
    row = cur.fetchone()
    assert row is None


def test_execute_binary_result(conn):
    cur = conn.cursor(binary=True)
    cur.execute("select %s, %s", ["foo", None])
    assert cur.pgresult.fformat(0) == 1

    row = cur.fetchone()
    assert row[0] == "foo"
    assert row[1] is None
    row = cur.fetchone()
    assert row is None


@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
def test_query_encode(conn, encoding):
    conn.set_client_encoding(encoding)
    cur = conn.cursor()
    (res,) = cur.execute("select '\u20ac'").fetchone()
    assert res == "\u20ac"


def test_query_badenc(conn):
    conn.set_client_encoding("latin1")
    cur = conn.cursor()
    with pytest.raises(UnicodeEncodeError):
        cur.execute("select '\u20ac'")
