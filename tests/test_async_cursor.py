import pytest
import psycopg3


def test_close(aconn, loop):
    cur = aconn.cursor()
    assert not cur.closed
    cur.close()
    assert cur.closed

    with pytest.raises(psycopg3.OperationalError):
        loop.run_until_complete(cur.execute("select 'foo'"))

    cur.close()
    assert cur.closed


def test_execute_many_results(aconn, loop):
    cur = aconn.cursor()
    rv = loop.run_until_complete(cur.execute("select 'foo'; select 'bar'"))
    assert rv is cur
    assert len(cur._results) == 2
    assert cur.pgresult.get_value(0, 0) == b"foo"
    assert cur.nextset()
    assert cur.pgresult.get_value(0, 0) == b"bar"
    assert cur.nextset() is None


def test_execute_sequence(aconn, loop):
    cur = aconn.cursor()
    rv = loop.run_until_complete(
        cur.execute("select %s, %s, %s", [1, "foo", None])
    )
    assert rv is cur
    assert len(cur._results) == 1
    assert cur.pgresult.get_value(0, 0) == b"1"
    assert cur.pgresult.get_value(0, 1) == b"foo"
    assert cur.pgresult.get_value(0, 2) is None
    assert cur.nextset() is None
