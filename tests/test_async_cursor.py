import pytest


def test_execute_many(aconn, loop):
    cur = aconn.cursor()
    rv = loop.run_until_complete(cur.execute("select 'foo'; select 'bar'"))
    assert rv is cur
    assert len(cur._results) == 2
    assert cur._result.get_value(0, 0) == b"foo"
    assert cur.nextset()
    assert cur._result.get_value(0, 0) == b"bar"
    assert cur.nextset() is None


def test_execute_sequence(aconn, loop):
    if aconn.pgconn.server_version < 100000:
        pytest.xfail("it doesn't work on pg < 10")
    cur = aconn.cursor()
    rv = loop.run_until_complete(
        cur.execute("select %s, %s, %s", [1, "foo", None])
    )
    assert rv is cur
    assert len(cur._results) == 1
    assert cur._result.get_value(0, 0) == b"1"
    assert cur._result.get_value(0, 1) == b"foo"
    assert cur._result.get_value(0, 2) is None
    assert cur.nextset() is None
