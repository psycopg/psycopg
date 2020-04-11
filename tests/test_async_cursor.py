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


def test_status(aconn, loop):
    cur = aconn.cursor()
    assert cur.status is None
    loop.run_until_complete(cur.execute("reset all"))
    assert cur.status == cur.ExecStatus.COMMAND_OK
    loop.run_until_complete(cur.execute("select 1"))
    assert cur.status == cur.ExecStatus.TUPLES_OK
    cur.close()
    assert cur.status is None


def test_execute_many_results(aconn, loop):
    cur = aconn.cursor()
    assert cur.nextset() is None

    rv = loop.run_until_complete(cur.execute("select 'foo'; select 'bar'"))
    assert rv is cur
    assert len(cur._results) == 2
    assert cur.pgresult.get_value(0, 0) == b"foo"
    assert cur.nextset()
    assert cur.pgresult.get_value(0, 0) == b"bar"
    assert cur.nextset() is None

    cur.close()
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


@pytest.mark.parametrize("query", ["", " ", ";"])
def test_execute_empty_query(aconn, loop, query):
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute(query))
    assert cur.status == cur.ExecStatus.EMPTY_QUERY
    with pytest.raises(psycopg3.ProgrammingError):
        loop.run_until_complete(cur.fetchone())


def test_fetchone(aconn, loop):
    cur = aconn.cursor()
    loop.run_until_complete(cur.execute("select %s, %s, %s", [1, "foo", None]))
    assert cur.pgresult.fformat(0) == 0

    row = loop.run_until_complete(cur.fetchone())
    assert row[0] == 1
    assert row[1] == "foo"
    assert row[2] is None
    row = loop.run_until_complete(cur.fetchone())
    assert row is None
