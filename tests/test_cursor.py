def test_execute_many(conn):
    cur = conn.cursor()
    rv = cur.execute("select 'foo'; select 'bar'")
    assert rv is cur
    assert len(cur._results) == 2
    assert cur._result.get_value(0, 0) == b"foo"
    assert cur.nextset()
    assert cur._result.get_value(0, 0) == b"bar"
    assert cur.nextset() is None


def test_execute_sequence(conn):
    cur = conn.cursor()
    rv = cur.execute("select %s, %s, %s", [1, "foo", None])
    assert rv is cur
    assert len(cur._results) == 1
    assert cur._result.get_value(0, 0) == b"1"
    assert cur._result.get_value(0, 1) == b"foo"
    assert cur._result.get_value(0, 2) is None
    assert cur.nextset() is None
