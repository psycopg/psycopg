def test_description(conn):
    cur = conn.cursor("foo")
    assert cur.name == "foo"
    cur.execute("select generate_series(1, 10) as bar")
    assert len(cur.description) == 1
    assert cur.description[0].name == "bar"
    assert cur.description[0].type_code == cur.adapters.types["int4"].oid
    assert cur.pgresult.ntuples == 0


def test_close(conn, recwarn):
    cur = conn.cursor("foo")
    cur.execute("select generate_series(1, 10) as bar")
    cur.close()
    assert cur.closed

    assert not conn.execute(
        "select * from pg_cursors where name = 'foo'"
    ).fetchone()
    del cur
    assert not recwarn


def test_context(conn, recwarn):
    with conn.cursor("foo") as cur:
        cur.execute("select generate_series(1, 10) as bar")

    assert cur.closed
    assert not conn.execute(
        "select * from pg_cursors where name = 'foo'"
    ).fetchone()
    del cur
    assert not recwarn


def test_warn_close(conn, recwarn):
    cur = conn.cursor("foo")
    cur.execute("select generate_series(1, 10) as bar")
    del cur
    assert ".close()" in str(recwarn.pop(ResourceWarning).message)
