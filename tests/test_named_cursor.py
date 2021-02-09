def test_description(conn):
    cur = conn.cursor("foo")
    assert cur.name == "foo"
    cur.execute("select generate_series(1, 10) as bar")
    assert len(cur.description) == 1
    assert cur.description[0].name == "bar"
    assert cur.description[0].type_code == cur.adapters.types["int4"].oid
    assert cur.pgresult.ntuples == 0
