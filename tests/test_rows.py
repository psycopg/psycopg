from psycopg3 import rows


def test_dict_row(conn):
    cur = conn.cursor(row_factory=rows.dict_row)
    cur.execute("select 'bob' as name, 3 as id")
    assert cur.fetchall() == [{"name": "bob", "id": 3}]

    cur.execute("select 'a' as letter; select 1 as number")
    assert cur.fetchall() == [{"letter": "a"}]
    assert cur.nextset()
    assert cur.fetchall() == [{"number": 1}]
    assert not cur.nextset()
