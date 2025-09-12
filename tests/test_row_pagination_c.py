import pytest

@pytest.mark.parametrize('pagesize', [1, 2, 3, 5, 7])
def test_pagesize_c(conn, pagesize):
    from psycopg._cmodule import _psycopg
    if not _psycopg:
        return
    cur = conn.cursor()
    cur.execute("SELECT *, 'abc' FROM generate_series(1, 10)")
    cur._tx._page_size = pagesize
    result = cur.fetchall()
    expected = [
        (1, 'abc'), (2, 'abc'), (3, 'abc'), (4, 'abc'), (5, 'abc'),
        (6, 'abc'), (7, 'abc'), (8, 'abc'), (9, 'abc'), (10, 'abc')
    ]
    assert result == expected
