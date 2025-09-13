import pytest

import psycopg._cmodule


@pytest.mark.parametrize("pagesize", [1, 2, 3, 5, 7])
@pytest.mark.skipif(not psycopg._cmodule._psycopg, reason="C module test only")
def test_pagesize_c(conn, pagesize):
    cur = conn.execute("SELECT *, 'abc' FROM generate_series(1, 10)")
    cur._tx._load_rows_page_size = pagesize
    result = cur.fetchall()
    expected = [(i, "abc") for i in range(1, 11)]
    assert result == expected
