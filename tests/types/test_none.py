from psycopg import sql
from psycopg.adapt import Transformer, PyFormat


def test_quote_none(conn):

    tx = Transformer()
    assert tx.get_dumper(None, PyFormat.TEXT).quote(None) == b"NULL"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}").format(v=sql.Literal(None)))
    assert cur.fetchone()[0] is None
