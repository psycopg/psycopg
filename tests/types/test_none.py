from psycopg3 import sql
from psycopg3.adapt import Transformer, Format


def test_quote_none(conn):

    tx = Transformer()
    assert tx.get_dumper(None, Format.TEXT).quote(None) == b"NULL"

    cur = conn.cursor()
    cur.execute(sql.SQL("select {v}").format(v=sql.Literal(None)))
    assert cur.fetchone()[0] is None
