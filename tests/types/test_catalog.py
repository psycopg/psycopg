from __future__ import annotations

from psycopg import postgres, pq
from psycopg.types.catalog import CidLoader, PgLsnLoader, XidLoader, register_catalog


def test_register_catalog_global(global_adapters):
    oid = postgres.types["cid"].oid

    assert postgres.adapters.get_loader(oid, pq.Format.TEXT) is not CidLoader

    register_catalog()

    assert postgres.adapters.get_loader(oid, pq.Format.TEXT) is CidLoader


def test_register_catalog_connection(conn):
    oid = postgres.types["pg_lsn"].oid

    assert conn.adapters.get_loader(oid, pq.Format.TEXT) is not PgLsnLoader

    register_catalog(conn)

    assert conn.adapters.get_loader(oid, pq.Format.TEXT) is PgLsnLoader


def test_register_catalog_cursor(conn):
    oid = postgres.types["xid"].oid
    cur = conn.cursor()

    register_catalog(cur)

    assert conn.adapters.get_loader(oid, pq.Format.TEXT) is not XidLoader
    assert cur.adapters.get_loader(oid, pq.Format.TEXT) is XidLoader
