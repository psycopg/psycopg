from __future__ import annotations

from typing import Any
from importlib import import_module

import pytest

from psycopg import postgres, pq

_has_catalog = True
try:
    _catalog = import_module("psycopg.types.catalog")
    CidLoader = _catalog.CidLoader
    PgLsnLoader = _catalog.PgLsnLoader
    XidLoader = _catalog.XidLoader
    register_catalog = _catalog.register_catalog
except (ImportError, AttributeError):
    _has_catalog = False
    CidLoader = PgLsnLoader = XidLoader = object

    def register_catalog(*args: Any, **kwargs: Any) -> None:
        raise NotImplementedError


pytestmark = pytest.mark.skipif(
    not _has_catalog, reason="catalog type adapters unavailable"
)


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
