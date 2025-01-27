import psycopg
from psycopg import pq
from psycopg import _oids


def test_uuid_loader(benchmark, conn):
    val = b"12345678-1234-5678-1234-567812345678"
    tx = psycopg.adapt.Transformer(conn)

    @benchmark
    def bench():
        loader = tx.get_loader(_oids.UUID_OID, pq.Format.TEXT)
        loader.load(val)


def test_uuid_binary_loader(benchmark, conn):
    val = b"\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78\x12\x34\x56\x78"
    tx = psycopg.adapt.Transformer(conn)

    @benchmark
    def bench():
        loader = tx.get_loader(_oids.UUID_OID, pq.Format.BINARY)
        loader.load(val)
