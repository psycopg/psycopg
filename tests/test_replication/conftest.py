"""
Fixtures for replication tests.
"""

from __future__ import annotations

import uuid

import pytest

import psycopg

pytest_plugins = (
    "tests.test_replication.fix_db_async",
    "tests.test_replication.fix_db",
)


def unique_name(prefix: str = "psycopg_test") -> str:
    """Generate a unique replication slot name."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def get_text_type(conn):
    if conn.pgconn._encoding == "ascii":
        return bytes
    else:
        return str


@pytest.fixture
def pub_name():
    """Return a unique publication name."""
    name = unique_name()
    yield name


@pytest.fixture
def slot_name(conn):
    """Return a unique replication slot name."""
    name = unique_name()
    yield name
    drop_slot_if_exists(conn, name)


def drop_slot_if_exists(conn, slot_name):
    """Drop a replication slot if it exists (best-effort cleanup)."""
    try:
        conn.execute(
            "SELECT pg_drop_replication_slot(slot_name)"
            + " FROM pg_replication_slots WHERE slot_name = %s",
            [slot_name],
        )
    except psycopg.errors.UndefinedObject:
        pass


@pytest.fixture
def test_table(conn):
    table_name = f"repl_test_{uuid.uuid4().hex[:8]}"
    conn.execute(
        f"CREATE TABLE {table_name} (id serial primary key, data text NOT NULL)"
    )
    yield table_name
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture()
def origin(conn):
    origin_name = "psycopg_test"

    conn.execute("SELECT pg_replication_origin_create(%s)", [origin_name])

    try:
        yield origin_name
    finally:
        conn.execute("SELECT pg_replication_origin_drop(%s)", [origin_name])


@pytest.fixture
def publication(conn, test_table, pub_name):
    conn.execute(f"CREATE PUBLICATION {pub_name} FOR TABLE {test_table}")
    yield pub_name
    conn.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")


@pytest.fixture
def empty_publication(conn):
    pub_name = "empty_publication"
    conn.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")
    conn.execute(f"CREATE PUBLICATION {pub_name}")
    yield pub_name
    conn.execute(f"DROP PUBLICATION IF EXISTS {pub_name}")


@pytest.fixture
def smallpoint_type(conn):
    conn.execute("CREATE TYPE smallpoint AS (x smallint, y smallint)")
    try:
        yield "smallpoint"
    finally:
        conn.execute("DROP TYPE smallpoint CASCADE")


@pytest.fixture
def intpoint_type(conn):
    conn.execute("CREATE TYPE intpoint AS (x int, y int)")
    try:
        yield "intpoint"
    finally:
        conn.execute("DROP TYPE intpoint CASCADE")
