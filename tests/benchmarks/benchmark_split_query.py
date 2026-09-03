"""Benchmarks for query parsing and conversion functions."""

import pytest

from psycopg._queries import _split_query

_report_cols = [f"col_{i}" for i in range(20)]

queries = [
    pytest.param(
        b"SELECT id, name, email FROM users WHERE org_id = %s AND active = %s",
        id="simple-positional",
    ),
    pytest.param(
        b"INSERT INTO events (user_id, type, payload, created_at) "
        b"VALUES (%s, %s, %s, %s)",
        id="insert",
    ),
    pytest.param(
        b"SELECT * FROM orders WHERE customer_id = %(customer_id)s"
        b" AND status = %(status)s AND created_at > %(since)s",
        id="named",
    ),
    pytest.param(
        b"SELECT * FROM t WHERE a = %(x)s AND b = %(y)s AND c = %(x)s AND d = %(y)s",
        id="named-repeat",
    ),
    pytest.param(
        b"SELECT * FROM products WHERE name LIKE '%%' || %s || '%%' AND category = %s",
        id="like-escapes",
    ),
    pytest.param(
        b"INSERT INTO items (a, b, c, d, e) VALUES "
        + b", ".join(b"(%s, %s, %s, %s, %s)" for _ in range(20)),
        id="bulk-insert-100params",
    ),
    pytest.param(
        b"SELECT * FROM t WHERE id %% 10 = 0 AND status %% 3 = 1",
        id="modulo-no-params",
    ),
    pytest.param(
        (
            f"SELECT {', '.join(_report_cols)} FROM report WHERE "
            + " AND ".join(f"{c} = %({c})s" for c in _report_cols)
        ).encode(),
        id="large-named-20params",
    ),
]


@pytest.mark.parametrize("query", queries)
def test_split_query(benchmark, query):
    benchmark(_split_query, query, "utf-8")


@pytest.mark.parametrize("query", queries)
def test_split_query_no_collapse(benchmark, query):
    benchmark(_split_query, query, "utf-8", collapse_double_percent=False)
