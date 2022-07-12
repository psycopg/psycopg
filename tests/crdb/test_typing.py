import pytest

from ..test_typing import _test_reveal


@pytest.mark.parametrize(
    "conn, type",
    [
        (
            "psycopg.crdb.connect()",
            "psycopg.crdb.CrdbConnection[Tuple[Any, ...]]",
        ),
        (
            "psycopg.crdb.connect(row_factory=rows.dict_row)",
            "psycopg.crdb.CrdbConnection[Dict[str, Any]]",
        ),
        (
            "psycopg.crdb.CrdbConnection.connect()",
            "psycopg.crdb.CrdbConnection[Tuple[Any, ...]]",
        ),
        (
            "psycopg.crdb.CrdbConnection.connect(row_factory=rows.tuple_row)",
            "psycopg.crdb.CrdbConnection[Tuple[Any, ...]]",
        ),
        (
            "psycopg.crdb.CrdbConnection.connect(row_factory=rows.dict_row)",
            "psycopg.crdb.CrdbConnection[Dict[str, Any]]",
        ),
        (
            "await psycopg.crdb.AsyncCrdbConnection.connect()",
            "psycopg.crdb.AsyncCrdbConnection[Tuple[Any, ...]]",
        ),
        (
            "await psycopg.crdb.AsyncCrdbConnection.connect(row_factory=rows.dict_row)",
            "psycopg.crdb.AsyncCrdbConnection[Dict[str, Any]]",
        ),
    ],
)
def test_connection_type(conn, type, mypy):
    stmts = f"obj = {conn}"
    _test_reveal_crdb(stmts, type, mypy)


def _test_reveal_crdb(stmts, type, mypy):
    stmts = f"""\
import psycopg.crdb
{stmts}
"""
    _test_reveal(stmts, type, mypy)
