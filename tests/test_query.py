import pytest

import psycopg3
from psycopg3 import pq
from psycopg3.adapt import Transformer, Format
from psycopg3._queries import PostgresQuery, _split_query


@pytest.mark.parametrize(
    "input, want",
    [
        (b"", [(b"", 0, Format.AUTO)]),
        (b"foo bar", [(b"foo bar", 0, Format.AUTO)]),
        (b"foo %% bar", [(b"foo % bar", 0, Format.AUTO)]),
        (b"%s", [(b"", 0, Format.AUTO), (b"", 0, Format.AUTO)]),
        (b"%s foo", [(b"", 0, Format.AUTO), (b" foo", 0, Format.AUTO)]),
        (b"%b foo", [(b"", 0, Format.BINARY), (b" foo", 0, Format.AUTO)]),
        (b"foo %s", [(b"foo ", 0, Format.AUTO), (b"", 0, Format.AUTO)]),
        (
            b"foo %%%s bar",
            [(b"foo %", 0, Format.AUTO), (b" bar", 0, Format.AUTO)],
        ),
        (
            b"foo %(name)s bar",
            [(b"foo ", "name", Format.AUTO), (b" bar", 0, Format.AUTO)],
        ),
        (
            b"foo %(name)s %(name)b bar",
            [
                (b"foo ", "name", Format.AUTO),
                (b" ", "name", Format.BINARY),
                (b" bar", 0, Format.AUTO),
            ],
        ),
        (
            b"foo %s%b bar %s baz",
            [
                (b"foo ", 0, Format.AUTO),
                (b"", 1, Format.BINARY),
                (b" bar ", 2, Format.AUTO),
                (b" baz", 0, Format.AUTO),
            ],
        ),
    ],
)
def test_split_query(input, want):
    assert _split_query(input) == want


@pytest.mark.parametrize(
    "input",
    [
        b"foo %d bar",
        b"foo % bar",
        b"foo %%% bar",
        b"foo %(foo)d bar",
        b"foo %(foo)s bar %s baz",
        b"foo %(foo) bar",
        b"foo %(foo bar",
        b"3%2",
    ],
)
def test_split_query_bad(input):
    with pytest.raises(psycopg3.ProgrammingError):
        _split_query(input)


@pytest.mark.parametrize(
    "query, params, want, wformats, wparams",
    [
        (b"", None, b"", None, None),
        (b"", [], b"", [], []),
        (b"%%", [], b"%", [], []),
        (b"select %t", (1,), b"select $1", [pq.Format.TEXT], [b"1"]),
        (
            b"%t %% %t",
            (1, 2),
            b"$1 % $2",
            [pq.Format.TEXT, pq.Format.TEXT],
            [b"1", b"2"],
        ),
        (
            b"%t %% %t",
            ("a", 2),
            b"$1 % $2",
            [pq.Format.TEXT, pq.Format.TEXT],
            [b"a", b"2"],
        ),
    ],
)
def test_pg_query_seq(query, params, want, wformats, wparams):
    pq = PostgresQuery(Transformer())
    pq.convert(query, params)
    assert pq.query == want
    assert pq.formats == wformats
    assert pq.params == wparams


@pytest.mark.parametrize(
    "query, params, want, wformats, wparams",
    [
        (b"", {}, b"", [], []),
        (b"hello %%", {"a": 1}, b"hello %", [], []),
        (
            b"select %(hello)t",
            {"hello": 1, "world": 2},
            b"select $1",
            [pq.Format.TEXT],
            [b"1"],
        ),
        (
            b"select %(hi)s %(there)s %(hi)s",
            {"hi": 0, "there": "a"},
            b"select $1 $2 $1",
            [pq.Format.BINARY, pq.Format.TEXT],
            [b"\x00" * 2, b"a"],
        ),
    ],
)
def test_pg_query_map(query, params, want, wformats, wparams):
    pq = PostgresQuery(Transformer())
    pq.convert(query, params)
    assert pq.query == want
    assert pq.formats == wformats
    assert pq.params == wparams


@pytest.mark.parametrize(
    "query, params",
    [
        (b"select %s", {"a": 1}),
        (b"select %(name)s", [1]),
        (b"select %s", "a"),
        (b"select %s", 1),
        (b"select %s", b"a"),
        (b"select %s", set()),
    ],
)
def test_pq_query_badtype(query, params):
    pq = PostgresQuery(Transformer())
    with pytest.raises(TypeError):
        pq.convert(query, params)


@pytest.mark.parametrize(
    "query, params",
    [
        (b"", [1]),
        (b"%s", []),
        (b"%%", [1]),
        (b"$1", [1]),
        (b"select %(", {"a": 1}),
        (b"select %(a", {"a": 1}),
        (b"select %(a)", {"a": 1}),
        (b"select %s %(hi)s", [1]),
        (b"select %(hi)s %(hi)b", {"hi": 1}),
    ],
)
def test_pq_query_badprog(query, params):
    pq = PostgresQuery(Transformer())
    with pytest.raises(psycopg3.ProgrammingError):
        pq.convert(query, params)
