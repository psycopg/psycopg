import codecs
import pytest

import psycopg3
from psycopg3.utils.queries import split_query, query2pg, reorder_params


@pytest.mark.parametrize(
    "input, want",
    [
        (b"", [(b"", 0, 0)]),
        (b"foo bar", [(b"foo bar", 0, 0)]),
        (b"foo %% bar", [(b"foo % bar", 0, 0)]),
        (b"%s", [(b"", 0, 0), (b"", 0, 0)]),
        (b"%s foo", [(b"", 0, 0), (b" foo", 0, 0)]),
        (b"%b foo", [(b"", 0, 1), (b" foo", 0, 0)]),
        (b"foo %s", [(b"foo ", 0, 0), (b"", 0, 0)]),
        (b"foo %%%s bar", [(b"foo %", 0, 0), (b" bar", 0, 0)]),
        (b"foo %(name)s bar", [(b"foo ", "name", 0), (b" bar", 0, 0)]),
        (
            b"foo %(name)s %(name)b bar",
            [(b"foo ", "name", 0), (b" ", "name", 1), (b" bar", 0, 0)],
        ),
        (
            b"foo %s%b bar %s baz",
            [(b"foo ", 0, 0), (b"", 1, 1), (b" bar ", 2, 0), (b" baz", 0, 0)],
        ),
    ],
)
def test_split_query(input, want):
    assert split_query(input) == want


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
        split_query(input)


@pytest.mark.parametrize(
    "query, params, want, wformats",
    [
        (b"", [], b"", []),
        (b"%%", [], b"%", []),
        (b"select %s", (1,), b"select $1", [False]),
        (b"%s %% %s", (1, 2), b"$1 % $2", [False, False]),
        (b"%b %% %s", (1, 2), b"$1 % $2", [True, False]),
    ],
)
def test_query2pg_seq(query, params, want, wformats):
    out, formats, order = query2pg(query, params, codecs.lookup("utf-8"))
    assert order is None
    assert out == want
    assert formats == wformats


@pytest.mark.parametrize(
    "query, params, want, wformats, worder",
    [
        (b"", {}, b"", [], []),
        (b"hello %%", {"a": 1}, b"hello %", [], []),
        (
            b"select %(hello)s",
            {"hello": 1, "world": 2},
            b"select $1",
            [False],
            ["hello"],
        ),
        (
            b"select %(hi)s %(there)b %(hi)s",
            {"hi": 1, "there": 2},
            b"select $1 $2 $1",
            [False, True],
            ["hi", "there"],
        ),
    ],
)
def test_query2pg_map(query, params, want, wformats, worder):
    out, formats, order = query2pg(query, params, codecs.lookup("utf-8"))
    assert out == want
    assert order == worder


@pytest.mark.parametrize(
    "query, params",
    [
        (b"select %s", {"a": 1}),
        (b"select %(name)s", [1]),
        (b"select %s", "a"),
        (b"select %s", 1),
        (b"select %s", b"a"),
        (b"select %s", set()),
        ("select", []),
        ("select", []),
    ],
)
def test_query2pg_badtype(query, params):
    with pytest.raises(TypeError):
        query2pg(query, params, codecs.lookup("utf-8"))


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
def test_query2pg_badprog(query, params):
    with pytest.raises(psycopg3.ProgrammingError):
        query2pg(query, params, codecs.lookup("utf-8"))


@pytest.mark.parametrize(
    "params, order, want",
    [
        ({"foo": 1, "bar": 2}, [], []),
        ({"foo": 1, "bar": 2}, ["foo"], [1]),
        ({"foo": 1, "bar": 2}, ["bar", "foo"], [2, 1]),
    ],
)
def test_reorder_params(params, order, want):
    rv = reorder_params(params, order)
    assert rv == want
