import codecs
import pytest

import psycopg3
from psycopg3.utils.queries import split_query, query2pg, reorder_params


@pytest.mark.parametrize(
    "input, want",
    [
        (b"", [[b"", None, None]]),
        (b"foo bar", [[b"foo bar", None, None]]),
        (b"foo %% bar", [[b"foo % bar", None, None]]),
        (b"%s", [[b"", 0, False], [b"", None, None]]),
        (b"%s foo", [[b"", 0, False], [b" foo", None, None]]),
        (b"%b foo", [[b"", 0, True], [b" foo", None, None]]),
        (b"foo %s", [[b"foo ", 0, False], [b"", None, None]]),
        (b"foo %%%s bar", [[b"foo %", 0, False], [b" bar", None, None]]),
        (
            b"foo %(name)s bar",
            [[b"foo ", b"name", False], [b" bar", None, None]],
        ),
        (
            b"foo %(name)s %(name)b bar",
            [
                [b"foo ", b"name", False],
                [b" ", b"name", True],
                [b" bar", None, None],
            ],
        ),
        (
            b"foo %s%b bar %s baz",
            [
                [b"foo ", 0, False],
                [b"", 1, True],
                [b" bar ", 2, False],
                [b" baz", None, None],
            ],
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
    "query, params, want",
    [
        (b"", [], b""),
        (b"%%", [], b"%"),
        (b"select %s", (1,), b"select $1"),
        (b"%s %% %s", (1, 2), b"$1 % $2"),
    ],
)
def test_query2pg_seq(query, params, want):
    out, order = query2pg(query, params, codecs.lookup("utf-8"))
    assert order is None
    assert out == want


@pytest.mark.parametrize(
    "query, params, want, worder",
    [
        (b"", {}, b"", []),
        (b"hello %%", {"a": 1}, b"hello %", []),
        (
            b"select %(hello)s",
            {"hello": 1, "world": 2},
            b"select $1",
            ["hello"],
        ),
        (
            b"select %(hi)s %(there)s %(hi)s",
            {"hi": 1, "there": 2},
            b"select $1 $2 $1",
            ["hi", "there"],
        ),
    ],
)
def test_query2pg_map(query, params, want, worder):
    out, order = query2pg(query, params, codecs.lookup("utf-8"))
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
        (b"select %s %(hi)s", 1),
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
