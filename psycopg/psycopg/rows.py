"""
psycopg row factories
"""

# Copyright (C) 2021 The Psycopg Team

import re
import functools
from typing import Any, Callable, Dict, NamedTuple, NoReturn, Sequence, Tuple
from typing import TYPE_CHECKING, Type, TypeVar
from collections import namedtuple

from . import errors as e
from ._compat import Protocol

if TYPE_CHECKING:
    from .cursor import BaseCursor, Cursor
    from .cursor_async import AsyncCursor

T = TypeVar("T")

# Row factories

Row = TypeVar("Row")
Row_co = TypeVar("Row_co", covariant=True)


class RowMaker(Protocol[Row_co]):
    """
    Callable protocol taking a sequence of value and returning an object.

    The sequence of value is what is returned from a database query, already
    adapted to the right Python types. The return value is the object that your
    program would like to receive: by default (`tuple_row()`) it is a simple
    tuple, but it may be any type of object.

    Typically, `!RowMaker` functions are returned by `RowFactory`.
    """

    def __call__(self, __values: Sequence[Any]) -> Row_co:
        ...


class RowFactory(Protocol[Row]):
    """
    Callable protocol taking a `~psycopg.Cursor` and returning a `RowMaker`.

    A `!RowFactory` is typically called when a `!Cursor` receives a result.
    This way it can inspect the cursor state (for instance the
    `~psycopg.Cursor.description` attribute) and help a `!RowMaker` to create
    a complete object.

    For instance the `dict_row()` `!RowFactory` uses the names of the column to
    define the dictionary key and returns a `!RowMaker` function which would
    use the values to create a dictionary for each record.
    """

    def __call__(self, __cursor: "Cursor[Row]") -> RowMaker[Row]:
        ...


class AsyncRowFactory(Protocol[Row]):
    """
    Like `RowFactory`, taking an async cursor as argument.
    """

    def __call__(self, __cursor: "AsyncCursor[Row]") -> RowMaker[Row]:
        ...


class BaseRowFactory(Protocol[Row]):
    """
    Like `RowFactory`, taking either type of cursor as argument.
    """

    def __call__(self, __cursor: "BaseCursor[Any, Row]") -> RowMaker[Row]:
        ...


TupleRow = Tuple[Any, ...]
"""
An alias for the type returned by `tuple_row()` (i.e. a tuple of any content).
"""


DictRow = Dict[str, Any]
"""
An alias for the type returned by `dict_row()`

A `!DictRow` is a dictionary with keys as string and any value returned by the
database.
"""


def tuple_row(cursor: "BaseCursor[Any, TupleRow]") -> "RowMaker[TupleRow]":
    r"""Row factory to represent rows as simple tuples.

    This is the default factory, used when `~psycopg.Connection.connect()` or
    `~psycopg.Connection.cursor()` are called withouth a `!row_factory`
    parameter.

    """
    # Implementation detail: make sure this is the tuple type itself, not an
    # equivalent function, because the C code fast-paths on it.
    return tuple


def dict_row(cursor: "BaseCursor[Any, DictRow]") -> "RowMaker[DictRow]":
    """Row factory to represent rows as dictionaries.

    The dictionary keys are taken from the column names of the returned columns.
    """
    desc = cursor.description
    if desc is None:
        return no_result

    titles = [c.name for c in desc]

    def dict_row_(values: Sequence[Any]) -> Dict[str, Any]:
        return dict(zip(titles, values))

    return dict_row_


def namedtuple_row(
    cursor: "BaseCursor[Any, NamedTuple]",
) -> "RowMaker[NamedTuple]":
    """Row factory to represent rows as `~collections.namedtuple`.

    The field names are taken from the column names of the returned columns,
    with some mangling to deal with invalid names.
    """
    desc = cursor.description
    if desc is None:
        return no_result

    nt = _make_nt(*(c.name for c in desc))
    return nt._make


# ascii except alnum and underscore
_re_clean = re.compile(
    "[" + re.escape(" !\"#$%&'()*+,-./:;<=>?@[\\]^`{|}~") + "]"
)


@functools.lru_cache(512)
def _make_nt(*key: str) -> Type[NamedTuple]:
    fields = []
    for s in key:
        s = _re_clean.sub("_", s)
        # Python identifier cannot start with numbers, namedtuple fields
        # cannot start with underscore. So...
        if s[0] == "_" or "0" <= s[0] <= "9":
            s = "f" + s
        fields.append(s)
    return namedtuple("Row", fields)  # type: ignore[return-value]


def class_row(cls: Type[T]) -> BaseRowFactory[T]:
    r"""Generate a row factory to represent rows as instances of the class *cls*.

    The class must support every output column name as a keyword parameter.

    :param cls: The class to return for each row. It must support the fields
        returned by the query as keyword arguments.
    :rtype: `!Callable[[Cursor],` `RowMaker`\[~T]]
    """

    def class_row_(cur: "BaseCursor[Any, T]") -> "RowMaker[T]":
        desc = cur.description
        if desc is None:
            return no_result

        names = [d.name for d in desc]

        def class_row__(values: Sequence[Any]) -> T:
            return cls(**dict(zip(names, values)))  # type: ignore

        return class_row__

    return class_row_


def args_row(func: Callable[..., T]) -> BaseRowFactory[T]:
    """Generate a row factory calling *func* with positional parameters for every row.

    :param func: The function to call for each row. It must support the fields
        returned by the query as positional arguments.
    """

    def args_row_(cur: "BaseCursor[Any, T]") -> "RowMaker[T]":
        def args_row__(values: Sequence[Any]) -> T:
            return func(*values)

        return args_row__

    return args_row_


def kwargs_row(func: Callable[..., T]) -> BaseRowFactory[T]:
    """Generate a row factory calling *func* with keyword parameters for every row.

    :param func: The function to call for each row. It must support the fields
        returned by the query as keyword arguments.
    """

    def kwargs_row_(cur: "BaseCursor[Any, T]") -> "RowMaker[T]":
        desc = cur.description
        if desc is None:
            return no_result

        names = [d.name for d in desc]

        def kwargs_row__(values: Sequence[Any]) -> T:
            return func(**dict(zip(names, values)))

        return kwargs_row__

    return kwargs_row_


def no_result(values: Sequence[Any]) -> NoReturn:
    """A `RowMaker` that always fail.

    It can be used as return value for a `RowFactory` called with no result.
    Note that the `!RowFactory` *will* be called with no result, but the
    resulting `!RowMaker` never should.
    """
    raise e.InterfaceError("the cursor doesn't have a result")
