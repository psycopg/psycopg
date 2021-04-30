"""
psycopg3 row factories
"""

# Copyright (C) 2021 The Psycopg Team

import functools
import re
from collections import namedtuple
from typing import Any, Callable, Dict, NamedTuple, Sequence, Tuple, Type
from typing import TypeVar, TYPE_CHECKING
from typing_extensions import Protocol

from . import errors as e

if TYPE_CHECKING:
    from .cursor import AnyCursor

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
    Callable protocol taking a `~psycopg3.Cursor` and returning a `RowMaker`.

    A `!RowFactory` is typically called when a `!Cursor` receives a result.
    This way it can inspect the cursor state (for instance the
    `~psycopg3.Cursor.description` attribute) and help a `!RowMaker` to create
    a complete object.

    For instance the `dict_row()` `!RowFactory` uses the names of the column to
    define the dictionary key and returns a `!RowMaker` function which would
    use the values to create a dictionary for each record.
    """

    def __call__(self, __cursor: "AnyCursor[Row]") -> RowMaker[Row]:
        ...


TupleRow = Tuple[Any, ...]
"""
An alias for the type returned by `tuple_row()` (i.e. a tuple of any content).
"""


def tuple_row(
    cursor: "AnyCursor[TupleRow]",
) -> Callable[[Sequence[Any]], TupleRow]:
    r"""Row factory to represent rows as simple tuples.

    This is the default factory.

    :param cursor: The cursor where the rows are read.
    :rtype: `RowMaker`\ [`TupleRow`]
    """
    # Implementation detail: make sure this is the tuple type itself, not an
    # equivalent function, because the C code fast-paths on it.
    return tuple


DictRow = Dict[str, Any]
"""
An alias for the type returned by `dict_row()`

A `!DictRow` is a dictionary with keys as string and any value returned by the
database.
"""


def dict_row(
    cursor: "AnyCursor[DictRow]",
) -> Callable[[Sequence[Any]], DictRow]:
    r"""Row factory to represent rows as dicts.

    Note that this is not compatible with the DBAPI, which expects the records
    to be sequences.

    :param cursor: The cursor where the rows are read.
    :rtype: `RowMaker`\ [`DictRow`]
    """

    def make_row(values: Sequence[Any]) -> Dict[str, Any]:
        desc = cursor.description
        if desc is None:
            raise e.InterfaceError("The cursor doesn't have a result")
        titles = (c.name for c in desc)
        return dict(zip(titles, values))

    return make_row


def namedtuple_row(
    cursor: "AnyCursor[NamedTuple]",
) -> Callable[[Sequence[Any]], NamedTuple]:
    r"""Row factory to represent rows as `~collections.namedtuple`.

    :param cursor: The cursor where the rows are read.
    :rtype: `RowMaker`\ [`NamedTuple`]
    """

    def make_row(values: Sequence[Any]) -> NamedTuple:
        desc = cursor.description
        if desc is None:
            raise e.InterfaceError("The cursor doesn't have a result")
        key = tuple(c.name for c in desc)
        nt = _make_nt(key)
        rv = nt._make(values)
        return rv

    return make_row


# ascii except alnum and underscore
_re_clean = re.compile(
    "[" + re.escape(" !\"#$%&'()*+,-./:;<=>?@[\\]^`{|}~") + "]"
)


@functools.lru_cache(512)
def _make_nt(key: Sequence[str]) -> Type[NamedTuple]:
    fields = []
    for s in key:
        s = _re_clean.sub("_", s)
        # Python identifier cannot start with numbers, namedtuple fields
        # cannot start with underscore. So...
        if s[0] == "_" or "0" <= s[0] <= "9":
            s = "f" + s
        fields.append(s)
    return namedtuple("Row", fields)  # type: ignore[return-value]
