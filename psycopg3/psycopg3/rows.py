"""
psycopg3 row factories
"""

# Copyright (C) 2021 The Psycopg Team

import functools
import re
from collections import namedtuple
from typing import Any, Callable, Dict, NamedTuple, Sequence, Tuple, Type
from typing import TYPE_CHECKING

from . import errors as e

if TYPE_CHECKING:
    from .cursor import BaseCursor


def tuple_row(
    cursor: "BaseCursor[Any]",
) -> Callable[[Sequence[Any]], Tuple[Any, ...]]:
    """Row factory to represent rows as simple tuples.

    This is the default factory.
    """
    # Implementation detail: make sure this is the tuple type itself, not an
    # equivalent function, because the C code fast-paths on it.
    return tuple


def dict_row(
    cursor: "BaseCursor[Any]",
) -> Callable[[Sequence[Any]], Dict[str, Any]]:
    """Row factory to represent rows as dicts.

    Note that this is not compatible with the DBAPI, which expects the records
    to be sequences.
    """

    def make_row(values: Sequence[Any]) -> Dict[str, Any]:
        desc = cursor.description
        if desc is None:
            raise e.InterfaceError("The cursor doesn't have a result")
        titles = (c.name for c in desc)
        return dict(zip(titles, values))

    return make_row


def namedtuple_row(
    cursor: "BaseCursor[Any]",
) -> Callable[[Sequence[Any]], NamedTuple]:
    """Row factory to represent rows as `~collections.namedtuple`."""

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
