"""
psycopg3 row factories
"""

# Copyright (C) 2021 The Psycopg Team

import functools
import re
from collections import namedtuple
from typing import Any, Callable, Dict, Sequence, Tuple, Type

from .cursor import BaseCursor
from .proto import ConnectionType


def dict_row(
    cursor: BaseCursor[ConnectionType],
) -> Callable[[Sequence[Any]], Dict[str, Any]]:
    """Row factory to represent rows as dicts."""

    def make_row(values: Sequence[Any]) -> Dict[str, Any]:
        assert cursor.description
        titles = (c.name for c in cursor.description)
        return dict(zip(titles, values))

    return make_row


def namedtuple_row(
    cursor: BaseCursor[ConnectionType],
) -> Callable[[Sequence[Any]], Tuple[Any, ...]]:
    """Row factory to represent rows as `~collections.namedtuple`."""

    def make_row(values: Sequence[Any]) -> Tuple[Any, ...]:
        assert cursor.description
        key = tuple(c.name for c in cursor.description)
        nt = _make_nt(key)
        rv = nt._make(values)  # type: ignore[attr-defined]
        return rv  # type: ignore[no-any-return]

    return make_row


# ascii except alnum and underscore
_re_clean = re.compile(
    "[" + re.escape(" !\"#$%&'()*+,-./:;<=>?@[\\]^`{|}~") + "]"
)


@functools.lru_cache(512)
def _make_nt(key: Sequence[str]) -> Type[Tuple[Any, ...]]:
    fields = []
    for s in key:
        s = _re_clean.sub("_", s)
        # Python identifier cannot start with numbers, namedtuple fields
        # cannot start with underscore. So...
        if s[0] == "_" or "0" <= s[0] <= "9":
            s = "f" + s
        fields.append(s)
    return namedtuple("Row", fields)
