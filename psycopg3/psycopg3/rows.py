"""
psycopg3 row factories
"""

# Copyright (C) 2021 The Psycopg Team

from typing import Any, Callable, Dict, Sequence

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
