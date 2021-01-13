"""
Enum values for psycopg3

These values are defined by us and are not necessarily dependent on
libpq-defined enums.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from enum import Enum

from . import pq


class Format(str, Enum):
    """
    Enum representing the format wanted for a query argument.

    The value `AUTO` allows psycopg3 to choose the best value for a certain
    value.
    """

    __module__ = "psycopg3.adapt"

    AUTO = "s"
    """Automatically chosen (``%s`` placeholder)."""
    TEXT = "t"
    """Text parameter (``%t`` placeholder)."""
    BINARY = "b"
    """Binary parameter (``%b`` placeholder)."""

    @classmethod
    def from_pq(cls, fmt: pq.Format) -> "Format":
        return _pg2py[fmt]

    @classmethod
    def as_pq(cls, fmt: "Format") -> pq.Format:
        return _py2pg[fmt]


_py2pg = {
    Format.TEXT: pq.Format.TEXT,
    Format.BINARY: pq.Format.BINARY,
}

_pg2py = {
    pq.Format.TEXT: Format.TEXT,
    pq.Format.BINARY: Format.BINARY,
}
