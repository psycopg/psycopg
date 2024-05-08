"""
PostgreSQL type modifiers.

The type modifiers parse catalog information to obtain the type modifier
of a column - the numeric part of varchar(10) or decimal(6,2).
"""

# Copyright (C) 2024 The Psycopg Team

from __future__ import annotations


class TypeModifier:
    """Type modifier that doesn't know any modifier.

    Useful to describe types with no type modifier.
    """

    def __init__(self, oid: int):
        self.oid = oid

    def get_modifier(self, typemod: int) -> tuple[int, ...] | None:
        return None

    def get_display_size(self, typemod: int) -> int | None:
        return None

    def get_precision(self, typemod: int) -> int | None:
        return None

    def get_scale(self, typemod: int) -> int | None:
        return None


class NumericTypeModifier(TypeModifier):
    """Handle numeric type modifier."""

    def get_modifier(self, typemod: int) -> tuple[int, ...] | None:
        rv = []
        precision = self.get_precision(typemod)
        if precision is not None:
            rv.append(precision)
        scale = self.get_scale(typemod)
        if scale is not None:
            rv.append(scale)
        return tuple(rv) if rv else None

    def get_precision(self, typemod: int) -> int | None:
        return typemod >> 16 if typemod >= 0 else None

    def get_scale(self, typemod: int) -> int | None:
        typemod -= 4
        return typemod & 0xFFFF if typemod >= 0 else None


class CharTypeModifier(TypeModifier):
    """Handle char/varchar type modifier."""

    def get_display_size(self, typemod: int) -> int | None:
        return typemod - 4 if typemod >= 0 else None


class TimeTypeModifier(TypeModifier):
    """Handle time-related types modifier."""

    def get_precision(self, typemod: int) -> int | None:
        return typemod & 0xFFFF if typemod >= 0 else None
