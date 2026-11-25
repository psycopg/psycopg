"""
Wrappers for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

# Wrappers to force numbers to be cast as specific PostgreSQL types

from ._compat import Self

# These types are implemented here but exposed by `psycopg.types.numeric`.
# They are defined here to avoid a circular import.
_MODULE = "psycopg.types.numeric"


class _IntWrapper(int):
    __slots__ = ()

    def __new__(cls, arg: int) -> Self:
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class _FloatWrapper(float):
    __slots__ = ()

    def __new__(cls, arg: float) -> Self:
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class Int2(_IntWrapper):
    """
    Force dumping a Python `!int` as a PostgreSQL :sql:`smallint/int2`.
    """

    __module__ = _MODULE
    __slots__ = ()


class Int4(_IntWrapper):
    """
    Force dumping a Python `!int` as a PostgreSQL :sql:`integer/int4`.
    """

    __module__ = _MODULE
    __slots__ = ()


class Int8(_IntWrapper):
    """
    Force dumping a Python `!int` as a PostgreSQL :sql:`bigint/int8`.
    """

    __module__ = _MODULE
    __slots__ = ()


class IntNumeric(_IntWrapper):
    """
    Force dumping a Python `!int` as a PostgreSQL :sql:`numeric/decimal`.
    """

    __module__ = _MODULE
    __slots__ = ()


class Float4(_FloatWrapper):
    """
    Force dumping a Python `!float` as a PostgreSQL :sql:`float4/real`.
    """

    __module__ = _MODULE
    __slots__ = ()


class Float8(_FloatWrapper):
    """
    Force dumping a Python `!float` as a PostgreSQL :sql:`float8/double precision`.
    """

    __module__ = _MODULE
    __slots__ = ()


class Oid(_IntWrapper):
    """
    Force dumping a Python `!int` as a PostgreSQL :sql:`oid`.
    """

    __module__ = _MODULE
    __slots__ = ()
