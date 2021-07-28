"""
Wrappers for numeric types.
"""

# Copyright (C) 2020-2021 The Psycopg Team

# Wrappers to force numbers to be cast as specific PostgreSQL types

# These types are implemented here but exposed by `psycopg.types.numeric`.
# They are defined here to avoid a circular import.
_MODULE = "psycopg.types.numeric"


class Int2(int):

    __module__ = _MODULE
    __slots__ = ()

    def __new__(cls, arg: int) -> "Int2":
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class Int4(int):

    __module__ = _MODULE
    __slots__ = ()

    def __new__(cls, arg: int) -> "Int4":
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class Int8(int):

    __module__ = _MODULE
    __slots__ = ()

    def __new__(cls, arg: int) -> "Int8":
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class IntNumeric(int):

    __module__ = _MODULE
    __slots__ = ()

    def __new__(cls, arg: int) -> "IntNumeric":
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class Float4(float):

    __module__ = _MODULE
    __slots__ = ()

    def __new__(cls, arg: float) -> "Float4":
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class Float8(float):

    __module__ = _MODULE
    __slots__ = ()

    def __new__(cls, arg: float) -> "Float8":
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"


class Oid(int):

    __module__ = _MODULE
    __slots__ = ()

    def __new__(cls, arg: int) -> "Oid":
        return super().__new__(cls, arg)

    def __str__(self) -> str:
        return super().__repr__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"
