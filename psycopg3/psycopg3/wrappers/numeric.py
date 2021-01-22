"""
Wrappers to force numbers to be cast as specific PostgreSQL types
"""

# Copyright (C) 2020-2021 The Psycopg Team


class Int2(int):
    def __new__(cls, arg: int) -> "Int2":
        return super().__new__(cls, arg)


class Int4(int):
    def __new__(cls, arg: int) -> "Int4":
        return super().__new__(cls, arg)


class Int8(int):
    def __new__(cls, arg: int) -> "Int8":
        return super().__new__(cls, arg)


class IntNumeric(int):
    def __new__(cls, arg: int) -> "IntNumeric":
        return super().__new__(cls, arg)


class Oid(int):
    def __new__(cls, arg: int) -> "Oid":
        return super().__new__(cls, arg)
