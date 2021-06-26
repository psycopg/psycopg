"""
Adapters for None.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from ..pq import Format
from ..adapt import Dumper
from ..proto import AdaptContext


class NoneDumper(Dumper):
    """
    Not a complete dumper as it doesn't implement dump(), but it implements
    quote(), so it can be used in sql composition.
    """

    format = Format.TEXT

    def dump(self, obj: None) -> bytes:
        raise NotImplementedError("NULL is passed to Postgres in other ways")

    def quote(self, obj: None) -> bytes:
        return b"NULL"


def register_default_globals(ctx: AdaptContext) -> None:
    NoneDumper.register(type(None), ctx)
