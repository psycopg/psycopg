"""
psycopg3 copy support
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Optional

from .proto import AdaptContext
from . import pq


class BaseCopy:
    def __init__(
        self,
        context: AdaptContext,
        result: pq.proto.PGresult,
        format: pq.Format = pq.Format.TEXT,
    ):
        from .transform import Transformer

        self._transformer = Transformer(context)
        self.format = format  # TODO: maybe not needed
        self.pgresult = result

    @property
    def pgresult(self) -> Optional[pq.proto.PGresult]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional[pq.proto.PGresult]) -> None:
        self._pgresult = result
        self._transformer.pgresult = result


class Copy(BaseCopy):
    pass


class AsyncCopy(BaseCopy):
    pass
