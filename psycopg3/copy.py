"""
psycopg3 copy support
"""

# Copyright (C) 2020 The Psycopg Team

import re
from typing import Any, Deque, Dict, List, Match, Optional, Tuple
from collections import deque

from .proto import AdaptContext
from . import errors as e
from . import pq


class BaseCopy:
    def __init__(
        self,
        context: AdaptContext,
        result: Optional[pq.proto.PGresult],
        format: pq.Format = pq.Format.TEXT,
    ):
        from .adapt import Transformer

        self._transformer = Transformer(context)
        self.format = format
        self.pgresult = result
        self._finished = False

        self._partial: Deque[bytes] = deque()
        self._header_seen = False

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def pgresult(self) -> Optional[pq.proto.PGresult]:
        return self._pgresult

    @pgresult.setter
    def pgresult(self, result: Optional[pq.proto.PGresult]) -> None:
        self._pgresult = result
        self._transformer.pgresult = result

    def load(self, buffer: bytes) -> List[Tuple[Any, ...]]:
        if self._finished:
            raise e.ProgrammingError("copy already finished")

        if self.format == pq.Format.TEXT:
            return self._load_text(buffer)
        else:
            return self._load_binary(buffer)

    def _load_text(self, buffer: bytes) -> List[Tuple[Any, ...]]:
        rows = buffer.split(b"\n")
        last_row = rows.pop(-1)

        if self._partial and rows:
            self._partial.append(rows[0])
            rows[0] = b"".join(self._partial)
            self._partial.clear()

        if last_row:
            self._partial.append(last_row)

        # If there is no result then the transformer has no info about types
        load_sequence = (
            self._transformer.load_sequence
            if self.pgresult is not None
            else None
        )

        rv = []
        for row in rows:
            if row == b"\\.":
                self._finished = True
                break

            values = row.split(b"\t")
            prow = tuple(
                _bsrepl_re.sub(_bsrepl_sub, v) if v != b"\\N" else None
                for v in values
            )
            rv.append(
                load_sequence(prow) if load_sequence is not None else prow
            )

        return rv

    def _load_binary(self, buffer: bytes) -> List[Tuple[Any, ...]]:
        raise NotImplementedError


def _bsrepl_sub(
    m: Match[bytes],
    __map: Dict[bytes, bytes] = {
        b"b": b"\b",
        b"t": b"\t",
        b"n": b"\n",
        b"v": b"\v",
        b"f": b"\f",
        b"r": b"\r",
    },
) -> bytes:
    g = m.group(0)
    return __map.get(g, g)


_bsrepl_re = re.compile(rb"\\(.)")


class Copy(BaseCopy):
    pass


class AsyncCopy(BaseCopy):
    pass
