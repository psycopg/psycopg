"""
The Column object in Cursor.description
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, NamedTuple, Optional, Sequence, TYPE_CHECKING
from operator import attrgetter

from . import errors as e
from .oids import builtins

if TYPE_CHECKING:
    from .cursor import BaseCursor


class ColumnData(NamedTuple):
    ftype: int
    fmod: int
    fsize: int


class Column(Sequence[Any]):

    __module__ = "psycopg3"

    def __init__(self, cursor: "BaseCursor[Any]", index: int):
        res = cursor.pgresult
        assert res

        fname = res.fname(index)
        if not fname:
            raise e.InterfaceError(f"no name available for column {index}")

        self._name = fname.decode(cursor.connection.client_encoding)

        self._data = ColumnData(
            ftype=res.ftype(index),
            fmod=res.fmod(index),
            fsize=res.fsize(index),
        )

    _attrs = tuple(
        attrgetter(attr)
        for attr in """
            name type_code display_size internal_size precision scale null_ok
            """.split()
    )

    def __repr__(self) -> str:
        return f"<Column {self.name}, type: {self._type_display()}>"

    def __len__(self) -> int:
        return 7

    def _type_display(self) -> str:
        parts = []
        t = builtins.get(self.type_code)
        parts.append(t.name if t else str(self.type_code))

        mod1 = self.precision
        if mod1 is None:
            mod1 = self.display_size
        if mod1:
            parts.append(f"({mod1}")
            if self.scale:
                parts.append(f", {self.scale}")
            parts.append(")")

        return "".join(parts)

    def __getitem__(self, index: Any) -> Any:
        if isinstance(index, slice):
            return tuple(getter(self) for getter in self._attrs[index])
        else:
            return self._attrs[index](self)

    @property
    def name(self) -> str:
        """The name of the column."""
        return self._name

    @property
    def type_code(self) -> int:
        """The numeric OID of the column."""
        return self._data.ftype

    @property
    def display_size(self) -> Optional[int]:
        """The field size, for :sql:`varchar(n)`, None otherwise."""
        t = builtins.get(self.type_code)
        if not t:
            return None

        if t.name in ("varchar", "char"):
            fmod = self._data.fmod
            if fmod >= 0:
                return fmod - 4

        return None

    @property
    def internal_size(self) -> Optional[int]:
        """The interal field size for fixed-size types, None otherwise."""
        fsize = self._data.fsize
        return fsize if fsize >= 0 else None

    @property
    def precision(self) -> Optional[int]:
        """The number of digits for fixed precision types."""
        t = builtins.get(self.type_code)
        if not t:
            return None

        dttypes = ("time", "timetz", "timestamp", "timestamptz", "interval")
        if t.name == "numeric":
            fmod = self._data.fmod
            if fmod >= 0:
                return fmod >> 16

        elif t.name in dttypes:
            fmod = self._data.fmod
            if fmod >= 0:
                return fmod & 0xFFFF

        return None

    @property
    def scale(self) -> Optional[int]:
        """The number of digits after the decimal point if available.

        TODO: probably better than precision for datetime objects? review.
        """
        if self.type_code == builtins["numeric"].oid:
            fmod = self._data.fmod - 4
            if fmod >= 0:
                return fmod & 0xFFFF

        return None

    @property
    def null_ok(self) -> Optional[bool]:
        """Always `!None`"""
        return None
