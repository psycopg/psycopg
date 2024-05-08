"""
The Column object in Cursor.description
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, Optional, Sequence, TYPE_CHECKING
from operator import attrgetter

if TYPE_CHECKING:
    from ._cursor_base import BaseCursor


class Column(Sequence[Any]):
    __module__ = "psycopg"

    def __init__(self, cursor: "BaseCursor[Any, Any]", index: int):
        res = cursor.pgresult
        assert res

        fname = res.fname(index)
        if fname:
            self._name = fname.decode(cursor._encoding)
        else:
            # COPY_OUT results have columns but no name
            self._name = f"column_{index + 1}"

        self._ftype = res.ftype(index)
        self._type = cursor.adapters.types.get(self._ftype)
        self._fmod = res.fmod(index)
        self._fsize = res.fsize(index)

    _attrs = tuple(
        attrgetter(attr)
        for attr in """
            name type_code display_size internal_size precision scale null_ok
            """.split()
    )

    def __repr__(self) -> str:
        return (
            f"<Column {self.name!r},"
            f" type: {self._type_display()} (oid: {self.type_code})>"
        )

    def __len__(self) -> int:
        return 7

    def _type_display(self) -> str:
        if not self._type:
            return str(self.type_code)

        parts = []
        parts.append(self._type.name)
        mod = self._type.get_modifier(self._fmod)
        if mod:
            parts.append(f"({','.join(map(str, mod))})")

        if self.type_code == self._type.array_oid:
            parts.append("[]")

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
        return self._ftype

    @property
    def display_size(self) -> Optional[int]:
        """The field size, for :sql:`varchar(n)`, None otherwise."""
        return self._type.get_display_size(self._fmod) if self._type else None

    @property
    def internal_size(self) -> Optional[int]:
        """The internal field size for fixed-size types, None otherwise."""
        fsize = self._fsize
        return fsize if fsize >= 0 else None

    @property
    def precision(self) -> Optional[int]:
        """The number of digits for fixed precision types."""
        return self._type.get_precision(self._fmod) if self._type else None

    @property
    def scale(self) -> Optional[int]:
        """The number of digits after the decimal point if available."""
        return self._type.get_scale(self._fmod) if self._type else None

    @property
    def null_ok(self) -> Optional[bool]:
        """Always `!None`"""
        return None
