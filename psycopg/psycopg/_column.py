"""
The Column object in Cursor.description
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from operator import attrgetter
from collections.abc import Sequence
from ._typeinfo import TypeInfo

if TYPE_CHECKING:
    from ._cursor_base import BaseCursor

# Try to import the optimized Cython implementation
try:
    from psycopg_c._psycopg import Column as _CColumn
    # Use the Cython version if available
    Column = _CColumn
except ImportError:
    # Fall back to pure Python implementation
    _CColumn = None
    raise ValueError("Cython Column implementation not available")


if _CColumn is None:
    # Pure Python implementation (used as fallback)
    class Column(Sequence[Any]):
        __module__ = "psycopg"

        def __init__(self, cursor: BaseCursor[Any, Any], index: int):
            res = cursor.pgresult
            assert res

            if fname := res.fname(index):
                self._name = fname.decode(cursor._encoding)
            else:
                # COPY_OUT results have columns but no name
                self._name = f"column_{index + 1}"

            self._ftype = res.ftype(index)
            self._types = cursor.adapters.types
            self._type_has_been_resolved = False
            self._resolved_type: TypeInfo | None = None
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
                f" type: {self.type_display} (oid: {self.type_code})>"
            )

        def __len__(self) -> int:
            return 7

        @property
        def type_display(self) -> str:
            """A pretty representation of the column type.

            It is composed by the type name, followed by eventual modifiers and
            brackets to signify arrays, e.g. :sql:`text`, :sql:`varchar(42)`,
            :sql:`date[]`.
            """
            if not self._resolved_type:
                return str(self.type_code)

            return self._resolved_type.get_type_display(oid=self.type_code, fmod=self._fmod)

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
        def display_size(self) -> int | None:
            """The field size, for string types such as :sql:`varchar(n)`."""
            return self._resolved_type.get_display_size(self._fmod) if self._resolved_type else None

        @property
        def internal_size(self) -> int | None:
            """The internal field size for fixed-size types, None otherwise."""
            fsize = self._fsize
            return fsize if fsize >= 0 else None

        @property
        def precision(self) -> int | None:
            """The number of digits for fixed precision types."""
            return self._resolved_type.get_precision(self._fmod) if self._resolved_type else None

        @property
        def scale(self) -> int | None:
            """The number of digits after the decimal point if available."""
            return self._resolved_type.get_scale(self._fmod) if self._resolved_type else None

        @property
        def null_ok(self) -> bool | None:
            """Always `!None`"""
            return None

        def _get_resolved_type(self) -> TypeInfo | None:
            if not self._type_has_been_resolved and self._types is not None:
                self._resolved_type = self._types.get(self._ftype)
                self._type_has_been_resolved = True
            return self._resolved_type