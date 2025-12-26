"""
The Column object in Cursor.description
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Protocol, Type
from operator import attrgetter
from collections.abc import Sequence

from psycopg.pq.abc import PGresult

from ._typeinfo import TypeInfo, TypesRegistry


class _ColumnBase(Protocol):
    def __init__(
        self, res: PGresult, encoding: str, types: TypesRegistry, index: int
    ): ...
    @property
    @abstractmethod
    def name(self) -> str: ...
    @property
    @abstractmethod
    def type_code(self) -> int: ...
    @property
    @abstractmethod
    def display_size(self) -> int | None: ...
    @property
    @abstractmethod
    def internal_size(self) -> int | None: ...
    @property
    @abstractmethod
    def precision(self) -> int | None: ...
    @property
    @abstractmethod
    def scale(self) -> int | None: ...
    @property
    @abstractmethod
    def null_ok(self) -> bool | None: ...
    @property
    @abstractmethod
    def type_display(self) -> str: ...


class _PythonColumn(_ColumnBase, Sequence[Any]):
    __module__ = "psycopg"

    # def __init__(self, cursor: BaseCursor[Any, Any], index: int):
    #     assert cursor.pgresult is not None
    #     self.__init__(
    #         cursor.pgresult, cursor._encoding, cursor.adapters.types, index
    #     )

    def __init__(self, res: PGresult, encoding: str, types: Any, index: int) -> None:
        self._name: str | None = None
        self._fname: bytes | None = None
        self._encoding = encoding
        self._index = index

        if fname := res.fname(index):
            self._fname = fname

        self._ftype = res.ftype(index)
        self._types = types
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
        if resolved_type := self._get_resolved_type():
            return resolved_type.get_type_display(oid=self.type_code, fmod=self._fmod)
        else:
            return str(self.type_code)

    def __getitem__(self, index: Any) -> Any:
        if isinstance(index, slice):
            return tuple(getter(self) for getter in self._attrs[index])
        else:
            return self._attrs[index](self)

    @property
    def name(self) -> str:
        """The name of the column."""
        name = self._name
        if name is None:
            name = self._decode_name()
            self._name = name
        return name

    def _decode_name(self) -> str:
        if self._fname is not None:
            name = self._fname.decode(self._encoding)
            self._fname = None
            return name

        # COPY_OUT results have columns but no name
        return f"column_{self._index + 1}"

    @property
    def type_code(self) -> int:
        """The numeric OID of the column."""
        return self._ftype

    @property
    def display_size(self) -> int | None:
        """The field size, for string types such as :sql:`varchar(n)`."""
        if resolved_type := self._get_resolved_type():
            return resolved_type.get_display_size(self._fmod)
        else:
            return None

    @property
    def internal_size(self) -> int | None:
        """The internal field size for fixed-size types, None otherwise."""
        fsize = self._fsize
        return fsize if fsize >= 0 else None

    @property
    def precision(self) -> int | None:
        """The number of digits for fixed precision types."""
        if resolved_type := self._get_resolved_type():
            return resolved_type.get_precision(self._fmod)
        else:
            return None

    @property
    def scale(self) -> int | None:
        """The number of digits after the decimal point if available."""
        if resolved_type := self._get_resolved_type():
            return resolved_type.get_scale(self._fmod)
        else:
            return None

    @property
    def null_ok(self) -> bool | None:
        """Always `!None`"""
        return None

    def _get_resolved_type(self) -> TypeInfo | None:
        if not self._type_has_been_resolved and self._types is not None:
            self._resolved_type = self._types.get(self._ftype)
            self._type_has_been_resolved = True
        return self._resolved_type


try:
    from psycopg_c._psycopg import CColumn
except ImportError:
    CColumn = None  # type: ignore[assignment,misc]

Column: Type[_ColumnBase] = CColumn if CColumn is not None else _PythonColumn
