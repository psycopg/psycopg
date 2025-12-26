"""
Cython implementation of the Column object in Cursor.description
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython
from typing import Any
from operator import attrgetter

from psycopg_c cimport pq
from psycopg_c.pq cimport libpq

# Don't import TypeInfo at module level to avoid circular imports
# It will be used only at runtime through the _types registry


@cython.freelist(16)
cdef class Column:
    """
    A column in a cursor description.

    This is an optimized Cython implementation that makes direct libpq calls
    to avoid Python property access overhead.
    """
    cdef readonly str _name
    cdef readonly int _ftype
    cdef readonly int _fmod
    cdef readonly int _fsize
    cdef object _types
    cdef bint _type_has_been_resolved
    cdef object _resolved_type

    # Define _attrs as a module-level constant instead of a class attribute
    # (will be defined after the class)

    def __init__(self, cursor, int index):
        cdef pq.PGresult res = cursor.pgresult
        cdef const char* fname_ptr
        cdef bytes fname_bytes

        # Get the column name directly from libpq
        fname_ptr = libpq.PQfname(res._pgresult_ptr, index)
        if fname_ptr != NULL:
            fname_bytes = fname_ptr
            self._name = fname_bytes.decode(cursor._encoding)
        else:
            # COPY_OUT results have columns but no name
            self._name = f"column_{index + 1}"

        # Get type information directly from libpq
        self._ftype = libpq.PQftype(res._pgresult_ptr, index)
        self._fmod = libpq.PQfmod(res._pgresult_ptr, index)
        self._fsize = libpq.PQfsize(res._pgresult_ptr, index)

        # Defer type resolution for performance
        self._types = cursor.adapters.types
        self._type_has_been_resolved = False
        self._resolved_type = None

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
            return tuple(getter(self) for getter in _COLUMN_ATTRS[index])
        else:
            return _COLUMN_ATTRS[index](self)

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
        cdef int fsize = self._fsize
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

    def _get_resolved_type(self):
        """Lazily resolve the TypeInfo for this column."""
        if not self._type_has_been_resolved and self._types is not None:
            self._resolved_type = self._types.get(self._ftype)
            self._type_has_been_resolved = True
        return self._resolved_type

    @property
    def _attrs(self):
        """Return the column attributes tuple for __getitem__ compatibility."""
        return _COLUMN_ATTRS


# Module-level constant for Column attributes (used by __getitem__)
_COLUMN_ATTRS = tuple(
    attrgetter(attr)
    for attr in """
        name type_code display_size internal_size precision scale null_ok
        """.split()
)
