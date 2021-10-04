"""
Information about PostgreSQL types

These types allow to read information from the system catalog and provide
information to the adapters if needed.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Any, Dict, Iterator, Optional, overload
from typing import Sequence, Tuple, Type, TypeVar, Union, TYPE_CHECKING

from . import errors as e
from .abc import AdaptContext
from .rows import dict_row

if TYPE_CHECKING:
    from .connection import Connection
    from .connection_async import AsyncConnection
    from .sql import Identifier

T = TypeVar("T", bound="TypeInfo")


class TypeInfo:
    """
    Hold information about a PostgreSQL base type.
    """

    __module__ = "psycopg.types"

    def __init__(
        self,
        name: str,
        oid: int,
        array_oid: int,
        alt_name: str = "",
        delimiter: str = ",",
    ):
        self.name = name
        self.oid = oid
        self.array_oid = array_oid
        self.alt_name = alt_name
        self.delimiter = delimiter

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__qualname__}:"
            f" {self.name} (oid: {self.oid}, array oid: {self.array_oid})>"
        )

    @overload
    @classmethod
    def fetch(
        cls: Type[T], conn: "Connection[Any]", name: Union[str, "Identifier"]
    ) -> Optional[T]:
        ...

    @overload
    @classmethod
    async def fetch(
        cls: Type[T],
        conn: "AsyncConnection[Any]",
        name: Union[str, "Identifier"],
    ) -> Optional[T]:
        ...

    @classmethod
    def fetch(
        cls: Type[T],
        conn: "Union[Connection[Any], AsyncConnection[Any]]",
        name: Union[str, "Identifier"],
    ) -> Any:
        """Query a system catalog to read information about a type."""
        from .connection_async import AsyncConnection

        if isinstance(conn, AsyncConnection):
            return cls._fetch_async(conn, name)

        from .sql import Composable

        if isinstance(name, Composable):
            name = name.as_string(conn)

        # This might result in a nested transaction. What we want is to leave
        # the function with the connection in the state we found (either idle
        # or intrans)
        try:
            with conn.transaction():
                with conn.cursor(binary=True, row_factory=dict_row) as cur:
                    cur.execute(cls._get_info_query(conn), {"name": name})
                    recs = cur.fetchall()
        except e.UndefinedObject:
            return None

        return cls._from_records(name, recs)

    @classmethod
    async def _fetch_async(
        cls: Type[T],
        conn: "AsyncConnection[Any]",
        name: Union[str, "Identifier"],
    ) -> Optional[T]:
        """
        Query a system catalog to read information about a type.

        Similar to `fetch()` but can use an asynchronous connection.
        """
        from .sql import Composable

        if isinstance(name, Composable):
            name = name.as_string(conn)

        try:
            async with conn.transaction():
                async with conn.cursor(
                    binary=True, row_factory=dict_row
                ) as cur:
                    await cur.execute(
                        cls._get_info_query(conn), {"name": name}
                    )
                    recs = await cur.fetchall()
        except e.UndefinedObject:
            return None

        return cls._from_records(name, recs)

    @classmethod
    def _from_records(
        cls: Type[T], name: str, recs: Sequence[Dict[str, Any]]
    ) -> Optional[T]:
        if len(recs) == 1:
            return cls(**recs[0])
        elif not recs:
            return None
        else:
            raise e.ProgrammingError(
                f"found {len(recs)} different types named {name}"
            )

    def register(self, context: Optional[AdaptContext] = None) -> None:
        """
        Register the type information, globally or in the specified *context*.
        """
        if context:
            types = context.adapters.types
        else:
            from . import postgres

            types = postgres.types

        types.add(self)

        if self.array_oid:
            from .types.array import register_array

            register_array(self, context)

    @classmethod
    def _get_info_query(
        cls, conn: "Union[Connection[Any], AsyncConnection[Any]]"
    ) -> str:
        return """\
SELECT
    typname AS name, oid, typarray AS array_oid,
    oid::regtype::text AS alt_name, typdelim AS delimiter
FROM pg_type t
WHERE t.oid = %(name)s::regtype
ORDER BY t.oid
"""

    def _added(self, registry: "TypesRegistry") -> None:
        """Method called by the *registry* when the object is added there."""
        pass


class RangeInfo(TypeInfo):
    """Manage information about a range type."""

    __module__ = "psycopg.types.range"

    def __init__(self, name: str, oid: int, array_oid: int, subtype_oid: int):
        super().__init__(name, oid, array_oid)
        self.subtype_oid = subtype_oid

    @classmethod
    def _get_info_query(
        cls, conn: "Union[Connection[Any], AsyncConnection[Any]]"
    ) -> str:
        return """\
SELECT t.typname AS name, t.oid AS oid, t.typarray AS array_oid,
    r.rngsubtype AS subtype_oid
FROM pg_type t
JOIN pg_range r ON t.oid = r.rngtypid
WHERE t.oid = %(name)s::regtype
"""

    def _added(self, registry: "TypesRegistry") -> None:
        # Map ranges subtypes to info
        registry._registry[RangeInfo, self.subtype_oid] = self


class MultirangeInfo(TypeInfo):
    """Manage information about a multirange type."""

    # TODO: expose to multirange module once added
    # __module__ = "psycopg.types.multirange"

    def __init__(
        self,
        name: str,
        oid: int,
        array_oid: int,
        range_oid: int,
        subtype_oid: int,
    ):
        super().__init__(name, oid, array_oid)
        self.range_oid = range_oid
        self.subtype_oid = subtype_oid

    @classmethod
    def _get_info_query(
        cls, conn: "Union[Connection[Any], AsyncConnection[Any]]"
    ) -> str:
        if conn.info.server_version < 140000:
            raise e.NotSupportedError(
                "multirange types are only available from PostgreSQL 14"
            )
        return """\
SELECT t.typname AS name, t.oid AS oid, t.typarray AS array_oid,
    r.rngtypid AS range_oid, r.rngsubtype AS subtype_oid
FROM pg_type t
JOIN pg_range r ON t.oid = r.rngmultitypid
WHERE t.oid = %(name)s::regtype
"""

    def _added(self, registry: "TypesRegistry") -> None:
        # Map multiranges ranges and subtypes to info
        registry._registry[MultirangeInfo, self.range_oid] = self
        registry._registry[MultirangeInfo, self.subtype_oid] = self


class CompositeInfo(TypeInfo):
    """Manage information about a composite type."""

    __module__ = "psycopg.types.composite"

    def __init__(
        self,
        name: str,
        oid: int,
        array_oid: int,
        field_names: Sequence[str],
        field_types: Sequence[int],
    ):
        super().__init__(name, oid, array_oid)
        self.field_names = field_names
        self.field_types = field_types
        # Will be set by register() if the `factory` is a type
        self.python_type: Optional[type] = None

    @classmethod
    def _get_info_query(
        cls, conn: "Union[Connection[Any], AsyncConnection[Any]]"
    ) -> str:
        return """\
SELECT
    t.typname AS name, t.oid AS oid, t.typarray AS array_oid,
    coalesce(a.fnames, '{}') AS field_names,
    coalesce(a.ftypes, '{}') AS field_types
FROM pg_type t
LEFT JOIN (
    SELECT
        attrelid,
        array_agg(attname) AS fnames,
        array_agg(atttypid) AS ftypes
    FROM (
        SELECT a.attrelid, a.attname, a.atttypid
        FROM pg_attribute a
        JOIN pg_type t ON t.typrelid = a.attrelid
        WHERE t.oid = %(name)s::regtype
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY a.attnum
    ) x
    GROUP BY attrelid
) a ON a.attrelid = t.typrelid
WHERE t.oid = %(name)s::regtype
"""


RegistryKey = Union[str, int, Tuple[type, int]]


class TypesRegistry:
    """
    Container for the information about types in a database.
    """

    __module__ = "psycopg.types"

    def __init__(self, template: Optional["TypesRegistry"] = None):
        self._registry: Dict[RegistryKey, TypeInfo]

        # Make a shallow copy: it will become a proper copy if the registry
        # is edited.
        if template:
            self._registry = template._registry
            self._own_state = False
            template._own_state = False
        else:
            self.clear()

    def clear(self) -> None:
        self._registry = {}
        self._own_state = True

    def add(self, info: TypeInfo) -> None:
        self._ensure_own_state()
        if info.oid:
            self._registry[info.oid] = info
        if info.array_oid:
            self._registry[info.array_oid] = info
        self._registry[info.name] = info

        if info.alt_name and info.alt_name not in self._registry:
            self._registry[info.alt_name] = info

        # Allow info to customise further their relation with the registry
        info._added(self)

    def __iter__(self) -> Iterator[TypeInfo]:
        seen = set()
        for t in self._registry.values():
            if t.oid not in seen:
                seen.add(t.oid)
                yield t

    @overload
    def __getitem__(self, key: Union[str, int]) -> TypeInfo:
        ...

    @overload
    def __getitem__(self, key: Tuple[Type[T], int]) -> T:
        ...

    def __getitem__(self, key: RegistryKey) -> TypeInfo:
        """
        Return info about a type, specified by name or oid

        :param key: the name or oid of the type to look for.

        Raise KeyError if not found.
        """
        if isinstance(key, str):
            if key.endswith("[]"):
                key = key[:-2]
        elif not isinstance(key, (int, tuple)):
            raise TypeError(
                f"the key must be an oid or a name, got {type(key)}"
            )
        try:
            return self._registry[key]
        except KeyError:
            raise KeyError(
                f"couldn't find the type {key!r} in the types registry"
            )

    @overload
    def get(self, key: Union[str, int]) -> Optional[TypeInfo]:
        ...

    @overload
    def get(self, key: Tuple[Type[T], int]) -> Optional[T]:
        ...

    def get(self, key: RegistryKey) -> Optional[TypeInfo]:
        """
        Return info about a type, specified by name or oid

        :param key: the name or oid of the type to look for.

        Unlike `__getitem__`, return None if not found.
        """
        try:
            return self[key]
        except KeyError:
            return None

    def get_oid(self, name: str) -> int:
        """
        Return the oid of a PostgreSQL type by name.

        :param key: the name of the type to look for.

        Return the array oid if the type ends with "``[]``"

        Raise KeyError if the name is unknown.
        """
        t = self[name]
        if name.endswith("[]"):
            return t.array_oid
        else:
            return t.oid

    def get_by_subtype(
        self, cls: Type[T], subtype: Union[int, str]
    ) -> Optional[T]:
        """
        Return info about a `TypeInfo` subclass by its element name or oid.

        :param cls: the subtype of `!TypeInfo` to look for. Currently
            supported are `~psycopg.types.range.RangeInfo` and
            `~psycopg.types.multirange.MultirangeInfo`.
        :param subtype: The name or OID of the subtype of the element to look for.
        :return: The `!TypeInfo` object of class *cls* whose subtype is
            *subtype*. `!None` if the element or its range are not found.
        """
        try:
            info = self[subtype]
        except KeyError:
            return None
        return self.get((cls, info.oid))

    def _ensure_own_state(self) -> None:
        # Time to write! so, copy.
        if not self._own_state:
            self._registry = self._registry.copy()
            self._own_state = True
