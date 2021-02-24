"""
Information about PostgreSQL types

These types allow to read information from the system catalog and provide
information to the adapters if needed.
"""

# Copyright (C) 2020-2021 The Psycopg Team

from typing import Any, Callable, Dict, Iterator, Optional
from typing import Sequence, Type, TypeVar, Union, TYPE_CHECKING

from . import errors as e
from .rows import dict_row
from .proto import AdaptContext

if TYPE_CHECKING:
    from .connection import Connection, AsyncConnection
    from .sql import Identifier

T = TypeVar("T", bound="TypeInfo")


class TypeInfo:
    """
    Hold information about a PostgreSQL base type.

    The class allows to:

    - read information about a range type using `fetch()` and `fetch_async()`
    - configure a composite type adaptation using `register()`
    """

    __module__ = "psycopg3.types"

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

    @classmethod
    def fetch(
        cls: Type[T], conn: "Connection", name: Union[str, "Identifier"]
    ) -> Optional[T]:
        """
        Query a system catalog to read information about a type.

        :param conn: the connection to query
        :param name: the name of the type to query. It can include a schema
            name.
        :return: a `!TypeInfo` object populated with the type information,
            `!None` if not found.
        """
        from .sql import Composable

        if isinstance(name, Composable):
            name = name.as_string(conn)
        cur = conn.cursor(binary=True, row_factory=dict_row)
        cur.execute(cls._info_query, {"name": name})
        recs: Sequence[Dict[str, Any]] = cur.fetchall()
        return cls._fetch(name, recs)

    @classmethod
    async def fetch_async(
        cls: Type[T], conn: "AsyncConnection", name: Union[str, "Identifier"]
    ) -> Optional[T]:
        """
        Query a system catalog to read information about a type.

        Similar to `fetch()` but can use an asynchronous connection.
        """
        from .sql import Composable

        if isinstance(name, Composable):
            name = name.as_string(conn)

        cur = conn.cursor(binary=True, row_factory=dict_row)
        await cur.execute(cls._info_query, {"name": name})
        recs: Sequence[Dict[str, Any]] = await cur.fetchall()
        return cls._fetch(name, recs)

    @classmethod
    def _fetch(
        cls: Type[T],
        name: str,
        recs: Sequence[Dict[str, Any]],
    ) -> Optional[T]:
        if len(recs) == 1:
            return cls(**recs[0])
        elif not recs:
            return None
        else:
            raise e.ProgrammingError(
                f"found {len(recs)} different types named {name}"
            )

    def register(
        self,
        context: Optional["AdaptContext"] = None,
    ) -> None:
        """
        Register the type information, globally or in the specified *context*.
        """
        if context:
            types = context.adapters.types
        else:
            from .oids import postgres_types

            types = postgres_types

        types.add(self)

        if self.array_oid:
            from .types.array import register_adapters

            register_adapters(self, context)

    _info_query = """\
select
    typname as name, oid, typarray as array_oid,
    oid::regtype as alt_name, typdelim as delimiter
from pg_type t
where t.oid = %(name)s::regtype
order by t.oid
"""


class RangeInfo(TypeInfo):
    """Manage information about a range type."""

    __module__ = "psycopg3.types"

    def __init__(self, name: str, oid: int, array_oid: int, subtype_oid: int):
        super().__init__(name, oid, array_oid)
        self.subtype_oid = subtype_oid

    def register(
        self,
        context: Optional[AdaptContext] = None,
    ) -> None:
        super().register(context)

        from .types.range import register_adapters

        register_adapters(self, context)

    _info_query = """\
select t.typname as name, t.oid as oid, t.typarray as array_oid,
    r.rngsubtype as subtype_oid
from pg_type t
join pg_range r on t.oid = r.rngtypid
where t.oid = %(name)s::regtype
"""


class CompositeInfo(TypeInfo):
    """Manage information about a composite type."""

    __module__ = "psycopg3.types"

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

    def register(
        self,
        context: Optional[AdaptContext] = None,
        factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        super().register(context)

        from .types.composite import register_adapters

        register_adapters(self, context, factory)

    _info_query = """\
select
    t.typname as name, t.oid as oid, t.typarray as array_oid,
    coalesce(a.fnames, '{}') as field_names,
    coalesce(a.ftypes, '{}') as field_types
from pg_type t
left join (
    select
        attrelid,
        array_agg(attname) as fnames,
        array_agg(atttypid) as ftypes
    from (
        select a.attrelid, a.attname, a.atttypid
        from pg_attribute a
        join pg_type t on t.typrelid = a.attrelid
        where t.oid = %(name)s::regtype
        and a.attnum > 0
        and not a.attisdropped
        order by a.attnum
    ) x
    group by attrelid
) a on a.attrelid = t.typrelid
where t.oid = %(name)s::regtype
"""


class TypesRegistry:
    """
    Container for the information about types in a database.
    """

    def __init__(self, template: Optional["TypesRegistry"] = None):
        self._by_oid: Dict[int, TypeInfo]
        self._by_name: Dict[str, TypeInfo]
        self._by_range_subtype: Dict[int, TypeInfo]

        # Make a shallow copy: it will become a proper copy if the registry
        # is edited (note the BUG: a child will get shallow-copied, but changing
        # the parent will change children who weren't copied yet. It can be
        # probably fixed by setting _own_state to False on the parent on copy,
        # but needs testing and for the moment I'll leave it there TODO).
        if template:
            self._by_oid = template._by_oid
            self._by_name = template._by_name
            self._by_range_subtype = template._by_range_subtype
            self._own_state = False
        else:
            self._by_oid = {}
            self._by_name = {}
            self._by_range_subtype = {}
            self._own_state = True

    def add(self, info: TypeInfo) -> None:
        self._ensure_own_state()
        self._by_oid[info.oid] = info
        if info.array_oid:
            self._by_oid[info.array_oid] = info
        self._by_name[info.name] = info

        if info.alt_name and info.alt_name not in self._by_name:
            self._by_name[info.alt_name] = info

        # Map ranges subtypes to info
        if isinstance(info, RangeInfo):
            self._by_range_subtype[info.subtype_oid] = info

    def __iter__(self) -> Iterator[TypeInfo]:
        seen = set()
        for t in self._by_oid.values():
            if t.oid not in seen:
                seen.add(t.oid)
                yield t

    def __getitem__(self, key: Union[str, int]) -> TypeInfo:
        """
        Return info about a type, specified by name or oid

        The type name or oid may refer to the array too.

        Raise KeyError if not found.
        """
        if isinstance(key, str):
            if key.endswith("[]"):
                key = key[:-2]
            return self._by_name[key]
        elif isinstance(key, int):
            return self._by_oid[key]
        else:
            raise TypeError(
                f"the key must be an oid or a name, got {type(key)}"
            )

    def get(self, key: Union[str, int]) -> Optional[TypeInfo]:
        """
        Return info about a type, specified by name or oid

        The type name or oid may refer to the array too.

        Return None if not found.
        """
        try:
            return self[key]
        except KeyError:
            return None

    def get_oid(self, name: str) -> int:
        """
        Return the oid of a PostgreSQL type by name.

        Return the array oid if the type ends with "[]"

        Raise KeyError if the name is unknown.
        """
        t = self[name]
        if name.endswith("[]"):
            return t.array_oid
        else:
            return t.oid

    def get_range(self, key: Union[str, int]) -> Optional[TypeInfo]:
        """
        Return info about a range by its element name or oid

        Return None if the element or its range are not found.
        """
        try:
            info = self[key]
        except KeyError:
            return None
        return self._by_range_subtype.get(info.oid)

    def _ensure_own_state(self) -> None:
        # Time to write! so, copy.
        if not self._own_state:
            self._by_oid = self._by_oid.copy()
            self._by_name = self._by_name.copy()
            self._by_range_subtype = self._by_range_subtype.copy()
            self._own_state = True
