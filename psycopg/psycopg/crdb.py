"""
Types configuration specific for CockroachDB.
"""

# Copyright (C) 2022 The Psycopg Team

import re
from enum import Enum
from typing import Any, Optional, Type, Union, overload, TYPE_CHECKING
from ._typeinfo import TypeInfo, TypesRegistry

from . import errors as e
from .abc import AdaptContext, NoneType
from .rows import Row, RowFactory, AsyncRowFactory, TupleRow
from .postgres import TEXT_OID
from .conninfo import ConnectionInfo
from .connection import Connection
from ._adapters_map import AdaptersMap
from .connection_async import AsyncConnection
from .types.enum import EnumDumper, EnumBinaryDumper
from .types.none import NoneDumper

if TYPE_CHECKING:
    from .pq.abc import PGconn
    from .cursor import Cursor
    from .cursor_async import AsyncCursor

types = TypesRegistry()

# Global adapter maps with PostgreSQL types configuration
adapters = AdaptersMap(types=types)


class _CrdbConnectionMixin:

    _adapters: Optional[AdaptersMap]
    pgconn: "PGconn"

    @classmethod
    def is_crdb(
        cls, conn: Union[Connection[Any], AsyncConnection[Any], "PGconn"]
    ) -> bool:
        """
        Return True if the server connected to ``conn`` is CockroachDB.
        """
        if isinstance(conn, (Connection, AsyncConnection)):
            conn = conn.pgconn

        return bool(conn.parameter_status(b"crdb_version"))

    @property
    def adapters(self) -> AdaptersMap:
        if not self._adapters:
            # By default, use CockroachDB adapters map
            self._adapters = AdaptersMap(adapters)

        return self._adapters

    @property
    def info(self) -> "CrdbConnectionInfo":
        return CrdbConnectionInfo(self.pgconn)


class CrdbConnection(_CrdbConnectionMixin, Connection[Row]):
    # TODO: this method shouldn't require re-definition if the base class
    # implements a generic self.
    # https://github.com/psycopg/psycopg/issues/308
    @overload
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: RowFactory[Row],
        prepare_threshold: Optional[int] = 5,
        cursor_factory: "Optional[Type[Cursor[Row]]]" = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "CrdbConnection[Row]":
        ...

    @overload
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: "Optional[Type[Cursor[Any]]]" = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "CrdbConnection[TupleRow]":
        ...

    @classmethod
    def connect(cls, conninfo: str = "", **kwargs: Any) -> "CrdbConnection[Any]":
        return super().connect(conninfo, **kwargs)  # type: ignore[return-value]


class AsyncCrdbConnection(_CrdbConnectionMixin, AsyncConnection[Row]):
    # TODO: this method shouldn't require re-definition if the base class
    # implements a generic self.
    # https://github.com/psycopg/psycopg/issues/308
    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        row_factory: AsyncRowFactory[Row],
        cursor_factory: "Optional[Type[AsyncCursor[Row]]]" = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AsyncCrdbConnection[Row]":
        ...

    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: "Optional[Type[AsyncCursor[Any]]]" = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AsyncCrdbConnection[TupleRow]":
        ...

    @classmethod
    async def connect(
        cls, conninfo: str = "", **kwargs: Any
    ) -> "AsyncCrdbConnection[Any]":
        return await super().connect(conninfo, **kwargs)  # type: ignore [no-any-return]


connect = CrdbConnection.connect


class CrdbConnectionInfo(ConnectionInfo):
    @property
    def vendor(self) -> str:
        return "CockroachDB"

    @property
    def crdb_version(self) -> int:
        """
        Return the CockroachDB server version connected.

        Return None if the server is not CockroachDB, else return a number in
        the PostgreSQL format (e.g. 21.2.10 -> 200210)

        Assume all the connections are on the same db: return a cached result on
        following calls.
        """
        sver = self.parameter_status("crdb_version")
        if not sver:
            raise e.InternalError("'crdb_version' parameter status not set")

        ver = self.parse_crdb_version(sver)
        if ver is None:
            raise e.InterfaceError(f"couldn't parse CockroachDB version from: {sver!r}")

        return ver

    @classmethod
    def parse_crdb_version(self, sver: str) -> Optional[int]:
        m = re.search(r"\bv(\d+)\.(\d+)\.(\d+)", sver)
        if not m:
            return None

        return int(m.group(1)) * 10000 + int(m.group(2)) * 100 + int(m.group(3))


class CrdbEnumDumper(EnumDumper):
    oid = TEXT_OID


class CrdbEnumBinaryDumper(EnumBinaryDumper):
    oid = TEXT_OID


class CrdbNoneDumper(NoneDumper):
    oid = TEXT_OID


def register_postgres_adapters(context: AdaptContext) -> None:
    # Same adapters used by PostgreSQL, or a good starting point for customization

    from .types import array, bool, composite, datetime
    from .types import json, numeric, string, uuid

    array.register_default_adapters(context)
    bool.register_default_adapters(context)
    composite.register_default_adapters(context)
    datetime.register_default_adapters(context)
    json.register_default_adapters(context)
    numeric.register_default_adapters(context)
    string.register_default_adapters(context)
    uuid.register_default_adapters(context)


def register_crdb_adapters(context: AdaptContext) -> None:
    from .types import array

    register_postgres_adapters(context)

    # String must come after enum to map text oid -> string dumper
    register_crdb_enum_adapters(context)
    register_crdb_string_adapters(context)
    register_crdb_json_adapters(context)
    register_crdb_net_adapters(context)
    register_crdb_none_adapters(context)

    array.register_all_arrays(adapters)


def register_crdb_string_adapters(context: AdaptContext) -> None:
    from .types import string

    # Dump strings with text oid instead of unknown.
    # Unlike PostgreSQL, CRDB seems able to cast text to most types.
    context.adapters.register_dumper(str, string.StrDumper)
    context.adapters.register_dumper(str, string.StrBinaryDumper)


def register_crdb_enum_adapters(context: AdaptContext) -> None:
    context.adapters.register_dumper(Enum, CrdbEnumBinaryDumper)
    context.adapters.register_dumper(Enum, CrdbEnumDumper)


def register_crdb_json_adapters(context: AdaptContext) -> None:
    from .types import json

    # CRDB doesn't have json/jsonb: both dump as the jsonb oid
    context.adapters.register_dumper(json.Json, json.JsonbBinaryDumper)
    context.adapters.register_dumper(json.Json, json.JsonbDumper)


def register_crdb_net_adapters(context: AdaptContext) -> None:
    from psycopg.types import net

    context.adapters.register_dumper("ipaddress.IPv4Address", net.InterfaceDumper)
    context.adapters.register_dumper("ipaddress.IPv6Address", net.InterfaceDumper)
    context.adapters.register_dumper("ipaddress.IPv4Interface", net.InterfaceDumper)
    context.adapters.register_dumper("ipaddress.IPv6Interface", net.InterfaceDumper)
    context.adapters.register_dumper("ipaddress.IPv4Address", net.AddressBinaryDumper)
    context.adapters.register_dumper("ipaddress.IPv6Address", net.AddressBinaryDumper)
    context.adapters.register_dumper(
        "ipaddress.IPv4Interface", net.InterfaceBinaryDumper
    )
    context.adapters.register_dumper(
        "ipaddress.IPv6Interface", net.InterfaceBinaryDumper
    )
    context.adapters.register_dumper(None, net.InetBinaryDumper)
    context.adapters.register_loader("inet", net.InetLoader)
    context.adapters.register_loader("inet", net.InetBinaryLoader)


def register_crdb_none_adapters(context: AdaptContext) -> None:
    context.adapters.register_dumper(NoneType, CrdbNoneDumper)


for t in [
    TypeInfo("json", 3802, 3807, regtype="jsonb"),  # Alias json -> jsonb.
    TypeInfo("int8", 20, 1016, regtype="integer"),  # Alias integer -> int8
    TypeInfo('"char"', 18, 1002),  # special case, not generated
    # autogenerated: start
    # Generated from CockroachDB 22.1.0
    TypeInfo("bit", 1560, 1561),
    TypeInfo("bool", 16, 1000, regtype="boolean"),
    TypeInfo("bpchar", 1042, 1014, regtype="character"),
    TypeInfo("bytea", 17, 1001),
    TypeInfo("date", 1082, 1182),
    TypeInfo("float4", 700, 1021, regtype="real"),
    TypeInfo("float8", 701, 1022, regtype="'double precision'"),
    TypeInfo("inet", 869, 1041),
    TypeInfo("int2", 21, 1005, regtype="smallint"),
    TypeInfo("int2vector", 22, 1006),
    TypeInfo("int4", 23, 1007),
    TypeInfo("int8", 20, 1016, regtype="bigint"),
    TypeInfo("interval", 1186, 1187),
    TypeInfo("jsonb", 3802, 3807),
    TypeInfo("name", 19, 1003),
    TypeInfo("numeric", 1700, 1231),
    TypeInfo("oid", 26, 1028),
    TypeInfo("oidvector", 30, 1013),
    TypeInfo("record", 2249, 2287),
    TypeInfo("regclass", 2205, 2210),
    TypeInfo("regnamespace", 4089, 4090),
    TypeInfo("regproc", 24, 1008),
    TypeInfo("regprocedure", 2202, 2207),
    TypeInfo("regrole", 4096, 4097),
    TypeInfo("regtype", 2206, 2211),
    TypeInfo("text", 25, 1009),
    TypeInfo("time", 1083, 1183, regtype="'time without time zone'"),
    TypeInfo("timestamp", 1114, 1115, regtype="'timestamp without time zone'"),
    TypeInfo("timestamptz", 1184, 1185, regtype="'timestamp with time zone'"),
    TypeInfo("timetz", 1266, 1270, regtype="'time with time zone'"),
    TypeInfo("unknown", 705, 0),
    TypeInfo("uuid", 2950, 2951),
    TypeInfo("varbit", 1562, 1563, regtype="'bit varying'"),
    TypeInfo("varchar", 1043, 1015, regtype="'character varying'"),
    # autogenerated: end
]:
    types.add(t)


register_crdb_adapters(adapters)
