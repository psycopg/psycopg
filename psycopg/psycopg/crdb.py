"""
Types configuration specific for CockroachDB.
"""

# Copyright (C) 2022 The Psycopg Team

import re
from enum import Enum
from typing import Any, Optional, Union, TYPE_CHECKING

from . import errors as e
from .abc import AdaptContext
from .postgres import adapters as pg_adapters, TEXT_OID
from .conninfo import ConnectionInfo
from ._adapters_map import AdaptersMap
from .types.enum import EnumDumper, EnumBinaryDumper

adapters = AdaptersMap(pg_adapters)

if TYPE_CHECKING:
    from .connection import Connection
    from .connection_async import AsyncConnection


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


def register_crdb_adapters(context: AdaptContext) -> None:
    from .types import string

    adapters = context.adapters

    # Dump strings with text oid instead of unknown.
    # Unlike PostgreSQL, CRDB seems able to cast text to most types.
    adapters.register_dumper(str, string.StrDumper)
    adapters.register_dumper(Enum, CrdbEnumBinaryDumper)
    adapters.register_dumper(Enum, CrdbEnumDumper)


register_crdb_adapters(adapters)


def customize_crdb_connection(
    conn: "Union[Connection[Any], AsyncConnection[Any]]",
) -> None:
    conn._info_class = CrdbConnectionInfo

    # TODOCRDB: what if someone is passing context? they will have
    # customised the postgres adapters, so those changes wouldn't apply
    # to crdb (e.g. the Django backend in preparation).
    if conn._adapters is None:
        # Not customized by connect()
        conn._adapters = AdaptersMap(adapters)
