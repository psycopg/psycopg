#!/usr/bin/env python
"""
Update the maps of builtin types and names.

This script updates some of the files in psycopg source code with data read
from a database catalog.

Hint: use docker to upgrade types from a new version in isolation. Run:

    docker run --rm -p 11111:5432 --name pg -e POSTGRES_PASSWORD=password postgres:TAG

with a specified version tag, and then query it using:

    %(prog)s "host=localhost port=11111 user=postgres password=password"
"""

import re
import argparse
import subprocess as sp
from typing import List
from pathlib import Path

import psycopg
from psycopg.rows import TupleRow
from psycopg.crdb import CrdbConnection
from psycopg._compat import TypeAlias

Connection: TypeAlias = psycopg.Connection[TupleRow]

ROOT = Path(__file__).parent.parent


def main() -> None:
    opt = parse_cmdline()
    conn = psycopg.connect(opt.dsn, autocommit=True)

    if CrdbConnection.is_crdb(conn):
        conn = CrdbConnection.connect(opt.dsn, autocommit=True)
        update_crdb_python_oids(conn)
    else:
        update_python_oids(conn)
        update_cython_oids(conn)


def update_python_oids(conn: Connection) -> None:
    fn = ROOT / "psycopg/psycopg/postgres.py"

    lines = []
    lines.extend(get_version_comment(conn))
    lines.extend(get_py_types(conn))
    lines.extend(get_py_ranges(conn))
    lines.extend(get_py_multiranges(conn))

    update_file(fn, lines)
    sp.check_call(["black", "-q", fn])


def update_cython_oids(conn: Connection) -> None:
    fn = ROOT / "psycopg_c/psycopg_c/_psycopg/oids.pxd"

    lines = []
    lines.extend(get_version_comment(conn))
    lines.extend(get_cython_oids(conn))

    update_file(fn, lines)


def update_crdb_python_oids(conn: Connection) -> None:
    fn = ROOT / "psycopg/psycopg/crdb/_types.py"

    lines = []
    lines.extend(get_version_comment(conn))
    lines.extend(get_py_types(conn))

    update_file(fn, lines)
    sp.check_call(["black", "-q", fn])


def get_version_comment(conn: Connection) -> List[str]:
    if conn.info.vendor == "PostgreSQL":
        # Assume PG > 10
        num = conn.info.server_version
        version = f"{num // 10000}.{num % 100}"
    elif conn.info.vendor == "CockroachDB":
        assert isinstance(conn, CrdbConnection)
        num = conn.info.server_version
        version = f"{num // 10000}.{num % 10000 // 100}.{num % 100}"
    else:
        raise NotImplementedError(f"unexpected vendor: {conn.info.vendor}")
    return ["", f"    # Generated from {conn.info.vendor} {version}", ""]


def get_py_types(conn: Connection) -> List[str]:
    # Note: "record" is a pseudotype but still a useful one to have.
    # "pg_lsn" is a documented public type and useful in streaming replication
    lines = []
    for (typname, oid, typarray, regtype, typdelim) in conn.execute(
        """
select typname, oid, typarray,
    -- CRDB might have quotes in the regtype representation
    replace(typname::regtype::text, '''', '') as regtype,
    typdelim
from pg_type t
where
    oid < 10000
    and oid != '"char"'::regtype
    and (typtype = 'b' or typname = 'record')
    and (typname !~ '^(_|pg_)' or typname = 'pg_lsn')
order by typname
"""
    ):
        # Weird legacy type in postgres catalog
        if typname == "char":
            typname = regtype = '"char"'

        # https://github.com/cockroachdb/cockroach/issues/81645
        if typname == "int4" and conn.info.vendor == "CockroachDB":
            regtype = typname

        params = [f"{typname!r}, {oid}, {typarray}"]
        if regtype != typname:
            params.append(f"regtype={regtype!r}")
        if typdelim != ",":
            params.append(f"delimiter={typdelim!r}")
        lines.append(f"TypeInfo({','.join(params)}),")

    return lines


def get_py_ranges(conn: Connection) -> List[str]:
    lines = []
    for (typname, oid, typarray, rngsubtype) in conn.execute(
        """
select typname, oid, typarray, rngsubtype
from
    pg_type t
    join pg_range r on t.oid = rngtypid
where
    oid < 10000
    and typtype = 'r'
order by typname
"""
    ):
        params = [f"{typname!r}, {oid}, {typarray}, subtype_oid={rngsubtype}"]
        lines.append(f"RangeInfo({','.join(params)}),")

    return lines


def get_py_multiranges(conn: Connection) -> List[str]:
    lines = []
    for (typname, oid, typarray, rngtypid, rngsubtype) in conn.execute(
        """
select typname, oid, typarray, rngtypid, rngsubtype
from
    pg_type t
    join pg_range r on t.oid = rngmultitypid
where
    oid < 10000
    and typtype = 'm'
order by typname
"""
    ):
        params = [
            f"{typname!r}, {oid}, {typarray},"
            f" range_oid={rngtypid}, subtype_oid={rngsubtype}"
        ]
        lines.append(f"MultirangeInfo({','.join(params)}),")

    return lines


def get_cython_oids(conn: Connection) -> List[str]:
    lines = []
    for (typname, oid) in conn.execute(
        """
select typname, oid
from pg_type
where
    oid < 10000
    and (typtype = any('{b,r,m}') or typname = 'record')
    and (typname !~ '^(_|pg_)' or typname = 'pg_lsn')
order by typname
"""
    ):
        const_name = typname.upper() + "_OID"
        lines.append(f"    {const_name} = {oid}")

    return lines


def update_file(fn: Path, new: List[str]) -> None:
    with fn.open("r") as f:
        lines = f.read().splitlines()
    istart, iend = [
        i
        for i, line in enumerate(lines)
        if re.match(r"\s*#\s*autogenerated:\s+(start|end)", line)
    ]
    lines[istart + 1 : iend] = new

    with fn.open("w") as f:
        f.write("\n".join(lines))
        f.write("\n")


def parse_cmdline() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("dsn", help="where to connect to")
    opt = parser.parse_args()
    return opt


if __name__ == "__main__":
    main()
