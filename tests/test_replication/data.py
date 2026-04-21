import hashlib

_manifest_base = b"""{ "PostgreSQL-Backup-Manifest-Version": 2,
"System-Identifier": %(system_identifier)s,
"Files": [
{ "Path": "backup_label", "Size": 219, "Last-Modified": "2026-05-01 02:52:20 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "4eb93b89" },
{ "Path": "pg_multixact/members/0000", "Size": 8192, "Last-Modified": "2026-04-11 17:47:46 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "23464490" },
{ "Path": "pg_multixact/offsets/0000", "Size": 8192, "Last-Modified": "2026-04-24 02:01:02 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "23464490" },
{ "Path": "PG_VERSION", "Size": 3, "Last-Modified": "2026-04-11 17:47:46 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "64440205" },
{ "Path": "postgresql.log", "Size": 6295468, "Last-Modified": "2026-05-01 02:52:20 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "664e8166" },
{ "Path": "pg_hba.conf", "Size": 5711, "Last-Modified": "2026-04-11 17:47:46 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "d62da38c" },
{ "Path": "pg_logical/replorigin_checkpoint", "Size": 8, "Last-Modified": "2026-05-01 02:52:20 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "c74b6748" },
{ "Path": "postgresql.conf", "Size": 30938, "Last-Modified": "2026-04-11 19:40:05 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "97d8a37b" },
{ "Path": "postgresql.auto.conf", "Size": 121, "Last-Modified": "2026-04-24 02:00:07 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "ace45e7e" },
{ "Path": "pg_xact/0000", "Size": 8192, "Last-Modified": "2026-04-25 02:08:00 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "3928b3d4" },
{ "Path": "postgresapp_config.plist", "Size": 679, "Last-Modified": "2026-04-11 17:47:47 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "05653956" },
{ "Path": "pg_ident.conf", "Size": 2640, "Last-Modified": "2026-04-11 17:47:46 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "0ce04d87" },
{ "Path": "base/1/PG_VERSION", "Size": 3, "Last-Modified": "2026-04-11 17:47:46 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "64440205" },
{ "Path": "global/pg_control", "Size": 8192, "Last-Modified": "2026-05-01 02:52:20 GMT", "Checksum-Algorithm": "CRC32C", "Checksum": "43872087" }
],
"WAL-Ranges": [
{ "Timeline": 1, "Start-LSN": "2/98000028", "End-LSN": "2/98000120" }
],
"""  # noqa: E501


def assemble_example_manifest(system_identifier):
    manifest = _manifest_base % {b"system_identifier": system_identifier}
    checksum = hashlib.sha256(manifest).hexdigest().encode("ascii")
    return manifest + b'"Manifest-Checksum": "' + checksum + b'"}\n'
