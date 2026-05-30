from typing import cast
from datetime import datetime, timedelta, timezone

from .._compat import LiteralString

# PostgreSQL epoch (2000-01-01 00:00:00 UTC)
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def lsn_to_string(lsn: int) -> LiteralString:
    """Convert an integer LSN to PostgreSQL's 'X/X' string format."""
    if lsn == 0:
        return "0/0"
    high = (lsn >> 32) & 0xFFFFFFFF
    low = lsn & 0xFFFFFFFF
    return cast(LiteralString, f"{high:X}/{low:X}")  # type: ignore[redundant-cast]


def string_to_lsn(lsn_str: str) -> int:
    """Convert PostgreSQL's 'X/X' LSN string format to integer."""
    if lsn_str == "0/0":
        return 0
    parts = lsn_str.split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid LSN format: {lsn_str}")
    high = int(parts[0], 16)
    low = int(parts[1], 16)
    return (high << 32) | low


def pg_epoch_to_datetime(microseconds: int) -> datetime:
    return PG_EPOCH + timedelta(microseconds=microseconds)
