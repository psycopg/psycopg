"""
Timezone utility functions.
"""

# Copyright (C) 2020 The Psycopg Team

import logging
from typing import Dict, Optional, Union
from datetime import timezone, tzinfo

from .pq.abc import PGconn
from ._compat import ZoneInfo

logger = logging.getLogger("psycopg")

_timezones: Dict[Union[None, bytes], tzinfo] = {
    None: timezone.utc,
    b"UTC": timezone.utc,
}


def get_tzinfo(pgconn: Optional[PGconn]) -> tzinfo:
    """Return the Python timezone info of the connection's timezone."""
    tzname = pgconn.parameter_status(b"TimeZone") if pgconn else None
    try:
        return _timezones[tzname]
    except KeyError:
        sname = tzname.decode() if tzname else "UTC"
        try:
            zi: tzinfo = ZoneInfo(sname)
        except KeyError:
            logger.warning("unknown PostgreSQL timezone: %r; will use UTC", sname)
            zi = timezone.utc

        _timezones[tzname] = zi
        return zi
