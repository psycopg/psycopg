"""
libpq enum definitions for psycopg3
"""

# Copyright (C) 2020 The Psycopg Team

from enum import IntEnum


class ConnStatusType(IntEnum):
    CONNECTION_OK = 0
    CONNECTION_BAD = 1
