"""
libpq enum definitions for psycopg3
"""

# Copyright (C) 2020 The Psycopg Team

from enum import IntEnum


class ConnStatus(IntEnum):
    CONNECTION_OK = 0
    CONNECTION_BAD = 1
