"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

import sys
from typing import Type

import psycopg.errors as e

if sys.version_info >= (3, 9):
    from collections import Counter, deque as Deque
else:
    from typing import Counter, Deque

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

__all__ = [
    "Counter",
    "Deque",
    "Self",
]

# Workaround for psycopg < 3.0.8.
# Timeout on NullPool connection mignt not work correctly.
try:
    ConnectionTimeout: Type[e.OperationalError] = e.ConnectionTimeout
except AttributeError:

    class DummyConnectionTimeout(e.OperationalError):
        pass

    ConnectionTimeout = DummyConnectionTimeout
