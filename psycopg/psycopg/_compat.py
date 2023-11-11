"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

import sys

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
    from functools import cache
    from collections import Counter, deque as Deque
else:
    from typing import Counter, Deque
    from functools import lru_cache
    from backports.zoneinfo import ZoneInfo

    cache = lru_cache(maxsize=None)

if sys.version_info >= (3, 10):
    from typing import TypeGuard
else:
    from typing_extensions import TypeGuard

if sys.version_info >= (3, 11):
    from typing import LiteralString
else:
    from typing_extensions import LiteralString

__all__ = [
    "Counter",
    "Deque",
    "LiteralString",
    "TypeGuard",
    "ZoneInfo",
    "cache",
]
