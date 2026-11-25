"""
Counters for the pool usage stats.
"""

# Copyright (C) 2026 The Psycopg Team

from __future__ import annotations

import threading
from collections import Counter


class BaseStats:
    """
    Counters for the pool usage stats.
    """

    def __init__(self) -> None:
        self._counter = Counter[str]()

    def incr(self, key: str, amount: int = 1) -> None:
        """Increment the counter `key` by `amount`."""
        self._counter[key] += amount

    def get(self) -> dict[str, int]:
        """Return a copy of the current counters."""
        return dict(self._counter)

    def pop(self) -> dict[str, int]:
        """Return the current counters and reset them to zero."""
        rv, self._counter = self._counter, Counter()
        return dict(rv)


class LocklessStats(BaseStats):
    """
    Stats not thread-safe: good for async code, where no method can switch.
    """


class LockedStats(BaseStats):
    """
    Stats safe to use from different threads.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()

    def incr(self, key: str, amount: int = 1) -> None:
        with self._lock:
            super().incr(key, amount)

    def get(self) -> dict[str, int]:
        with self._lock:
            return super().get()

    def pop(self) -> dict[str, int]:
        with self._lock:
            return super().pop()
