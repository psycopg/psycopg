"""
Task for Scheduler and AsyncScheduler
"""

# Copyright (C) 2023 The Psycopg Team

from typing import Any, Callable, Optional, NamedTuple


class Task(NamedTuple):
    time: float
    action: Optional[Callable[[], Any]]

    def __eq__(self, other: "Task") -> Any:  # type: ignore[override]
        return self.time == other.time

    def __lt__(self, other: "Task") -> Any:  # type: ignore[override]
        return self.time < other.time

    def __le__(self, other: "Task") -> Any:  # type: ignore[override]
        return self.time <= other.time

    def __gt__(self, other: "Task") -> Any:  # type: ignore[override]
        return self.time > other.time

    def __ge__(self, other: "Task") -> Any:  # type: ignore[override]
        return self.time >= other.time
