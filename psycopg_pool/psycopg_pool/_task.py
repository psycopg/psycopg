"""
Task for Scheduler and AsyncScheduler
"""

# Copyright (C) 2023 The Psycopg Team

from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass, field


@dataclass(order=True)
class Task:
    time: float
    action: Callable[[], Any] | None = field(compare=False)
