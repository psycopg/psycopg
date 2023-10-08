"""
Task for Scheduler and AsyncScheduler
"""

# Copyright (C) 2023 The Psycopg Team

from typing import Any, Callable, Optional
from dataclasses import dataclass, field


@dataclass(order=True)
class Task:
    time: float
    action: Optional[Callable[[], Any]] = field(compare=False)
