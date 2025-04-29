"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

from __future__ import annotations

import sys
from typing import Any, Iterator

if sys.version_info >= (3, 11):
    from typing import LiteralString, Self
else:
    from typing_extensions import LiteralString, Self

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

if sys.version_info >= (3, 14):
    from string.templatelib import Interpolation, Template
else:

    class Template:
        def __iter__(self) -> Iterator[str | Interpolation]:
            return
            yield

    class Interpolation:
        value: Any
        expression: str
        format_spec: str
        conversion: str | None


__all__ = [
    "Interpolation",
    "LiteralString",
    "Self",
    "Template",
    "TypeVar",
]
