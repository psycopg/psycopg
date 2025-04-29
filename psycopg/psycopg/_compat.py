"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

import sys

if sys.version_info >= (3, 10):
    from typing import TypeAlias, TypeGuard
else:
    from typing_extensions import TypeAlias, TypeGuard

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
        pass

    class Interpolation:
        pass


__all__ = [
    "Interpolation",
    "LiteralString",
    "Self",
    "Template",
    "TypeAlias",
    "TypeGuard",
    "TypeVar",
]
