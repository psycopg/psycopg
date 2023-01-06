"""
Helper object to transform values between Python and PostgreSQL

This module exports the requested implementation to the rest of the package.
"""

# Copyright (C) 2023 The Psycopg Team

from typing import Type

from . import abc
from ._cmodule import _psycopg

Transformer: Type[abc.Transformer]

if _psycopg:
    Transformer = _psycopg.Transformer
else:
    from . import _py_transformer

    Transformer = _py_transformer.Transformer
