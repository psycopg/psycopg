"""
Build backend to build a Cython-based project only if needed.

This backend adds a build dependency on Cython if pxd files are available,
otherwise it only relies on the c files to have been precompiled.
"""

# Copyright (C) 2023 The Psycopg Team

from __future__ import annotations

import os
import sys
from typing import Any

from setuptools import build_meta

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def get_requires_for_build_wheel(config_settings: Any = None) -> list[str]:
    if not os.path.exists("psycopg_c/_psycopg.pyx"):
        # Cython files don't exist: we must be in a sdist and we can trust
        # that the .c files we have packaged exist.
        return []

    # Cython files exists: we must be in a git checkout and we need Cython
    # to build. Get the version from the pyproject itself to keep things in the
    # same place.
    with open("pyproject.toml", "rb") as f:
        pyprj = tomllib.load(f)

    rv: list[str] = pyprj["cython-backend"]["cython-requires"]
    return rv


get_requires_for_build_sdist = get_requires_for_build_wheel

# For the rest, behave like the rest of setuptoos.build_meta
prepare_metadata_for_build_wheel = build_meta.prepare_metadata_for_build_wheel
build_wheel = build_meta.build_wheel
build_sdist = build_meta.build_sdist
