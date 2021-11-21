#!/usr/bin/env python3
"""
PostgreSQL database adapter for Python - pure Python package
"""

# Copyright (C) 2020-2021 The Psycopg Team

import re
import os
from setuptools import setup

# Move to the directory of setup.py: executing this file from another location
# (e.g. from the project root) will fail
here = os.path.abspath(os.path.dirname(__file__))
if os.path.abspath(os.getcwd()) != here:
    os.chdir(here)

with open("psycopg/version.py") as f:
    data = f.read()
    m = re.search(r"""(?m)^__version__\s*=\s*['"]([^'"]+)['"]""", data)
    if not m:
        raise Exception(f"cannot find version in {f.name}")
    version = m.group(1)

extras_require = {
    # Install the C extension module (requires dev tools)
    "c": [
        f"psycopg-c == {version}",
    ],
    # Install the stand-alone C extension module
    "binary": [
        f"psycopg-binary == {version}",
    ],
    # Install the connection pool
    "pool": [
        "psycopg-pool",
    ],
    # Requirements to run the test suite
    "test": [
        "mypy >= 0.910",
        "pproxy ~= 2.7.8",
        "pytest ~= 6.2.5",
        "pytest-asyncio ~= 0.16.0",
        "pytest-cov ~= 3.0.0",
        "pytest-randomly ~= 3.10.1",
        "tenacity ~= 8.0.1",
    ],
    # Requirements needed for development
    "dev": [
        "black",
        "dnspython ~= 2.1.0",
        "flake8 ~= 4.0.1",
        "mypy >= 0.910",
        "pytest-mypy >= 0.8.1",
        "types-setuptools >= 57.4.0",
        "wheel",
    ],
    # Requirements needed to build the documentation
    "docs": [
        "Sphinx ~= 4.2.0",
        "docutils ~= 0.17.0",
        "furo >= 2021.9.8",
        "sphinx-autobuild >= 2021.3.14",
        "sphinx-autodoc-typehints ~= 1.12.0",
        # to document optional modules
        "dnspython ~= 2.1.0",
        "shapely ~= 1.7.0",
    ],
}

setup(
    version=version,
    extras_require=extras_require,
)
