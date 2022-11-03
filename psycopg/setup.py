#!/usr/bin/env python3
"""
PostgreSQL database adapter for Python - pure Python package
"""

# Copyright (C) 2020 The Psycopg Team

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
        "mypy >= 0.990",
        "pproxy >= 2.7",
        "pytest >= 6.2.5",
        "pytest-asyncio >= 0.17",
        "pytest-cov >= 3.0",
        "pytest-randomly >= 3.10",
    ],
    # Requirements needed for development
    "dev": [
        "black >= 22.3.0",
        "dnspython >= 2.1",
        "flake8 >= 4.0",
        "mypy >= 0.990",
        "types-setuptools >= 57.4",
        "wheel >= 0.37",
    ],
    # Requirements needed to build the documentation
    "docs": [
        "Sphinx >= 5.0",
        "furo == 2022.6.21",
        "sphinx-autobuild >= 2021.3.14",
        "sphinx-autodoc-typehints >= 1.12",
    ],
}

setup(
    version=version,
    extras_require=extras_require,
)
