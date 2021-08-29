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
        "mypy >= 0.812",
        "pproxy >= 2.7, < 2.8",
        "pytest >= 6.2.4, < 6.3",
        "pytest-asyncio >= 0.15.0, < 0.16",
        "pytest-randomly >= 3.7, < 3.8",
        "tenacity >= 7, < 7.1",
    ],
    # Requirements needed for development
    "dev": [
        "black",
        "flake8 >= 3.8, < 3.9",
        "mypy >= 0.812",
        "wheel",
    ],
    # Requirements needed to build the documentation
    "docs": [
        "Sphinx >= 4.1, < 4.2",
        "dnspython >= 2.1.0",  # to become a package dependency
        "docutils >= 0.17, < 0.18",
        "furo >= furo-2021.8.17b43",
        "sphinx-autobuild >= 2021.3.14",
        "sphinx-autodoc-typehints >= 1.12, < 1.13",
    ],
}

setup(
    version=version,
    extras_require=extras_require,
)
