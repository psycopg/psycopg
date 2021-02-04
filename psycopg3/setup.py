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

with open("psycopg3/version.py") as f:
    data = f.read()
    m = re.search(r"""(?m)^__version__\s*=\s*['"]([^'"]+)['"]""", data)
    if not m:
        raise Exception(f"cannot find version in {f.name}")
    version = m.group(1)

extras_require = {
    # Install the C extension module (requires dev tools)
    "c": [
        f"psycopg3-c == {version}",
    ],
    # Install the stand-alone C extension module
    "binary": [
        f"psycopg3-binary == {version}",
    ],
    "test": [
        "pytest >= 6, < 6.1",
        "pytest-asyncio >= 0.14.0, < 0.15",
        "pytest-randomly >= 3.5, < 3.6",
    ],
    "dev": [
        "black",
        "flake8 >= 3.8, < 3.9",
        "mypy >= 0.800",
    ],
}

setup(
    version=version,
    extras_require=extras_require,
    # TODO: doesn't work in setup.cfg
    package_data={"psycopg3": ["py.typed"]},
)
