#!/usr/bin/env python3
"""
psycopg3 -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team


import re
import os
from setuptools import setup, find_packages  # type: ignore

# Grab the version without importing the module
# or we will get import errors on install if prerequisites are still missing
fn = os.path.join(os.path.dirname(__file__), "psycopg3/consts.py")
with open(fn) as f:
    m = re.search(r"""(?mi)^VERSION\s*=\s*["']+([^'"]+)["']+""", f.read())
if m:
    version = m.group(1)
else:
    raise ValueError("cannot find VERSION in the consts module")

# Read the description from the README
with open("README.rst") as f:
    readme = f.read()

# TODO: classifiers
classifiers = """
Intended Audience :: Developers
Programming Language :: Python :: 3
License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)
Topic :: Database
Topic :: Database :: Front-Ends
Topic :: Software Development
Topic :: Software Development :: Libraries :: Python Modules
"""

setup(
    name="psycopg3",
    description=readme.splitlines()[0],
    long_description="\n".join(readme.splitlines()[2:]).lstrip(),
    author="Daniele Varrazzo",
    author_email="daniele.varrazzo@gmail.com",
    url="https://psycopg.org/psycopg3/",
    python_requires=">=3.6",
    packages=find_packages(exclude=["tests"]),
    classifiers=[x for x in classifiers.split("\n") if x],
    zip_safe=False,
    version=version,
    project_urls={
        "Homepage": "https://psycopg.org/",
        "Code": "https://github.com/psycopg/psycopg3",
        "Issue Tracker": "https://github.com/psycopg/psycopg3/issues",
        "Download": "https://pypi.org/project/psycopg3/",
    },
)
