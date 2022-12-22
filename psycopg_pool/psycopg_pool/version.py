"""
psycopg pool version file.
"""
from ._compat import metadata

# Copyright (C) 2021 The Psycopg Team

# Use a versioning scheme as defined in
# https://www.python.org/dev/peps/pep-0440/

# STOP AND READ! if you change:
try:
    __version__ = metadata.version("psycopg-pool")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0.0"
# also change:
# - `docs/news_pool.rst` to declare this version current or unreleased
