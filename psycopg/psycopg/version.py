"""
psycopg distribution version file.
"""
from ._compat import metadata

# Copyright (C) 2020 The Psycopg Team

# Use a versioning scheme as defined in
# https://www.python.org/dev/peps/pep-0440/

try:
    __version__ = metadata.version("psycopg")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0.0"
