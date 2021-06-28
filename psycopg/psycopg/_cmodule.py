"""
Simplify access to the _psycopg module
"""

# Copyright (C) 2021 The Psycopg Team

from . import pq

# Note: "c" must the first attempt so that mypy associates the variable the
# right module interface. It will not result Optional, but hey.
if pq.__impl__ == "c":
    from psycopg_c import _psycopg
elif pq.__impl__ == "binary":
    from psycopg_binary import _psycopg  # type: ignore
elif pq.__impl__ == "python":
    _psycopg = None  # type: ignore
else:
    raise ImportError(
        f"can't find _psycopg optimised module in {pq.__impl__!r}"
    )
