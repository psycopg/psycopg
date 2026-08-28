"""isort function to sort module names by length, objects naturally."""

from __future__ import annotations

import re
from typing import Any
from collections.abc import Callable, Iterable

from isort.sorting import naturally


def psycosort(
    to_sort: Iterable[str],
    key: Callable[[str], Any] | None = None,
    reverse: bool = False,
) -> list[str]:
    # isort feeds the objects of a `from module import a, b, c` list as a
    # list, while the module lists (`import a`, `from a import`) are fed as
    # dicts.  Relying on the stack to tell the two apart (as this plug-in
    # used to) breaks with isort >= 9, which is Cython-compiled and hides
    # the intermediate frames from `inspect.stack()`.
    is_from_import = isinstance(to_sort, list)

    new_key: Callable[[str], Any] | None
    if is_from_import:
        if key:
            old_key = key

            def new_key(s: str) -> Any:
                return drop_length(old_key(s))

        else:
            new_key = drop_length
    else:
        new_key = key

    return naturally(to_sort, key=new_key, reverse=reverse)


def drop_length(s: str) -> Any:
    """Drop the length prefix from the objects sorted."""
    return re.sub(r"\d+:", "", s) if s else s
