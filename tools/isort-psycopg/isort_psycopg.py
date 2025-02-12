"""isort function to sort module names by length, objects naturally."""

from __future__ import annotations

import re
import inspect
from typing import Any, Callable, Iterable

from isort.sorting import naturally


def psycosort(
    to_sort: Iterable[str],
    key: Callable[[str], Any] | None = None,
    reverse: bool = False,
) -> list[str]:
    # Sniff whether we are sorting an import list (from module import a, b, c)
    # or a list of modules.
    # Tested with isort 6.0. It might break in the future!
    is_from_import = any(
        f for f in inspect.stack() if f.function == "_with_from_imports"
    )

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
