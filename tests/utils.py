import gc
import re
import sys
import operator
from typing import Callable, Optional, Tuple
from contextlib import contextmanager

import pytest

eur = "\u20ac"


def check_libpq_version(got, want):
    """
    Verify if the libpq version is a version accepted.

    This function is called on the tests marked with something like::

        @pytest.mark.libpq(">= 12")

    and skips the test if the requested version doesn't match what's loaded.
    """
    return check_version(got, want, "libpq", postgres_rule=True)


def check_postgres_version(got, want):
    """
    Verify if the server version is a version accepted.

    This function is called on the tests marked with something like::

        @pytest.mark.pg(">= 12")

    and skips the test if the server version doesn't match what expected.
    """
    return check_version(got, want, "PostgreSQL", postgres_rule=True)


def check_version(got, want, whose_version, postgres_rule=True):
    pred = VersionCheck.parse(want, postgres_rule=postgres_rule)
    pred.whose = whose_version
    return pred.get_skip_message(got)


class VersionCheck:
    """
    Helper to compare a version number with a test spec.
    """

    def __init__(
        self,
        *,
        skip: bool = False,
        op: Optional[str] = None,
        version_tuple: Tuple[int, ...] = (),
        whose: str = "(wanted)",
        postgres_rule: bool = False,
    ):
        self.skip = skip
        self.op = op or "=="
        self.version_tuple = version_tuple
        self.whose = whose
        # Treat 10.1 as 10.0.1
        self.postgres_rule = postgres_rule

    @classmethod
    def parse(cls, spec: str, *, postgres_rule: bool = False) -> "VersionCheck":
        # Parse a spec like "> 9.6", "skip < 21.2.0"
        m = re.match(
            r"""(?ix)
            ^\s* (skip|only)?
            \s* (==|!=|>=|<=|>|<)?
            \s* (?:(\d+)(?:\.(\d+)(?:\.(\d+))?)?)?
            \s* $
            """,
            spec,
        )
        if m is None:
            pytest.fail(f"bad wanted version spec: {spec}")

        skip = (m.group(1) or "only").lower() == "skip"
        op = m.group(2)
        version_tuple = tuple(int(n) for n in m.groups()[2:] if n)

        return cls(
            skip=skip, op=op, version_tuple=version_tuple, postgres_rule=postgres_rule
        )

    def get_skip_message(self, version: Optional[int]) -> Optional[str]:
        got_tuple = self._parse_int_version(version)

        msg: Optional[str] = None
        if self.skip:
            if got_tuple:
                if not self.version_tuple:
                    msg = f"skip on {self.whose}"
                elif self._match_version(got_tuple):
                    msg = (
                        f"skip on {self.whose} {self.op}"
                        f" {'.'.join(map(str, self.version_tuple))}"
                    )
        else:
            if not got_tuple:
                msg = f"only for {self.whose}"
            elif not self._match_version(got_tuple):
                if self.version_tuple:
                    msg = (
                        f"only for {self.whose} {self.op}"
                        f" {'.'.join(map(str, self.version_tuple))}"
                    )
                else:
                    msg = f"only for {self.whose}"

        return msg

    _OP_NAMES = {">=": "ge", "<=": "le", ">": "gt", "<": "lt", "==": "eq", "!=": "ne"}

    def _match_version(self, got_tuple: Tuple[int, ...]) -> bool:
        if not self.version_tuple:
            return True

        version_tuple = self.version_tuple
        if self.postgres_rule and version_tuple and version_tuple[0] >= 10:
            assert len(version_tuple) <= 2
            version_tuple = version_tuple[:1] + (0,) + version_tuple[1:]

        op: Callable[[Tuple[int, ...], Tuple[int, ...]], bool]
        op = getattr(operator, self._OP_NAMES[self.op])
        return op(got_tuple, version_tuple)

    def _parse_int_version(self, version: Optional[int]) -> Tuple[int, ...]:
        if version is None:
            return ()
        version, ver_fix = divmod(version, 100)
        ver_maj, ver_min = divmod(version, 100)
        return (ver_maj, ver_min, ver_fix)


def gc_collect():
    """
    gc.collect(), but more insisting.
    """
    for i in range(3):
        gc.collect()


NO_COUNT_TYPES: Tuple[type, ...] = ()

if sys.version_info[:2] == (3, 10):
    # On my laptop there are occasional creations of a single one of these objects
    # with empty content, which might be some Decimal caching.
    # Keeping the guard as strict as possible, to be extended if other types
    # or versions are necessary.
    try:
        from _contextvars import Context  # type: ignore
    except ImportError:
        pass
    else:
        NO_COUNT_TYPES += (Context,)


def gc_count() -> int:
    """
    len(gc.get_objects()), with subtleties.
    """
    if not NO_COUNT_TYPES:
        return len(gc.get_objects())

    # Note: not using a list comprehension because it pollutes the objects list.
    rv = 0
    for obj in gc.get_objects():
        if isinstance(obj, NO_COUNT_TYPES):
            continue
        rv += 1

    return rv


async def alist(it):
    return [i async for i in it]


@contextmanager
def raiseif(cond, *args, **kwargs):
    """
    Context behaving like `pytest.raises` if cond is true, else no-op.

    Return None if no error was thrown (i.e. condition is false), else
    return what `pytest.raises` returns.
    """
    if not cond:
        yield
        return

    else:
        with pytest.raises(*args, **kwargs) as ex:
            yield ex
        return
