import gc
import sys
from typing import Tuple

import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if "gc" in item.fixturenames:
            item.add_marker(pytest.mark.refcount)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "refcount: the test checks ref counts which is sometimes flaky",
    )


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


class GCFixture:
    __slots__ = ()

    @staticmethod
    def collect() -> None:
        """
        gc.collect(), but more insisting.
        """
        for i in range(3):
            gc.collect()

    @staticmethod
    def count() -> int:
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


@pytest.fixture(name="gc")
def fixture_gc():
    """
    Provides a consistent way to run garbage collection and count references.

    **Note:** This will skip tests on PyPy.
    """
    if sys.implementation.name == "pypy":
        pytest.skip(reason="depends on refcount semantics")
    return GCFixture()


@pytest.fixture
def gc_collect():
    """
    Provides a consistent way to run garbage collection.

    **Note:** This will *not* skip tests on PyPy.
    """
    return GCFixture.collect
