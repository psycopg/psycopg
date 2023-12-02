import gc
import sys
import asyncio
import selectors
from typing import Any, Dict, List, Tuple

import pytest

pytest_plugins = (
    "tests.fix_db",
    "tests.fix_pq",
    "tests.fix_mypy",
    "tests.fix_faker",
    "tests.fix_proxy",
    "tests.fix_psycopg",
    "tests.fix_crdb",
    "tests.pool.fix_pool",
)


def pytest_configure(config):
    markers = [
        "slow: this test is kinda slow (skip with -m 'not slow')",
        "flakey(reason): this test may fail unpredictably')",
        # There are troubles on travis with these kind of tests and I cannot
        # catch the exception for my life.
        "subprocess: the test import psycopg after subprocess",
        "timing: the test is timing based and can fail on cheese hardware",
        "gevent: the test requires the gevent module to be installed",
        "dns: the test requires dnspython to run",
        "postgis: the test requires the PostGIS extension to run",
        "numpy: the test requires numpy module to be installed",
    ]

    for marker in markers:
        config.addinivalue_line("markers", marker)


def pytest_addoption(parser):
    parser.addoption(
        "--loop",
        choices=["default", "uvloop"],
        default="default",
        help="The asyncio loop to use for async tests.",
    )


def pytest_report_header(config):
    rv = []

    rv.append(f"default selector: {selectors.DefaultSelector.__name__}")
    loop = config.getoption("--loop")
    if loop != "default":
        rv.append(f"asyncio loop: {loop}")

    return rv


def pytest_sessionstart(session):
    # Detect if there was a segfault in the previous run.
    #
    # In case of segfault, pytest doesn't get a chance to write failed tests
    # in the cache. As a consequence, retries would find no test failed and
    # assume that all tests passed in the previous run, making the whole test pass.
    cache = session.config.cache
    if cache.get("segfault", False):
        session.warn(Warning("Previous run resulted in segfault! Not running any test"))
        session.warn(Warning("(delete '.pytest_cache/v/segfault' to clear this state)"))
        raise session.Failed
    cache.set("segfault", True)


asyncio_options: Dict[str, Any] = {}
if sys.platform == "win32":
    asyncio_options[
        "loop_factory"
    ] = asyncio.WindowsSelectorEventLoopPolicy().new_event_loop


@pytest.fixture(
    params=[pytest.param(("asyncio", asyncio_options.copy()), id="asyncio")],
    scope="session",
)
def anyio_backend(request):
    backend, options = request.param
    if request.config.option.loop == "uvloop":
        options["use_uvloop"] = True
    return backend, options


allow_fail_messages: List[str] = []


def pytest_sessionfinish(session, exitstatus):
    # Mark the test run successful (in the sense -weak- that we didn't segfault).
    session.config.cache.set("segfault", False)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if allow_fail_messages:
        terminalreporter.section("failed tests ignored")
        for msg in allow_fail_messages:
            terminalreporter.line(msg)


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
