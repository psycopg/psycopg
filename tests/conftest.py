import sys
import asyncio
import selectors
from typing import Any, Dict, List

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
        "dns: the test requires dnspython to run",
        "postgis: the test requires the PostGIS extension to run",
    ]

    for marker in markers:
        config.addinivalue_line("markers", marker)


def pytest_addoption(parser):
    parser.addoption(
        "--anyio",
        choices=["asyncio", "trio"],
        help=(
            "Use AnyIO implementation of the async API, run tests with "
            "specified backend. If unset, use plain asyncio implementation, "
            "and run with asyncio backend."
        ),
    )
    parser.addoption(
        "--loop",
        choices=["default", "uvloop"],
        default="default",
        help="The asyncio loop to use for async tests.",
    )


def pytest_report_header(config):
    rv = []

    rv.append(f"default selector: {selectors.DefaultSelector.__name__}")
    backend = config.getoption("--anyio")
    if backend:
        rv.append(f"AnyIO backend: {backend}")
    loop = config.getoption("--loop")
    if loop != "default":
        if backend not in (None, "asyncio"):
            raise pytest.UsageError(
                f"--loop={loop} is incompatible with --anyio={backend}"
            )
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


@pytest.fixture(scope="session")
def use_anyio(pytestconfig, anyio_backend):
    """True if AnyIO-based implementations of Psycopg API should be used."""
    return pytestconfig.option.anyio is not None


asyncio_options: Dict[str, Any] = {}
if sys.platform == "win32" and sys.version_info >= (3, 8):
    asyncio_options["policy"] = asyncio.WindowsSelectorEventLoopPolicy()


@pytest.fixture(scope="session")
def anyio_backend(pytestconfig):
    opt = pytestconfig.option.anyio
    if opt in (None, "asyncio"):
        options = asyncio_options.copy()
        if pytestconfig.option.loop == "uvloop":
            options["use_uvloop"] = True
        return "asyncio", options
    else:
        assert opt == "trio"
        return "trio", {}


@pytest.fixture(scope="session")
def anyio_backend_name(anyio_backend):
    # Redefined because 'anyio_backend' is session-scoped for us.
    if isinstance(anyio_backend, str):
        return anyio_backend
    else:
        return anyio_backend[0]


asyncio_backend = pytest.mark.parametrize(
    "anyio_backend", [("asyncio", asyncio_options)], ids=["asyncio"], scope="session"
)


allow_fail_messages: List[str] = []


def pytest_sessionfinish(session, exitstatus):
    # Mark the test run successful (in the sense -weak- that we didn't segfault).
    session.config.cache.set("segfault", False)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if allow_fail_messages:
        terminalreporter.section("failed tests ignored")
        for msg in allow_fail_messages:
            terminalreporter.line(msg)
