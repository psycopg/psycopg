import sys
import asyncio
import inspect

import pytest

pytest_plugins = (
    "tests.fix_db",
    "tests.fix_pq",
    "tests.fix_mypy",
    "tests.fix_faker",
    "tests.fix_proxy",
    "tests.fix_psycopg",
    "tests.pool.fix_pool",
)


def pytest_configure(config):
    markers = [
        "slow: this test is kinda slow (skip with -m 'not slow')",
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
        "--loop",
        choices=["default", "uvloop"],
        default="default",
        help="The asyncio loop to use for async tests.",
    )


def pytest_report_header(config):
    loop = config.getoption("--loop")
    if loop == "default":
        return []

    return [f"asyncio loop: {loop}"]


@pytest.fixture
def retries(request):
    """Retry a block in a test a few times before giving up."""
    import tenacity

    if inspect.iscoroutinefunction(request.function):
        return tenacity.AsyncRetrying(
            reraise=True, stop=tenacity.stop_after_attempt(3)
        )
    else:
        return tenacity.Retrying(
            reraise=True, stop=tenacity.stop_after_attempt(3)
        )


def pytest_sessionstart(session):
    # Configure the async loop.
    loop = session.config.getoption("--loop")
    if loop == "uvloop":
        import uvloop

        uvloop.install()
    else:
        assert loop == "default"

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
