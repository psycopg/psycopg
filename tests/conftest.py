import asyncio
import inspect

import pytest

pytest_plugins = (
    "tests.fix_db",
    "tests.fix_pq",
    "tests.fix_proxy",
    "tests.fix_faker",
)


def pytest_configure(config):
    # register slow marker
    config.addinivalue_line(
        "markers", "slow: this test is kinda slow (skip with -m 'not slow')"
    )

    # There are troubles on travis with these kind of tests and I cannot
    # catch the exception for my life.
    config.addinivalue_line(
        "markers", "subprocess: the test import psycopg after subprocess"
    )

    config.addinivalue_line(
        "markers",
        "timing: the test is timing based and can fail on cheese hardware",
    )


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


@pytest.fixture
def event_loop(request):
    """Return the event loop to test asyncio-marked tests."""

    loop = request.config.getoption("--loop")
    if loop == "uvloop":
        import uvloop

        uvloop.install()
    else:
        assert loop == "default"

    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
