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
        "markers", "subprocess: the test import psycopg3 after subprocess"
    )


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
