pytest_plugins = (
    "tests.fix_db",
    "tests.fix_pq",
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
