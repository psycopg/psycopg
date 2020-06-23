pytest_plugins = (
    "tests.fix_db",
    "tests.fix_pq",
)


def pytest_configure(config):
    # register slow marker
    config.addinivalue_line(
        "markers", "slow: this test is kinda slow (skip with -m 'not slow')"
    )
