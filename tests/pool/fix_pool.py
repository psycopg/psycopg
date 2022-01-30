import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "pool: test related to the psycopg_pool package")


def pytest_collection_modifyitems(items):
    # Add the pool markers to all the tests in the pool package
    for item in items:
        if "/pool/" in item.nodeid:
            item.add_marker(pytest.mark.pool)
