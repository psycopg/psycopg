import os

import pytest

from psycopg import pq

from ..utils import check_libpq_version


def test_version():
    rv = pq.version()
    assert rv > 90500
    assert rv < 200000  # you are good for a while


def test_build_version():
    if pq.__impl__ == "python":
        assert pq.__build_version__ is None
    elif pq.__impl__ in ["c", "binary"]:
        assert pq.__build_version__ and pq.__build_version__ >= 70400
    else:
        assert False, f"unexpected libpq implementation: {pq.__impl__}"


@pytest.mark.skipif("not os.environ.get('PSYCOPG_TEST_WANT_LIBPQ_BUILD')")
def test_want_built_version():
    want = os.environ["PSYCOPG_TEST_WANT_LIBPQ_BUILD"]
    got = pq.__build_version__
    assert not check_libpq_version(got, want)


@pytest.mark.skipif("not os.environ.get('PSYCOPG_TEST_WANT_LIBPQ_IMPORT')")
def test_want_import_version():
    want = os.environ["PSYCOPG_TEST_WANT_LIBPQ_IMPORT"]
    got = pq.version()
    assert not check_libpq_version(got, want)
