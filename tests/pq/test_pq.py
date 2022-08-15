import os

import pytest

import psycopg
from psycopg import pq

from ..utils import check_libpq_version


def test_version():
    rv = pq.version()
    assert rv > 90500
    assert rv < 200000  # you are good for a while


def test_build_version():
    assert pq.__build_version__ and pq.__build_version__ >= 70400


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


def test_pipeline_supported():
    # Note: This test is here because pipeline tests are skipped on libpq < 14
    if pq.__impl__ == "python":
        assert psycopg.Pipeline.is_supported() == (pq.version() >= 140000)
    else:
        assert pq.__build_version__ is not None
        assert psycopg.Pipeline.is_supported() == (pq.__build_version__ >= 140000)
