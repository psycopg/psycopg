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


# Note: These tests are here because test_pipeline.py tests are all skipped
# when pipeline mode is not supported.


@pytest.mark.libpq(">= 14")
def test_pipeline_supported(conn):
    assert psycopg.Pipeline.is_supported()
    assert psycopg.AsyncPipeline.is_supported()

    with conn.pipeline():
        pass


@pytest.mark.libpq("< 14")
def test_pipeline_not_supported(conn):
    assert not psycopg.Pipeline.is_supported()
    assert not psycopg.AsyncPipeline.is_supported()

    with pytest.raises(psycopg.NotSupportedError) as exc:
        with conn.pipeline():
            pass

    assert "too old" in str(exc.value)
