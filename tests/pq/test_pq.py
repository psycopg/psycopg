import psycopg
from psycopg import pq


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


def test_pipeline_supported():
    # Note: This test is here because pipeline tests are skipped on libpq < 14
    if pq.__impl__ == "python":
        assert psycopg.Pipeline.is_supported() == (pq.version() >= 140000)
    else:
        assert pq.__build_version__ is not None
        assert psycopg.Pipeline.is_supported() == (pq.__build_version__ >= 140000)
