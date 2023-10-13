def test_version(mypy):
    cp = mypy.run_on_source(
        """\
from psycopg_pool import __version__
assert __version__
"""
    )
    assert not cp.stdout
