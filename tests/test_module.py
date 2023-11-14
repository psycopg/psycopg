import pytest

from psycopg._cmodule import _psycopg
from psycopg.conninfo import conninfo_to_dict

from ._test_connection import drop_default_args_from_conninfo


@pytest.mark.parametrize(
    "args, kwargs, want",
    [
        ((), {}, ""),
        (("dbname=foo",), {"user": "bar"}, "dbname=foo user=bar"),
        ((), {"port": 15432}, "port=15432"),
        ((), {"user": "foo", "dbname": None}, "user=foo"),
    ],
)
def test_connect(monkeypatch, dsn, args, kwargs, want):
    # Check the main args passing from psycopg.connect to the conn generator
    # Details of the params manipulation are in test_conninfo.
    import psycopg.connection

    orig_connect = psycopg.generators.connect

    got_conninfo = None

    def mock_connect(conninfo):
        nonlocal got_conninfo
        got_conninfo = conninfo
        return orig_connect(dsn)

    monkeypatch.setattr(psycopg.generators, "connect", mock_connect)

    conn = psycopg.connect(*args, **kwargs)
    assert drop_default_args_from_conninfo(got_conninfo) == conninfo_to_dict(want)
    conn.close()


def test_version(mypy):
    cp = mypy.run_on_source(
        """\
from psycopg import __version__
assert __version__
"""
    )
    assert not cp.stdout


@pytest.mark.skipif(_psycopg is None, reason="C module test")
def test_version_c(mypy):
    # can be psycopg_c, psycopg_binary
    cpackage = _psycopg.__name__.split(".")[0]

    cp = mypy.run_on_source(
        f"""\
from {cpackage} import __version__
assert __version__
"""
    )
    assert not cp.stdout
