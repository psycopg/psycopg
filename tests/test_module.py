import pytest


@pytest.mark.parametrize(
    "args, kwargs, want_conninfo",
    [
        ((), {}, ""),
        (("dbname=foo",), {"user": "bar"}, "dbname=foo user=bar"),
        ((), {"port": 15432}, "port=15432"),
        ((), {"user": "foo", "dbname": None}, "user=foo"),
    ],
)
def test_connect(monkeypatch, dsn, args, kwargs, want_conninfo):
    # Check the main args passing from psycopg.connect to the conn generator
    # Details of the params manipulation are in test_conninfo.
    import psycopg.connection

    orig_connect = psycopg.connection.connect

    got_conninfo = None

    def mock_connect(conninfo):
        nonlocal got_conninfo
        got_conninfo = conninfo
        return orig_connect(dsn)

    monkeypatch.setattr(psycopg.connection, "connect", mock_connect)

    psycopg.connect(*args, **kwargs)
    assert got_conninfo == want_conninfo
