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
    # Check the main args passing from psycopg3.connect to the conn generator
    # Details of the params manipulation are in test_conninfo.
    import psycopg3.connection

    orig_connect = psycopg3.connection.connect

    got_conninfo = None

    def mock_connect(conninfo):
        nonlocal got_conninfo
        got_conninfo = conninfo
        return orig_connect(dsn)

    monkeypatch.setattr(psycopg3.connection, "connect", mock_connect)

    psycopg3.connect(*args, **kwargs)
    assert got_conninfo == want_conninfo
