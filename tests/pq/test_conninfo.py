import pytest

from psycopg3 import pq


def test_defaults(monkeypatch):
    monkeypatch.setenv("PGPORT", "15432")
    defs = pq.Conninfo.get_defaults()
    assert len(defs) > 20
    port = [d for d in defs if d.keyword == b"port"][0]
    assert port.envvar == b"PGPORT"
    assert port.compiled == b"5432"
    assert port.val == b"15432"
    assert port.label == b"Database-Port"
    assert port.dispchar == b""
    assert port.dispsize == 6


def test_conninfo_parse():
    info = pq.Conninfo.parse(
        b"postgresql://host1:123,host2:456/somedb"
        b"?target_session_attrs=any&application_name=myapp"
    )
    info = {i.keyword: i.val for i in info if i.val is not None}
    assert info[b"host"] == b"host1,host2"
    assert info[b"port"] == b"123,456"
    assert info[b"dbname"] == b"somedb"
    assert info[b"application_name"] == b"myapp"


def test_conninfo_parse_bad():
    with pytest.raises(pq.PQerror) as e:
        pq.Conninfo.parse(b"bad_conninfo=")
        assert "bad_conninfo" in str(e.value)
