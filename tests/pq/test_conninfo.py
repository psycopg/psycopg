import pytest

import psycopg
from psycopg import pq


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


@pytest.mark.libpq(">= 10")
def test_conninfo_parse():
    infos = pq.Conninfo.parse(
        b"postgresql://host1:123,host2:456/somedb"
        b"?target_session_attrs=any&application_name=myapp"
    )
    info = {i.keyword: i.val for i in infos if i.val is not None}
    assert info[b"host"] == b"host1,host2"
    assert info[b"port"] == b"123,456"
    assert info[b"dbname"] == b"somedb"
    assert info[b"application_name"] == b"myapp"


@pytest.mark.libpq("< 10")
def test_conninfo_parse_96():
    conninfo = pq.Conninfo.parse(
        b"postgresql://other@localhost/otherdb"
        b"?connect_timeout=10&application_name=myapp"
    )
    info = {i.keyword: i.val for i in conninfo if i.val is not None}
    assert info[b"host"] == b"localhost"
    assert info[b"dbname"] == b"otherdb"
    assert info[b"application_name"] == b"myapp"


def test_conninfo_parse_bad():
    with pytest.raises(psycopg.OperationalError) as e:
        pq.Conninfo.parse(b"bad_conninfo=")
        assert "bad_conninfo" in str(e.value)
