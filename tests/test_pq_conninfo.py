import pytest


def test_defaults(pq, tempenv):
    tempenv["PGPORT"] = "15432"
    defs = pq.Conninfo.get_defaults()
    assert len(defs) > 20
    port = [d for d in defs if d.keyword == "port"][0]
    assert port.envvar == "PGPORT"
    assert port.compiled == "5432"
    assert port.val == "15432"
    assert port.label == "Database-Port"
    assert port.dispatcher == ""
    assert port.dispsize == 6


def test_conninfo_parse(pq):
    info = pq.Conninfo.parse(
        "postgresql://host1:123,host2:456/somedb"
        "?target_session_attrs=any&application_name=myapp"
    )
    info = {i.keyword: i.val for i in info if i.val is not None}
    assert info["host"] == "host1,host2"
    assert info["port"] == "123,456"
    assert info["dbname"] == "somedb"
    assert info["application_name"] == "myapp"


def test_conninfo_parse_bad(pq):
    with pytest.raises(pq.PQerror) as e:
        pq.Conninfo.parse("bad_conninfo=")
        assert "bad_conninfo" in str(e.value)
