import pytest

import psycopg
from psycopg.conninfo import conninfo_to_dict

from .test_conninfo import fake_resolve  # noqa: F401  # fixture


@pytest.mark.usefixtures("fake_resolve")
async def test_resolve_hostaddr_conn(aconn_cls, monkeypatch):
    got = []

    def fake_connect_gen(conninfo, **kwargs):
        got.append(conninfo)
        1 / 0

    monkeypatch.setattr(aconn_cls, "_connect_gen", fake_connect_gen)

    with pytest.raises(ZeroDivisionError):
        await aconn_cls.connect("host=foo.com")

    assert len(got) == 1
    want = {"host": "foo.com", "hostaddr": "1.1.1.1"}
    assert conninfo_to_dict(got[0]) == want


@pytest.mark.dns
@pytest.mark.anyio
async def test_resolve_hostaddr_async_warning(recwarn):
    import_dnspython()
    conninfo = "dbname=foo"
    params = conninfo_to_dict(conninfo)
    params = await psycopg._dns.resolve_hostaddr_async(  # type: ignore[attr-defined]
        params
    )
    assert conninfo_to_dict(conninfo) == params
    assert "resolve_hostaddr_async" in str(recwarn.pop(DeprecationWarning).message)


def import_dnspython():
    try:
        import dns.rdtypes.IN.A  # noqa: F401
    except ImportError:
        pytest.skip("dnspython package not available")

    import psycopg._dns  # noqa: F401
