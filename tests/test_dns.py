import pytest

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import Row

pytestmark = [pytest.mark.dns]


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        ("", "", None),
        ("host='' user=bar", "host='' user=bar", None),
        (
            "host=127.0.0.1 user=bar",
            "host=127.0.0.1 user=bar hostaddr=127.0.0.1",
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 user=bar",
            "host=1.1.1.1,2.2.2.2 user=bar hostaddr=1.1.1.1,2.2.2.2",
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 port=5432",
            "host=1.1.1.1,2.2.2.2 port=5432 hostaddr=1.1.1.1,2.2.2.2",
            None,
        ),
        (
            "port=5432",
            "host=1.1.1.1,2.2.2.2 port=5432 hostaddr=1.1.1.1,2.2.2.2",
            {"PGHOST": "1.1.1.1,2.2.2.2"},
        ),
        (
            "host=foo.com port=5432",
            "host=foo.com port=5432",
            {"PGHOSTADDR": "1.2.3.4"},
        ),
    ],
)
@pytest.mark.asyncio
async def test_resolve_hostaddr_async_no_resolve(
    monkeypatch, conninfo, want, env, fail_resolve
):
    if env:
        for k, v in env.items():
            monkeypatch.setenv(k, v)
    params = conninfo_to_dict(conninfo)
    params = await psycopg._dns.resolve_hostaddr_async(  # type: ignore[attr-defined]
        params
    )
    assert conninfo_to_dict(want) == params


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        (
            "host=foo.com,qux.com",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2",
            None,
        ),
        (
            "host=foo.com,qux.com port=5433",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2 port=5433",
            None,
        ),
        (
            "host=foo.com,qux.com port=5432,5433",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2 port=5432,5433",
            None,
        ),
        (
            "host=foo.com,nosuchhost.com",
            "host=foo.com hostaddr=1.1.1.1",
            None,
        ),
        (
            "host=foo.com, port=5432,5433",
            "host=foo.com, hostaddr=1.1.1.1, port=5432,5433",
            None,
        ),
        (
            "host=nosuchhost.com,foo.com",
            "host=foo.com hostaddr=1.1.1.1",
            None,
        ),
        (
            "host=foo.com,qux.com",
            "host=foo.com,qux.com hostaddr=1.1.1.1,2.2.2.2",
            {},
        ),
    ],
)
@pytest.mark.asyncio
async def test_resolve_hostaddr_async(conninfo, want, env, fake_resolve):
    params = conninfo_to_dict(conninfo)
    params = await psycopg._dns.resolve_hostaddr_async(  # type: ignore[attr-defined]
        params
    )
    assert conninfo_to_dict(want) == params


@pytest.mark.parametrize(
    "conninfo, env",
    [
        ("host=bad1.com,bad2.com", None),
        ("host=foo.com port=1,2", None),
        ("host=1.1.1.1,2.2.2.2 port=5432,5433,5434", None),
        ("host=1.1.1.1,2.2.2.2", {"PGPORT": "1,2,3"}),
    ],
)
@pytest.mark.asyncio
async def test_resolve_hostaddr_async_bad(monkeypatch, conninfo, env, fake_resolve):
    if env:
        for k, v in env.items():
            monkeypatch.setenv(k, v)
    params = conninfo_to_dict(conninfo)
    with pytest.raises(psycopg.Error):
        await psycopg._dns.resolve_hostaddr_async(params)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_resolve_hostaddr_conn(monkeypatch, fake_resolve):
    got = []

    def fake_connect_gen(conninfo, **kwargs):
        got.append(conninfo)
        1 / 0

    monkeypatch.setattr(psycopg.AsyncConnection, "_connect_gen", fake_connect_gen)

    # TODO: not enabled by default, but should be usable to make a subclass
    class AsyncDnsConnection(psycopg.AsyncConnection[Row]):
        @classmethod
        async def _get_connection_params(cls, conninfo, **kwargs):
            params = await super()._get_connection_params(conninfo, **kwargs)
            params = await (
                psycopg._dns.resolve_hostaddr_async(  # type: ignore[attr-defined]
                    params
                )
            )
            return params

    with pytest.raises(ZeroDivisionError):
        await AsyncDnsConnection.connect("host=foo.com")

    assert len(got) == 1
    want = {"host": "foo.com", "hostaddr": "1.1.1.1"}
    assert conninfo_to_dict(got[0]) == want


@pytest.fixture
def fake_resolve(monkeypatch):
    import_dnspython()

    import dns.rdtypes.IN.A
    from dns.exception import DNSException

    fake_hosts = {
        "localhost": "127.0.0.1",
        "foo.com": "1.1.1.1",
        "qux.com": "2.2.2.2",
    }

    async def fake_resolve_(qname):
        try:
            addr = fake_hosts[qname]
        except KeyError:
            raise DNSException(f"unknown test host: {qname}")
        else:
            return [dns.rdtypes.IN.A.A("IN", "A", addr)]

    monkeypatch.setattr(
        psycopg._dns.async_resolver,  # type: ignore[attr-defined]
        "resolve",
        fake_resolve_,
    )


@pytest.fixture
def fail_resolve(monkeypatch):
    import_dnspython()

    async def fail_resolve_(qname):
        pytest.fail(f"shouldn't try to resolve {qname}")

    monkeypatch.setattr(
        psycopg._dns.async_resolver,  # type: ignore[attr-defined]
        "resolve",
        fail_resolve_,
    )


def import_dnspython():
    try:
        import dns.rdtypes.IN.A  # noqa: F401
    except ImportError:
        pytest.skip("dnspython package not available")

    import psycopg._dns  # noqa: F401
