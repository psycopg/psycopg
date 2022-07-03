import pytest

import psycopg
from psycopg.conninfo import conninfo_to_dict

pytestmark = [pytest.mark.dns]


@pytest.mark.asyncio
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
