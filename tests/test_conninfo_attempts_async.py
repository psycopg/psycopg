import pytest

import psycopg
from psycopg.conninfo import conninfo_to_dict, conninfo_attempts_async

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        ("", [""], None),
        ("service=foo", ["service=foo"], None),
        ("host='' user=bar", ["host='' user=bar"], None),
        (
            "host=127.0.0.1 user=bar port=''",
            ["host=127.0.0.1 user=bar port='' hostaddr=127.0.0.1"],
            None,
        ),
        (
            "host=127.0.0.1 user=bar",
            ["host=127.0.0.1 user=bar hostaddr=127.0.0.1"],
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 user=bar",
            [
                "host=1.1.1.1 user=bar hostaddr=1.1.1.1",
                "host=2.2.2.2 user=bar hostaddr=2.2.2.2",
            ],
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 port=5432",
            [
                "host=1.1.1.1 port=5432 hostaddr=1.1.1.1",
                "host=2.2.2.2 port=5432 hostaddr=2.2.2.2",
            ],
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 port=5432,",
            [
                "host=1.1.1.1 port=5432 hostaddr=1.1.1.1",
                "host=2.2.2.2 port='' hostaddr=2.2.2.2",
            ],
            None,
        ),
        (
            "port=5432",
            [
                "host=1.1.1.1 port=5432 hostaddr=1.1.1.1",
                "host=2.2.2.2 port=5432 hostaddr=2.2.2.2",
            ],
            {"PGHOST": "1.1.1.1,2.2.2.2"},
        ),
        (
            "host=foo.com port=5432",
            ["host=foo.com port=5432"],
            {"PGHOSTADDR": "1.2.3.4"},
        ),
    ],
)
async def test_conninfo_attempts_no_resolve(
    setpgenv, conninfo, want, env, fail_resolve
):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    attempts = await conninfo_attempts_async(params)
    want = list(map(conninfo_to_dict, want))
    assert want == attempts


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        (
            "host=foo.com,qux.com",
            ["host=foo.com hostaddr=1.1.1.1", "host=qux.com hostaddr=2.2.2.2"],
            None,
        ),
        (
            "host=foo.com,qux.com port=5433",
            [
                "host=foo.com hostaddr=1.1.1.1 port=5433",
                "host=qux.com hostaddr=2.2.2.2 port=5433",
            ],
            None,
        ),
        (
            "host=foo.com,qux.com port=5432,5433",
            [
                "host=foo.com hostaddr=1.1.1.1 port=5432",
                "host=qux.com hostaddr=2.2.2.2 port=5433",
            ],
            None,
        ),
        (
            "host=foo.com,foo.com port=5432,",
            [
                "host=foo.com hostaddr=1.1.1.1 port=5432",
                "host=foo.com hostaddr=1.1.1.1 port=''",
            ],
            None,
        ),
        (
            "host=foo.com,nosuchhost.com",
            ["host=foo.com hostaddr=1.1.1.1"],
            None,
        ),
        (
            "host=foo.com, port=5432,5433",
            ["host=foo.com hostaddr=1.1.1.1 port=5432", "host='' port=5433"],
            None,
        ),
        (
            "host=nosuchhost.com,foo.com",
            ["host=foo.com hostaddr=1.1.1.1"],
            None,
        ),
        (
            "host=foo.com,qux.com",
            ["host=foo.com hostaddr=1.1.1.1", "host=qux.com hostaddr=2.2.2.2"],
            {},
        ),
        (
            "host=dup.com",
            ["host=dup.com hostaddr=3.3.3.3", "host=dup.com hostaddr=3.3.3.4"],
            None,
        ),
    ],
)
async def test_conninfo_attempts(conninfo, want, env, fake_resolve):
    params = conninfo_to_dict(conninfo)
    attempts = await conninfo_attempts_async(params)
    want = list(map(conninfo_to_dict, want))
    assert want == attempts


@pytest.mark.parametrize(
    "conninfo, env",
    [
        ("host=bad1.com,bad2.com", None),
        ("host=foo.com port=1,2", None),
        ("host=1.1.1.1,2.2.2.2 port=5432,5433,5434", None),
        ("host=1.1.1.1,2.2.2.2", {"PGPORT": "1,2,3"}),
    ],
)
async def test_conninfo_attempts_bad(setpgenv, conninfo, env, fake_resolve):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    with pytest.raises(psycopg.Error):
        await conninfo_attempts_async(params)


async def test_conninfo_random_multi_host():
    hosts = [f"host{n:02d}" for n in range(50)]
    args = {"host": ",".join(hosts), "hostaddr": ",".join(["127.0.0.1"] * len(hosts))}
    ahosts = [att["host"] for att in await conninfo_attempts_async(args)]
    assert ahosts == hosts

    args["load_balance_hosts"] = "disable"
    ahosts = [att["host"] for att in await conninfo_attempts_async(args)]
    assert ahosts == hosts

    args["load_balance_hosts"] = "random"
    ahosts = [att["host"] for att in await conninfo_attempts_async(args)]
    assert ahosts != hosts
    ahosts.sort()
    assert ahosts == hosts


async def test_conninfo_random_multi_ips(fake_resolve):
    args = {"host": "alot.com"}
    hostaddrs = [att["hostaddr"] for att in await conninfo_attempts_async(args)]
    assert len(hostaddrs) == 20
    assert hostaddrs == sorted(hostaddrs)

    args["load_balance_hosts"] = "disable"
    hostaddrs = [att["hostaddr"] for att in await conninfo_attempts_async(args)]
    assert hostaddrs == sorted(hostaddrs)

    args["load_balance_hosts"] = "random"
    hostaddrs = [att["hostaddr"] for att in await conninfo_attempts_async(args)]
    assert hostaddrs != sorted(hostaddrs)
