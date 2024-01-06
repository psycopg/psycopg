import socket
import asyncio

import pytest

import psycopg
from psycopg import ProgrammingError
from psycopg.conninfo import make_conninfo, conninfo_to_dict
from psycopg.conninfo import conninfo_attempts, conninfo_attempts_async
from psycopg.conninfo import timeout_from_conninfo, _DEFAULT_CONNECT_TIMEOUT

snowman = "\u2603"


class MyString(str):
    pass


@pytest.mark.parametrize(
    "conninfo, kwargs, exp",
    [
        ("", {}, ""),
        ("dbname=foo", {}, "dbname=foo"),
        ("dbname=foo", {"user": "bar"}, "dbname=foo user=bar"),
        ("dbname=sony", {"password": ""}, "dbname=sony password="),
        ("dbname=foo", {"dbname": "bar"}, "dbname=bar"),
        ("user=bar", {"dbname": "foo bar"}, "dbname='foo bar' user=bar"),
        ("", {"dbname": "foo"}, "dbname=foo"),
        ("", {"dbname": "foo", "user": None}, "dbname=foo"),
        ("", {"dbname": "foo", "port": 15432}, "dbname=foo port=15432"),
        ("", {"dbname": "a'b"}, r"dbname='a\'b'"),
        (f"dbname={snowman}", {}, f"dbname={snowman}"),
        ("", {"dbname": snowman}, f"dbname={snowman}"),
        (
            "postgresql://host1/test",
            {"host": "host2"},
            "dbname=test host=host2",
        ),
        (MyString(""), {}, ""),
    ],
)
def test_make_conninfo(conninfo, kwargs, exp):
    out = make_conninfo(conninfo, **kwargs)
    assert conninfo_to_dict(out) == conninfo_to_dict(exp)


@pytest.mark.parametrize(
    "conninfo, kwargs",
    [
        ("hello", {}),
        ("dbname=foo bar", {}),
        ("foo=bar", {}),
        ("dbname=foo", {"bar": "baz"}),
        ("postgresql://tester:secret@/test?port=5433=x", {}),
        (f"{snowman}={snowman}", {}),
    ],
)
def test_make_conninfo_bad(conninfo, kwargs):
    with pytest.raises(ProgrammingError):
        make_conninfo(conninfo, **kwargs)


@pytest.mark.parametrize(
    "conninfo, exp",
    [
        ("", {}),
        ("dbname=foo user=bar", {"dbname": "foo", "user": "bar"}),
        ("dbname=sony password=", {"dbname": "sony", "password": ""}),
        ("dbname='foo bar'", {"dbname": "foo bar"}),
        ("dbname='a\"b'", {"dbname": 'a"b'}),
        (r"dbname='a\'b'", {"dbname": "a'b"}),
        (r"dbname='a\\b'", {"dbname": r"a\b"}),
        (f"dbname={snowman}", {"dbname": snowman}),
        (
            "postgresql://tester:secret@/test?port=5433",
            {
                "user": "tester",
                "password": "secret",
                "dbname": "test",
                "port": "5433",
            },
        ),
    ],
)
def test_conninfo_to_dict(conninfo, exp):
    assert conninfo_to_dict(conninfo) == exp


def test_no_munging():
    dsnin = "dbname=a host=b user=c password=d"
    dsnout = make_conninfo(dsnin)
    assert dsnin == dsnout


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        ("", [""], None),
        ("service=foo", ["service=foo"], None),
        ("host='' user=bar", ["host='' user=bar"], None),
        (
            "host=127.0.0.1 user=bar",
            ["host=127.0.0.1 user=bar"],
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 user=bar",
            ["host=1.1.1.1 user=bar", "host=2.2.2.2 user=bar"],
            None,
        ),
        (
            "host=1.1.1.1,2.2.2.2 port=5432",
            ["host=1.1.1.1 port=5432", "host=2.2.2.2 port=5432"],
            None,
        ),
        (
            "host=1.1.1.1,1.1.1.1 port=5432,",
            ["host=1.1.1.1 port=5432", "host=1.1.1.1 port=''"],
            None,
        ),
        (
            "host=foo.com port=5432",
            ["host=foo.com port=5432"],
            {"PGHOSTADDR": "1.2.3.4"},
        ),
    ],
)
@pytest.mark.anyio
def test_conninfo_attempts(setpgenv, conninfo, want, env):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    attempts = conninfo_attempts(params)
    want = list(map(conninfo_to_dict, want))
    assert want == attempts


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        ("", [""], None),
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
@pytest.mark.anyio
async def test_conninfo_attempts_async_no_resolve(
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
@pytest.mark.anyio
async def test_conninfo_attempts_async(conninfo, want, env, fake_resolve):
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
@pytest.mark.anyio
async def test_conninfo_attempts_async_bad(setpgenv, conninfo, env, fake_resolve):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    with pytest.raises(psycopg.Error):
        await conninfo_attempts_async(params)


@pytest.mark.parametrize(
    "conninfo, env",
    [
        ("host=foo.com port=1,2", None),
        ("host=1.1.1.1,2.2.2.2 port=5432,5433,5434", None),
        ("host=1.1.1.1,2.2.2.2", {"PGPORT": "1,2,3"}),
    ],
)
@pytest.mark.anyio
def test_conninfo_attempts_bad(setpgenv, conninfo, env):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    with pytest.raises(psycopg.Error):
        conninfo_attempts(params)


def test_conninfo_random():
    hosts = [f"host{n:02d}" for n in range(50)]
    args = {"host": ",".join(hosts)}
    ahosts = [att["host"] for att in conninfo_attempts(args)]
    assert ahosts == hosts

    args["load_balance_hosts"] = "disable"
    ahosts = [att["host"] for att in conninfo_attempts(args)]
    assert ahosts == hosts

    args["load_balance_hosts"] = "random"
    ahosts = [att["host"] for att in conninfo_attempts(args)]
    assert ahosts != hosts
    ahosts.sort()
    assert ahosts == hosts


@pytest.mark.anyio
async def test_conninfo_random_async(fake_resolve):
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


@pytest.mark.parametrize(
    "conninfo, want, env",
    [
        ("", _DEFAULT_CONNECT_TIMEOUT, None),
        ("host=foo", _DEFAULT_CONNECT_TIMEOUT, None),
        ("connect_timeout=-1", _DEFAULT_CONNECT_TIMEOUT, None),
        ("connect_timeout=0", _DEFAULT_CONNECT_TIMEOUT, None),
        ("connect_timeout=1", 2, None),
        ("connect_timeout=10", 10, None),
        ("", 15, {"PGCONNECT_TIMEOUT": "15"}),
    ],
)
def test_timeout(setpgenv, conninfo, want, env):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    timeout = timeout_from_conninfo(params)
    assert timeout == want
