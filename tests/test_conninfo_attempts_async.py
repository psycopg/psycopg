import sys

import pytest

import psycopg
from psycopg.conninfo import conninfo_attempts_async, conninfo_to_dict

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
            "host=1.1.1.1,2.2.2.2 target_session_attrs=prefer-standby",
            [
                "host=1.1.1.1 hostaddr=1.1.1.1 target_session_attrs=standby",
                "host=2.2.2.2 hostaddr=2.2.2.2 target_session_attrs=standby",
                "host=1.1.1.1 hostaddr=1.1.1.1",
                "host=2.2.2.2 hostaddr=2.2.2.2",
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
        (
            "host=dup.com target_session_attrs=prefer-standby",
            [
                "host=dup.com hostaddr=3.3.3.3 target_session_attrs=standby",
                "host=dup.com hostaddr=3.3.3.4 target_session_attrs=standby",
                "host=dup.com hostaddr=3.3.3.3",
                "host=dup.com hostaddr=3.3.3.4",
            ],
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
    args = {"host": "alots.com"}
    hostaddrs = [str(att["hostaddr"]) for att in await conninfo_attempts_async(args)]
    assert len(hostaddrs) == 20
    assert hostaddrs == sorted(hostaddrs)

    args["load_balance_hosts"] = "disable"
    hostaddrs = [str(att["hostaddr"]) for att in await conninfo_attempts_async(args)]
    assert hostaddrs == sorted(hostaddrs)

    args["load_balance_hosts"] = "random"
    hostaddrs = [str(att["hostaddr"]) for att in await conninfo_attempts_async(args)]
    assert hostaddrs != sorted(hostaddrs)


@pytest.fixture
def service_file(tmp_path, monkeypatch, setpgenv):
    setpgenv({})

    def write(content):
        fn = tmp_path / "pg_service.conf"
        fn.write_text(content)
        monkeypatch.setenv("PGSERVICEFILE", str(fn))

    return write


@pytest.mark.parametrize(
    "conninfo, want",
    [
        (
            "service=multi",
            [
                "service=multi host=1.1.1.1 port=5433 hostaddr=1.1.1.1",
                "service=multi host=2.2.2.2 port=5433 hostaddr=2.2.2.2",
            ],
        ),
        (
            "service=multi host=3.3.3.3",
            ["service=multi host=3.3.3.3 hostaddr=3.3.3.3"],
        ),
        (
            "service=names",
            [
                "service=names host=foo.com hostaddr=1.1.1.1",
                "service=names host=qux.com hostaddr=2.2.2.2",
            ],
        ),
    ],
)
async def test_conninfo_attempts_service(service_file, fake_resolve, conninfo, want):
    service_file(
        "# a comment\n"
        "[multi]\n"
        "host=1.1.1.1,2.2.2.2\n"
        "port=5433\n"
        "\n"
        "[names]\n"
        "host=foo.com,qux.com\n"
    )
    params = conninfo_to_dict(conninfo)
    attempts = await conninfo_attempts_async(params)
    want = list(map(conninfo_to_dict, want))
    assert want == attempts


async def test_conninfo_attempts_service_load_balance(
    service_file, fake_resolve, monkeypatch
):
    service_file("[lb]\nhost=dup.com\nload_balance_hosts=random\n")
    shuffled = []
    mod = sys.modules[conninfo_attempts_async.__module__]
    monkeypatch.setattr(mod, "shuffle", lambda x: shuffled.append(list(x)))
    params = conninfo_to_dict("service=lb")
    await conninfo_attempts_async(params)
    assert shuffled


@pytest.fixture
def service_env(tmp_path, monkeypatch, setpgenv):
    setpgenv({})
    for name in ("PGSERVICEFILE", "PGSYSCONFDIR", "APPDATA", "PGSERVICE"):
        monkeypatch.delenv(name, raising=False)
    return tmp_path


async def test_conninfo_attempts_pgservice_env(service_env, monkeypatch, fake_resolve):
    fn = service_env / "svc.conf"
    fn.write_text("[envsvc]\nhost=1.1.1.1,2.2.2.2\n")
    monkeypatch.setenv("PGSERVICEFILE", str(fn))
    monkeypatch.setenv("PGSERVICE", "envsvc")
    attempts = await conninfo_attempts_async(conninfo_to_dict(""))
    assert [a["hostaddr"] for a in attempts] == ["1.1.1.1", "2.2.2.2"]


async def test_conninfo_attempts_service_system_fallback(
    service_env, monkeypatch, fake_resolve
):
    # Service not in PGSERVICEFILE: libpq tries $PGSYSCONFDIR/pg_service.conf
    user = service_env / "user.conf"
    user.write_text("[other]\nhost=9.9.9.9\n")
    sysdir = service_env / "sys"
    sysdir.mkdir()
    (sysdir / "pg_service.conf").write_text("[sysonly]\nhost=3.3.3.3,4.4.4.4\n")
    monkeypatch.setenv("PGSERVICEFILE", str(user))
    monkeypatch.setenv("PGSYSCONFDIR", str(sysdir))
    attempts = await conninfo_attempts_async(conninfo_to_dict("service=sysonly"))
    assert [a["hostaddr"] for a in attempts] == ["3.3.3.3", "4.4.4.4"]

    # A service found in the first file wins over the system file
    (sysdir / "pg_service.conf").write_text("[other]\nhost=8.8.8.8,7.7.7.7\n")
    attempts = await conninfo_attempts_async(conninfo_to_dict("service=other"))
    assert [a["hostaddr"] for a in attempts] == ["9.9.9.9"]


async def test_conninfo_attempts_service_whitespace(
    service_env, monkeypatch, fake_resolve
):
    # As in libpq: lines are trimmed, key and value are not.
    fn = service_env / "svc.conf"
    lines = ["  [ws]  ", "  host=1.1.1.1,2.2.2.2  ", "[bad", "host=5.5.5.5"]
    fn.write_text("\n".join(lines))
    monkeypatch.setenv("PGSERVICEFILE", str(fn))
    attempts = await conninfo_attempts_async(conninfo_to_dict("service=ws"))
    assert [a["hostaddr"] for a in attempts] == ["1.1.1.1", "2.2.2.2"]


async def test_conninfo_attempts_service_windows_appdata(
    service_env, monkeypatch, fake_resolve
):
    # On Windows libpq reads %APPDATA%\postgresql\.pg_service.conf
    appdata = service_env / "appdata"
    (appdata / "postgresql").mkdir(parents=True)
    (appdata / "postgresql" / ".pg_service.conf").write_text(
        "[win]\nhost=1.1.1.1,2.2.2.2\n"
    )
    monkeypatch.setenv("APPDATA", str(appdata))
    mod = sys.modules["psycopg._conninfo_utils"]
    monkeypatch.setattr(mod, "sys", type("S", (), {"platform": "win32"}))
    attempts = await conninfo_attempts_async(conninfo_to_dict("service=win"))
    assert [a["hostaddr"] for a in attempts] == ["1.1.1.1", "2.2.2.2"]


async def test_conninfo_attempts_service_posix_home(
    service_env, monkeypatch, fake_resolve
):
    # On POSIX libpq reads ~/.pg_service.conf
    (service_env / ".pg_service.conf").write_text("[home]\nhost=1.1.1.1,2.2.2.2\n")
    mod = sys.modules["psycopg._conninfo_utils"]
    monkeypatch.setattr(mod, "sys", type("S", (), {"platform": "linux"}))
    monkeypatch.setattr(mod.os.path, "expanduser", lambda p: str(service_env))
    attempts = await conninfo_attempts_async(conninfo_to_dict("service=home"))
    assert [a["hostaddr"] for a in attempts] == ["1.1.1.1", "2.2.2.2"]
