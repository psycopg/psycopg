from __future__ import annotations

import random
from typing import TYPE_CHECKING
from collections import Counter

import pytest

import psycopg
from psycopg.conninfo import conninfo_to_dict

from .test_dns import import_dnspython

if TYPE_CHECKING:
    import psycopg._dns

pytestmark = [pytest.mark.dns]

samples_ok = [
    ("", "", None),
    ("host=_pg._tcp.foo.com", "host=db1.example.com port=5432", None),
    ("", "host=db1.example.com port=5432", {"PGHOST": "_pg._tcp.foo.com"}),
    (
        "host=foo.com,_pg._tcp.foo.com",
        "host=foo.com,db1.example.com port=,5432",
        None,
    ),
    (
        "host=_pg._tcp.dot.com,foo.com,_pg._tcp.foo.com",
        "host=foo.com,db1.example.com port=,5432",
        None,
    ),
    (
        "host=_pg._tcp.bar.com",
        "host=db1.example.com,db4.example.com,db3.example.com,db2.example.com"
        " port=5432,5432,5433,5432",
        None,
    ),
    (
        "host=service.foo.com port=srv",
        "host=service.example.com port=15432",
        None,
    ),
    # No resolution
    (
        "host=_pg._tcp.foo.com hostaddr=1.1.1.1",
        "host=_pg._tcp.foo.com hostaddr=1.1.1.1",
        None,
    ),
]


@pytest.mark.parametrize("conninfo, want, env", samples_ok)
def test_srv(conninfo, want, env, fake_srv, setpgenv):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    params = psycopg._dns.resolve_srv(params)
    assert conninfo_to_dict(want) == params


@pytest.mark.anyio
@pytest.mark.parametrize("conninfo, want, env", samples_ok)
async def test_srv_async(conninfo, want, env, afake_srv, setpgenv):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    params = await psycopg._dns.resolve_srv_async(params)
    assert conninfo_to_dict(want) == params


samples_bad = [
    ("host=_pg._tcp.dot.com", None),
    ("host=_pg._tcp.foo.com port=1,2", None),
]


@pytest.mark.parametrize("conninfo,  env", samples_bad)
def test_srv_bad(conninfo, env, fake_srv, setpgenv):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    with pytest.raises(psycopg.OperationalError):
        psycopg._dns.resolve_srv(params)


@pytest.mark.anyio
@pytest.mark.parametrize("conninfo,  env", samples_bad)
async def test_srv_bad_async(conninfo, env, afake_srv, setpgenv):
    setpgenv(env)
    params = conninfo_to_dict(conninfo)
    with pytest.raises(psycopg.OperationalError):
        await psycopg._dns.resolve_srv_async(params)


def make_srvs(*entries):
    import_dnspython()
    from dns.rdtypes.IN.SRV import SRV

    return [
        SRV("IN", "SRV", pri, weight, 5432, f"{name}.example.com.")
        for name, pri, weight in entries
    ]


# Entries with priority 0 are shuffled; z, the only one with priority 1, goes last.
weighted_srvs = [("z", 1, 5), ("c", 0, 20), ("a", 0, 0), ("b", 0, 10)]


@pytest.mark.parametrize(
    "draws, totals, want",
    [
        # Sorted by weight, the running sums are a: 0, b: 10, c: 30.
        ([0, 10, 20], [30, 30, 20], "abcz"),
        ([1, 0, 0], [30, 20, 20], "bacz"),
        ([10, 11, 0], [30, 20, 0], "bcaz"),
        ([11, 0, 0], [30, 10, 10], "cabz"),
        ([30, 10, 0], [30, 10, 0], "cbaz"),
    ],
)
def test_sort_rfc2782(draws, totals, want, monkeypatch):
    srvs = make_srvs(*weighted_srvs)
    calls = []

    def fake_randint(a, b):
        calls.append((a, b))
        return draws[len(calls) - 1]

    monkeypatch.setattr(psycopg._dns, "randint", fake_randint)
    resolver = psycopg._dns.Rfc2782Resolver()
    rv = resolver.sort_rfc2782(srvs)
    assert "".join(str(ent.target)[0] for ent in rv) == want
    assert calls == [(0, t) for t in totals]


def test_sort_rfc2782_distribution(monkeypatch):
    srvs = make_srvs(*weighted_srvs)
    monkeypatch.setattr(psycopg._dns, "randint", random.Random(42).randint)
    resolver = psycopg._dns.Rfc2782Resolver()
    n = 3000
    firsts = Counter(str(resolver.sort_rfc2782(srvs)[0].target)[0] for _ in range(n))
    # randint(0, 30): a is picked only on 0, then b on 1-10 and c on 11-30.
    assert firsts["a"] / n == pytest.approx(1 / 31, abs=0.02)
    assert firsts["b"] / n == pytest.approx(10 / 31, abs=0.03)
    assert firsts["c"] / n == pytest.approx(20 / 31, abs=0.03)


@pytest.fixture
def fake_srv(monkeypatch):
    f = get_fake_srv_function(monkeypatch)
    monkeypatch.setattr(
        psycopg._dns.resolver,
        "resolve",
        f,
    )


@pytest.fixture
def afake_srv(monkeypatch):
    f = get_fake_srv_function(monkeypatch)

    async def af(qname, rdtype):
        return f(qname, rdtype)

    monkeypatch.setattr(
        psycopg._dns.async_resolver,
        "resolve",
        af,
    )


def get_fake_srv_function(monkeypatch):
    import_dnspython()

    from dns.exception import DNSException
    from dns.rdtypes.IN.A import A
    from dns.rdtypes.IN.SRV import SRV

    # Make the RFC 2782 weighted shuffle deterministic: always drawing the
    # top of the range selects the heaviest remaining entry.
    monkeypatch.setattr(psycopg._dns, "randint", lambda a, b: b)

    fake_hosts = {
        ("_pg._tcp.dot.com", "SRV"): ["0 0 5432 ."],
        ("_pg._tcp.foo.com", "SRV"): ["0 0 5432 db1.example.com."],
        ("_pg._tcp.bar.com", "SRV"): [
            "1 0 5432 db2.example.com.",
            "1 255 5433 db3.example.com.",
            "0 0 5432 db1.example.com.",
            "1 65535 5432 db4.example.com.",
        ],
        ("service.foo.com", "SRV"): ["0 0 15432 service.example.com."],
    }

    def fake_srv_(qname, rdtype):
        try:
            ans = fake_hosts[qname, rdtype]
        except KeyError:
            raise DNSException(f"unknown test host: {qname} {rdtype}")
        rv: list[A | SRV] = []

        if rdtype == "A":
            for entry in ans:
                rv.append(A("IN", "A", entry))
        else:
            for entry in ans:
                pri, w, port, target = entry.split()
                rv.append(SRV("IN", "SRV", int(pri), int(w), int(port), target))

        return rv

    return fake_srv_
