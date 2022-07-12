from copy import deepcopy

import pytest

from psycopg.crdb import adapters, CrdbConnection

from psycopg.adapt import PyFormat, Transformer
from psycopg.types.array import ListDumper
from psycopg.postgres import types as builtins

from ..test_adapt import MyStr, make_dumper, make_bin_dumper
from ..test_adapt import make_loader, make_bin_loader

pytestmark = pytest.mark.crdb


@pytest.mark.parametrize("fmt_in", PyFormat)
def test_return_untyped(conn, fmt_in):
    # Analyze and check for changes using strings in untyped/typed contexts
    cur = conn.cursor()
    # Currently string are passed as text oid to CockroachDB, unlike Postgres,
    # to which strings are passed as unknown. This is because CRDB doesn't
    # allow the unknown oid to be emitted; execute("SELECT %s", ["str"]) raises
    # an error. However, unlike PostgreSQL, text can be cast to any other type.
    cur.execute(f"select %{fmt_in.value}, %{fmt_in.value}", ["hello", 10])
    assert cur.fetchone() == ("hello", 10)

    cur.execute("create table testjson(data jsonb)")
    cur.execute(f"insert into testjson (data) values (%{fmt_in.value})", ["{}"])
    assert cur.execute("select data from testjson").fetchone() == ({},)


def test_str_list_dumper_text(conn):
    t = Transformer(conn)
    dstr = t.get_dumper([""], PyFormat.TEXT)
    assert isinstance(dstr, ListDumper)
    assert dstr.oid == builtins["text"].array_oid
    assert dstr.sub_dumper and dstr.sub_dumper.oid == builtins["text"].oid


@pytest.fixture
def crdb_adapters():
    """Restore the crdb adapters after a test has changed them."""
    dumpers = deepcopy(adapters._dumpers)
    dumpers_by_oid = deepcopy(adapters._dumpers_by_oid)
    loaders = deepcopy(adapters._loaders)
    types = list(adapters.types)

    yield None

    adapters._dumpers = dumpers
    adapters._dumpers_by_oid = dumpers_by_oid
    adapters._loaders = loaders
    adapters.types.clear()
    for t in types:
        adapters.types.add(t)


def test_dump_global_ctx(dsn, crdb_adapters, pgconn):
    adapters.register_dumper(MyStr, make_bin_dumper("gb"))
    adapters.register_dumper(MyStr, make_dumper("gt"))
    with CrdbConnection.connect(dsn) as conn:
        cur = conn.execute("select %s", [MyStr("hello")])
        assert cur.fetchone() == ("hellogt",)
        cur = conn.execute("select %b", [MyStr("hello")])
        assert cur.fetchone() == ("hellogb",)
        cur = conn.execute("select %t", [MyStr("hello")])
        assert cur.fetchone() == ("hellogt",)


def test_load_global_ctx(dsn, crdb_adapters):
    adapters.register_loader("text", make_loader("gt"))
    adapters.register_loader("text", make_bin_loader("gb"))
    with CrdbConnection.connect(dsn) as conn:
        cur = conn.cursor(binary=False).execute("select 'hello'::text")
        assert cur.fetchone() == ("hellogt",)
        cur = conn.cursor(binary=True).execute("select 'hello'::text")
        assert cur.fetchone() == ("hellogb",)
