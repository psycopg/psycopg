import pytest

from psycopg import rows


def test_tuple_row(conn):
    conn.row_factory = rows.dict_row
    assert conn.execute("select 1 as a").fetchone() == {"a": 1}
    cur = conn.cursor(row_factory=rows.tuple_row)
    row = cur.execute("select 1 as a").fetchone()
    assert row == (1,)
    assert type(row) is tuple
    assert cur._make_row is tuple


def test_dict_row(conn):
    cur = conn.cursor(row_factory=rows.dict_row)
    cur.execute("select 'bob' as name, 3 as id")
    assert cur.fetchall() == [{"name": "bob", "id": 3}]

    cur.execute("select 'a' as letter; select 1 as number")
    assert cur.fetchall() == [{"letter": "a"}]
    assert cur.nextset()
    assert cur.fetchall() == [{"number": 1}]
    assert not cur.nextset()


def test_namedtuple_row(conn):
    rows._make_nt.cache_clear()
    cur = conn.cursor(row_factory=rows.namedtuple_row)
    cur.execute("select 'bob' as name, 3 as id")
    (person1,) = cur.fetchall()
    assert f"{person1.name} {person1.id}" == "bob 3"

    ci1 = rows._make_nt.cache_info()
    assert ci1.hits == 0 and ci1.misses == 1

    cur.execute("select 'alice' as name, 1 as id")
    (person2,) = cur.fetchall()
    assert type(person2) is type(person1)

    ci2 = rows._make_nt.cache_info()
    assert ci2.hits == 1 and ci2.misses == 1

    cur.execute("select 'foo', 1 as id")
    (r0,) = cur.fetchall()
    assert r0.f_column_ == "foo"
    assert r0.id == 1

    cur.execute("select 'a' as letter; select 1 as number")
    (r1,) = cur.fetchall()
    assert r1.letter == "a"
    assert cur.nextset()
    (r2,) = cur.fetchall()
    assert r2.number == 1
    assert not cur.nextset()
    assert type(r1) is not type(r2)


@pytest.mark.parametrize(
    "factory", "tuple_row dict_row namedtuple_row".split()
)
def test_no_result(factory, conn):
    cur = conn.cursor(row_factory=factory_from_name(factory))
    cur.execute("reset search_path")


@pytest.mark.parametrize(
    "factory", "tuple_row dict_row namedtuple_row".split()
)
def test_no_column(factory, conn):
    cur = conn.cursor(row_factory=factory_from_name(factory))
    cur.execute("select")
    recs = cur.fetchall()
    assert len(recs) == 1
    assert not recs[0]


def factory_from_name(name):
    factory = getattr(rows, name)
    return factory
