import pytest

import psycopg
from psycopg import rows

from .utils import eur


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

    cur.execute(f'select 1 as üåäö, 2 as _, 3 as "123", 4 as "a-b", 5 as "{eur}eur"')
    (r3,) = cur.fetchall()
    assert r3.üåäö == 1
    assert r3.f_ == 2
    assert r3.f123 == 3
    assert r3.a_b == 4
    assert r3.f_eur == 5


def test_class_row(conn):
    cur = conn.cursor(row_factory=rows.class_row(Person))
    cur.execute("select 'John' as first, 'Doe' as last")
    (p,) = cur.fetchall()
    assert isinstance(p, Person)
    assert p.first == "John"
    assert p.last == "Doe"
    assert p.age is None

    for query in (
        "select 'John' as first",
        "select 'John' as first, 'Doe' as last, 42 as wat",
    ):
        cur.execute(query)
        with pytest.raises(TypeError):
            cur.fetchone()


def test_args_row(conn):
    cur = conn.cursor(row_factory=rows.args_row(argf))
    cur.execute("select 'John' as first, 'Doe' as last")
    assert cur.fetchone() == "JohnDoe"


def test_kwargs_row(conn):
    cur = conn.cursor(row_factory=rows.kwargs_row(kwargf))
    cur.execute("select 'John' as first, 'Doe' as last")
    (p,) = cur.fetchall()
    assert isinstance(p, Person)
    assert p.first == "John"
    assert p.last == "Doe"
    assert p.age == 42


@pytest.mark.parametrize(
    "factory",
    "tuple_row dict_row namedtuple_row class_row args_row kwargs_row".split(),
)
def test_no_result(factory, conn):
    cur = conn.cursor(row_factory=factory_from_name(factory))
    cur.execute("reset search_path")
    with pytest.raises(psycopg.ProgrammingError):
        cur.fetchone()


@pytest.mark.crdb_skip("no col query")
@pytest.mark.parametrize(
    "factory", "tuple_row dict_row namedtuple_row args_row".split()
)
def test_no_column(factory, conn):
    cur = conn.cursor(row_factory=factory_from_name(factory))
    cur.execute("select")
    recs = cur.fetchall()
    assert len(recs) == 1
    assert not recs[0]


@pytest.mark.crdb("skip")
def test_no_column_class_row(conn):
    class Empty:
        def __init__(self, x=10, y=20):
            self.x = x
            self.y = y

    cur = conn.cursor(row_factory=rows.class_row(Empty))
    cur.execute("select")
    x = cur.fetchone()
    assert isinstance(x, Empty)
    assert x.x == 10
    assert x.y == 20


def factory_from_name(name):
    factory = getattr(rows, name)
    if factory is rows.class_row:
        factory = factory(Person)
    if factory is rows.args_row:
        factory = factory(argf)
    if factory is rows.kwargs_row:
        factory = factory(argf)

    return factory


class Person:
    def __init__(self, first, last, age=None):
        self.first = first
        self.last = last
        self.age = age


def argf(*args):
    return "".join(map(str, args))


def kwargf(**kwargs):
    return Person(**kwargs, age=42)
