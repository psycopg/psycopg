# test_sql.py - tests for the psycopg2.sql module

# Copyright (C) 2020 The Psycopg Team

import re
import datetime as dt

import pytest

from psycopg import pq, sql, ProgrammingError
from psycopg.adapt import PyFormat
from psycopg._encodings import py2pgenc
from psycopg.types import TypeInfo
from psycopg.types.string import StrDumper

from .utils import eur
from .fix_crdb import crdb_encoding, crdb_scs_off


@pytest.mark.parametrize(
    "obj, quoted",
    [
        ("foo\\bar", " E'foo\\\\bar'"),
        ("hello", "'hello'"),
        (42, "42"),
        (True, "true"),
        (None, "NULL"),
    ],
)
def test_quote(obj, quoted):
    assert sql.quote(obj) == quoted


@pytest.mark.parametrize("scs", ["on", crdb_scs_off("off")])
def test_quote_roundtrip(conn, scs):
    messages = []
    conn.add_notice_handler(lambda msg: messages.append(msg.message_primary))
    conn.execute(f"set standard_conforming_strings to {scs}")

    for i in range(1, 256):
        want = chr(i)
        quoted = sql.quote(want)
        got = conn.execute(f"select {quoted}::text").fetchone()[0]
        assert want == got

        # No "nonstandard use of \\ in a string literal" warning
        assert not messages, f"error with {want!r}"


@pytest.mark.parametrize("dummy", [crdb_scs_off("off")])
def test_quote_stable_despite_deranged_libpq(conn, dummy):
    # Verify the libpq behaviour of PQescapeString using the last setting seen.
    # Check that we are not affected by it.
    good_str = " E'\\\\'"
    good_bytes = " E'\\\\000'::bytea"
    conn.execute("set standard_conforming_strings to on")
    assert pq.Escaping().escape_string(b"\\") == b"\\"
    assert sql.quote("\\") == good_str
    assert pq.Escaping().escape_bytea(b"\x00") == b"\\000"
    assert sql.quote(b"\x00") == good_bytes

    conn.execute("set standard_conforming_strings to off")
    assert pq.Escaping().escape_string(b"\\") == b"\\\\"
    assert sql.quote("\\") == good_str
    assert pq.Escaping().escape_bytea(b"\x00") == b"\\\\000"
    assert sql.quote(b"\x00") == good_bytes

    # Verify that the good values are actually good
    messages = []
    conn.add_notice_handler(lambda msg: messages.append(msg.message_primary))
    conn.execute("set escape_string_warning to on")
    for scs in ("on", "off"):
        conn.execute(f"set standard_conforming_strings to {scs}")
        cur = conn.execute(f"select {good_str}, {good_bytes}::bytea")
        assert cur.fetchone() == ("\\", b"\x00")

    # No "nonstandard use of \\ in a string literal" warning
    assert not messages


class TestSqlFormat:
    def test_pos(self, conn):
        s = sql.SQL("select {} from {}").format(
            sql.Identifier("field"), sql.Identifier("table")
        )
        s1 = s.as_string(conn)
        assert isinstance(s1, str)
        assert s1 == 'select "field" from "table"'

    def test_pos_spec(self, conn):
        s = sql.SQL("select {0} from {1}").format(
            sql.Identifier("field"), sql.Identifier("table")
        )
        s1 = s.as_string(conn)
        assert isinstance(s1, str)
        assert s1 == 'select "field" from "table"'

        s = sql.SQL("select {1} from {0}").format(
            sql.Identifier("table"), sql.Identifier("field")
        )
        s1 = s.as_string(conn)
        assert isinstance(s1, str)
        assert s1 == 'select "field" from "table"'

    def test_dict(self, conn):
        s = sql.SQL("select {f} from {t}").format(
            f=sql.Identifier("field"), t=sql.Identifier("table")
        )
        s1 = s.as_string(conn)
        assert isinstance(s1, str)
        assert s1 == 'select "field" from "table"'

    def test_compose_literal(self, conn):
        s = sql.SQL("select {0};").format(sql.Literal(dt.date(2016, 12, 31)))
        s1 = s.as_string(conn)
        assert s1 == "select '2016-12-31'::date;"

    def test_compose_empty(self, conn):
        s = sql.SQL("select foo;").format()
        s1 = s.as_string(conn)
        assert s1 == "select foo;"

    def test_percent_escape(self, conn):
        s = sql.SQL("42 % {0}").format(sql.Literal(7))
        s1 = s.as_string(conn)
        assert s1 == "42 % 7"

    def test_braces_escape(self, conn):
        s = sql.SQL("{{{0}}}").format(sql.Literal(7))
        assert s.as_string(conn) == "{7}"
        s = sql.SQL("{{1,{0}}}").format(sql.Literal(7))
        assert s.as_string(conn) == "{1,7}"

    def test_compose_badnargs(self):
        with pytest.raises(IndexError):
            sql.SQL("select {0};").format()

    def test_compose_badnargs_auto(self):
        with pytest.raises(IndexError):
            sql.SQL("select {};").format()
        with pytest.raises(ValueError):
            sql.SQL("select {} {1};").format(10, 20)
        with pytest.raises(ValueError):
            sql.SQL("select {0} {};").format(10, 20)

    def test_compose_bad_args_type(self):
        with pytest.raises(IndexError):
            sql.SQL("select {0};").format(a=10)
        with pytest.raises(KeyError):
            sql.SQL("select {x};").format(10)

    def test_no_modifiers(self):
        with pytest.raises(ValueError):
            sql.SQL("select {a!r};").format(a=10)
        with pytest.raises(ValueError):
            sql.SQL("select {a:<};").format(a=10)

    def test_must_be_adaptable(self, conn):
        class Foo:
            pass

        s = sql.SQL("select {0};").format(sql.Literal(Foo()))
        with pytest.raises(ProgrammingError):
            s.as_string(conn)

    def test_auto_literal(self, conn):
        s = sql.SQL("select {}, {}, {}").format("he'lo", 10, dt.date(2020, 1, 1))
        assert s.as_string(conn) == "select 'he''lo', 10, '2020-01-01'::date"

    def test_execute(self, conn):
        cur = conn.cursor()
        cur.execute(
            """
            create table test_compose (
                id serial primary key,
                foo text, bar text, "ba'z" text)
            """
        )
        cur.execute(
            sql.SQL("insert into {0} (id, {1}) values (%s, {2})").format(
                sql.Identifier("test_compose"),
                sql.SQL(", ").join(map(sql.Identifier, ["foo", "bar", "ba'z"])),
                (sql.Placeholder() * 3).join(", "),
            ),
            (10, "a", "b", "c"),
        )

        cur.execute("select * from test_compose")
        assert cur.fetchall() == [(10, "a", "b", "c")]

    def test_executemany(self, conn):
        cur = conn.cursor()
        cur.execute(
            """
            create table test_compose (
                id serial primary key,
                foo text, bar text, "ba'z" text)
            """
        )
        cur.executemany(
            sql.SQL("insert into {0} (id, {1}) values (%s, {2})").format(
                sql.Identifier("test_compose"),
                sql.SQL(", ").join(map(sql.Identifier, ["foo", "bar", "ba'z"])),
                (sql.Placeholder() * 3).join(", "),
            ),
            [(10, "a", "b", "c"), (20, "d", "e", "f")],
        )

        cur.execute("select * from test_compose")
        assert cur.fetchall() == [(10, "a", "b", "c"), (20, "d", "e", "f")]

    @pytest.mark.crdb_skip("copy")
    def test_copy(self, conn):
        cur = conn.cursor()
        cur.execute(
            """
            create table test_compose (
                id serial primary key,
                foo text, bar text, "ba'z" text)
            """
        )

        with cur.copy(
            sql.SQL("copy {t} (id, foo, bar, {f}) from stdin").format(
                t=sql.Identifier("test_compose"), f=sql.Identifier("ba'z")
            ),
        ) as copy:
            copy.write_row((10, "a", "b", "c"))
            copy.write_row((20, "d", "e", "f"))

        with cur.copy(
            sql.SQL("copy (select {f} from {t} order by id) to stdout").format(
                t=sql.Identifier("test_compose"), f=sql.Identifier("ba'z")
            )
        ) as copy:
            assert list(copy) == [b"c\n", b"f\n"]


class TestIdentifier:
    def test_class(self):
        assert issubclass(sql.Identifier, sql.Composable)

    def test_init(self):
        assert isinstance(sql.Identifier("foo"), sql.Identifier)
        assert isinstance(sql.Identifier("foo"), sql.Identifier)
        assert isinstance(sql.Identifier("foo", "bar", "baz"), sql.Identifier)
        with pytest.raises(TypeError):
            sql.Identifier()
        with pytest.raises(TypeError):
            sql.Identifier(10)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            sql.Identifier(dt.date(2016, 12, 31))  # type: ignore[arg-type]

    def test_repr(self):
        obj = sql.Identifier("fo'o")
        assert repr(obj) == 'Identifier("fo\'o")'
        assert repr(obj) == str(obj)

        obj = sql.Identifier("fo'o", 'ba"r')
        assert repr(obj) == "Identifier(\"fo'o\", 'ba\"r')"
        assert repr(obj) == str(obj)

    def test_eq(self):
        assert sql.Identifier("foo") == sql.Identifier("foo")
        assert sql.Identifier("foo", "bar") == sql.Identifier("foo", "bar")
        assert sql.Identifier("foo") != sql.Identifier("bar")
        assert sql.Identifier("foo") != "foo"
        assert sql.Identifier("foo") != sql.SQL("foo")

    @pytest.mark.parametrize(
        "args, want",
        [
            (("foo",), '"foo"'),
            (("foo", "bar"), '"foo"."bar"'),
            (("fo'o", 'ba"r'), '"fo\'o"."ba""r"'),
        ],
    )
    def test_as_string(self, conn, args, want):
        assert sql.Identifier(*args).as_string(conn) == want

    @pytest.mark.parametrize(
        "args, want, enc",
        [
            crdb_encoding(("foo",), '"foo"', "ascii"),
            crdb_encoding(("foo", "bar"), '"foo"."bar"', "ascii"),
            crdb_encoding(("fo'o", 'ba"r'), '"fo\'o"."ba""r"', "ascii"),
            (("foo", eur), f'"foo"."{eur}"', "utf8"),
            crdb_encoding(("foo", eur), f'"foo"."{eur}"', "latin9"),
        ],
    )
    def test_as_bytes(self, conn, args, want, enc):
        want = want.encode(enc)
        conn.execute(f"set client_encoding to {py2pgenc(enc).decode()}")
        assert sql.Identifier(*args).as_bytes(conn) == want

    def test_join(self):
        assert not hasattr(sql.Identifier("foo"), "join")


class TestLiteral:
    def test_class(self):
        assert issubclass(sql.Literal, sql.Composable)

    def test_init(self):
        assert isinstance(sql.Literal("foo"), sql.Literal)
        assert isinstance(sql.Literal("foo"), sql.Literal)
        assert isinstance(sql.Literal(b"foo"), sql.Literal)
        assert isinstance(sql.Literal(42), sql.Literal)
        assert isinstance(sql.Literal(dt.date(2016, 12, 31)), sql.Literal)

    def test_repr(self):
        assert repr(sql.Literal("foo")) == "Literal('foo')"
        assert str(sql.Literal("foo")) == "Literal('foo')"

    def test_as_string(self, conn):
        assert sql.Literal(None).as_string(conn) == "NULL"
        assert no_e(sql.Literal("foo").as_string(conn)) == "'foo'"
        assert sql.Literal(42).as_string(conn) == "42"
        assert sql.Literal(dt.date(2017, 1, 1)).as_string(conn) == "'2017-01-01'::date"

    def test_as_bytes(self, conn):
        assert sql.Literal(None).as_bytes(conn) == b"NULL"
        assert no_e(sql.Literal("foo").as_bytes(conn)) == b"'foo'"
        assert sql.Literal(42).as_bytes(conn) == b"42"
        assert sql.Literal(dt.date(2017, 1, 1)).as_bytes(conn) == b"'2017-01-01'::date"

    @pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
    def test_as_bytes_encoding(self, conn, encoding):
        conn.execute(f"set client_encoding to {encoding}")
        assert sql.Literal(eur).as_bytes(conn) == f"'{eur}'".encode(encoding)

    def test_eq(self):
        assert sql.Literal("foo") == sql.Literal("foo")
        assert sql.Literal("foo") != sql.Literal("bar")
        assert sql.Literal("foo") != "foo"
        assert sql.Literal("foo") != sql.SQL("foo")

    def test_must_be_adaptable(self, conn):
        class Foo:
            pass

        with pytest.raises(ProgrammingError):
            sql.Literal(Foo()).as_string(conn)

    def test_array(self, conn):
        assert (
            sql.Literal([dt.date(2000, 1, 1)]).as_string(conn)
            == "'{2000-01-01}'::date[]"
        )

    def test_short_name_builtin(self, conn):
        assert sql.Literal(dt.time(0, 0)).as_string(conn) == "'00:00:00'::time"
        assert (
            sql.Literal(dt.datetime(2000, 1, 1)).as_string(conn)
            == "'2000-01-01 00:00:00'::timestamp"
        )
        assert (
            sql.Literal([dt.datetime(2000, 1, 1)]).as_string(conn)
            == "'{\"2000-01-01 00:00:00\"}'::timestamp[]"
        )

    def test_text_literal(self, conn):
        conn.adapters.register_dumper(str, StrDumper)
        assert sql.Literal("foo").as_string(conn) == "'foo'"

    @pytest.mark.crdb_skip("composite")  # create type, actually
    @pytest.mark.parametrize("name", ["a-b", f"{eur}", "order", "foo bar"])
    def test_invalid_name(self, conn, name):
        conn.execute(
            f"""
            set client_encoding to utf8;
            create type "{name}";
            create function invin(cstring) returns "{name}"
                language internal immutable strict as 'textin';
            create function invout("{name}") returns cstring
                language internal immutable strict as 'textout';
            create type "{name}" (input=invin, output=invout, like=text);
            """
        )
        info = TypeInfo.fetch(conn, f'"{name}"')

        class InvDumper(StrDumper):
            oid = info.oid

            def dump(self, obj):
                rv = super().dump(obj)
                return b"%s-inv" % rv

        info.register(conn)
        conn.adapters.register_dumper(str, InvDumper)

        assert sql.Literal("hello").as_string(conn) == f"'hello-inv'::\"{name}\""
        cur = conn.execute(sql.SQL("select {}").format("hello"))
        assert cur.fetchone()[0] == "hello-inv"

        assert (
            sql.Literal(["hello"]).as_string(conn) == f"'{{hello-inv}}'::\"{name}\"[]"
        )
        cur = conn.execute(sql.SQL("select {}").format(["hello"]))
        assert cur.fetchone()[0] == ["hello-inv"]


class TestSQL:
    def test_class(self):
        assert issubclass(sql.SQL, sql.Composable)

    def test_init(self):
        assert isinstance(sql.SQL("foo"), sql.SQL)
        assert isinstance(sql.SQL("foo"), sql.SQL)
        with pytest.raises(TypeError):
            sql.SQL(10)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            sql.SQL(dt.date(2016, 12, 31))  # type: ignore[arg-type]

    def test_repr(self, conn):
        assert repr(sql.SQL("foo")) == "SQL('foo')"
        assert str(sql.SQL("foo")) == "SQL('foo')"
        assert sql.SQL("foo").as_string(conn) == "foo"

    def test_eq(self):
        assert sql.SQL("foo") == sql.SQL("foo")
        assert sql.SQL("foo") != sql.SQL("bar")
        assert sql.SQL("foo") != "foo"
        assert sql.SQL("foo") != sql.Literal("foo")

    def test_sum(self, conn):
        obj = sql.SQL("foo") + sql.SQL("bar")
        assert isinstance(obj, sql.Composed)
        assert obj.as_string(conn) == "foobar"

    def test_sum_inplace(self, conn):
        obj = sql.SQL("f") + sql.SQL("oo")
        obj += sql.SQL("bar")
        assert isinstance(obj, sql.Composed)
        assert obj.as_string(conn) == "foobar"

    def test_multiply(self, conn):
        obj = sql.SQL("foo") * 3
        assert isinstance(obj, sql.Composed)
        assert obj.as_string(conn) == "foofoofoo"

    def test_join(self, conn):
        obj = sql.SQL(", ").join(
            [sql.Identifier("foo"), sql.SQL("bar"), sql.Literal(42)]
        )
        assert isinstance(obj, sql.Composed)
        assert obj.as_string(conn) == '"foo", bar, 42'

        obj = sql.SQL(", ").join(
            sql.Composed([sql.Identifier("foo"), sql.SQL("bar"), sql.Literal(42)])
        )
        assert isinstance(obj, sql.Composed)
        assert obj.as_string(conn) == '"foo", bar, 42'

        obj = sql.SQL(", ").join([])
        assert obj == sql.Composed([])

    def test_as_string(self, conn):
        assert sql.SQL("foo").as_string(conn) == "foo"

    @pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
    def test_as_bytes(self, conn, encoding):
        if encoding:
            conn.execute(f"set client_encoding to {encoding}")

        assert sql.SQL(eur).as_bytes(conn) == eur.encode(encoding)


class TestComposed:
    def test_class(self):
        assert issubclass(sql.Composed, sql.Composable)

    def test_repr(self):
        obj = sql.Composed([sql.Literal("foo"), sql.Identifier("b'ar")])
        assert repr(obj) == """Composed([Literal('foo'), Identifier("b'ar")])"""
        assert str(obj) == repr(obj)

    def test_eq(self):
        L = [sql.Literal("foo"), sql.Identifier("b'ar")]
        l2 = [sql.Literal("foo"), sql.Literal("b'ar")]
        assert sql.Composed(L) == sql.Composed(list(L))
        assert sql.Composed(L) != L
        assert sql.Composed(L) != sql.Composed(l2)

    def test_join(self, conn):
        obj = sql.Composed([sql.Literal("foo"), sql.Identifier("b'ar")])
        obj = obj.join(", ")
        assert isinstance(obj, sql.Composed)
        assert no_e(obj.as_string(conn)) == "'foo', \"b'ar\""

    def test_auto_literal(self, conn):
        obj = sql.Composed(["fo'o", dt.date(2020, 1, 1)])
        obj = obj.join(", ")
        assert isinstance(obj, sql.Composed)
        assert no_e(obj.as_string(conn)) == "'fo''o', '2020-01-01'::date"

    def test_sum(self, conn):
        obj = sql.Composed([sql.SQL("foo ")])
        obj = obj + sql.Literal("bar")
        assert isinstance(obj, sql.Composed)
        assert no_e(obj.as_string(conn)) == "foo 'bar'"

    def test_sum_inplace(self, conn):
        obj = sql.Composed([sql.SQL("foo ")])
        obj += sql.Literal("bar")
        assert isinstance(obj, sql.Composed)
        assert no_e(obj.as_string(conn)) == "foo 'bar'"

        obj = sql.Composed([sql.SQL("foo ")])
        obj += sql.Composed([sql.Literal("bar")])
        assert isinstance(obj, sql.Composed)
        assert no_e(obj.as_string(conn)) == "foo 'bar'"

    def test_iter(self):
        obj = sql.Composed([sql.SQL("foo"), sql.SQL("bar")])
        it = iter(obj)
        i = next(it)
        assert i == sql.SQL("foo")
        i = next(it)
        assert i == sql.SQL("bar")
        with pytest.raises(StopIteration):
            next(it)

    def test_as_string(self, conn):
        obj = sql.Composed([sql.SQL("foo"), sql.SQL("bar")])
        assert obj.as_string(conn) == "foobar"

    def test_as_bytes(self, conn):
        obj = sql.Composed([sql.SQL("foo"), sql.SQL("bar")])
        assert obj.as_bytes(conn) == b"foobar"

    @pytest.mark.parametrize("encoding", ["utf8", crdb_encoding("latin9")])
    def test_as_bytes_encoding(self, conn, encoding):
        obj = sql.Composed([sql.SQL("foo"), sql.SQL(eur)])
        conn.execute(f"set client_encoding to {encoding}")
        assert obj.as_bytes(conn) == ("foo" + eur).encode(encoding)


class TestPlaceholder:
    def test_class(self):
        assert issubclass(sql.Placeholder, sql.Composable)

    @pytest.mark.parametrize("format", PyFormat)
    def test_repr_format(self, conn, format):
        ph = sql.Placeholder(format=format)
        add = f"format={format.name}" if format != PyFormat.AUTO else ""
        assert str(ph) == repr(ph) == f"Placeholder({add})"

    @pytest.mark.parametrize("format", PyFormat)
    def test_repr_name_format(self, conn, format):
        ph = sql.Placeholder("foo", format=format)
        add = f", format={format.name}" if format != PyFormat.AUTO else ""
        assert str(ph) == repr(ph) == f"Placeholder('foo'{add})"

    def test_bad_name(self):
        with pytest.raises(ValueError):
            sql.Placeholder(")")

    def test_eq(self):
        assert sql.Placeholder("foo") == sql.Placeholder("foo")
        assert sql.Placeholder("foo") != sql.Placeholder("bar")
        assert sql.Placeholder("foo") != "foo"
        assert sql.Placeholder() == sql.Placeholder()
        assert sql.Placeholder("foo") != sql.Placeholder()
        assert sql.Placeholder("foo") != sql.Literal("foo")

    @pytest.mark.parametrize("format", PyFormat)
    def test_as_string(self, conn, format):
        ph = sql.Placeholder(format=format)
        assert ph.as_string(conn) == f"%{format.value}"

        ph = sql.Placeholder(name="foo", format=format)
        assert ph.as_string(conn) == f"%(foo){format.value}"

    @pytest.mark.parametrize("format", PyFormat)
    def test_as_bytes(self, conn, format):
        ph = sql.Placeholder(format=format)
        assert ph.as_bytes(conn) == f"%{format.value}".encode("ascii")

        ph = sql.Placeholder(name="foo", format=format)
        assert ph.as_bytes(conn) == f"%(foo){format.value}".encode("ascii")


class TestValues:
    def test_null(self, conn):
        assert isinstance(sql.NULL, sql.SQL)
        assert sql.NULL.as_string(conn) == "NULL"

    def test_default(self, conn):
        assert isinstance(sql.DEFAULT, sql.SQL)
        assert sql.DEFAULT.as_string(conn) == "DEFAULT"


def no_e(s):
    """Drop an eventual E from E'' quotes"""
    if isinstance(s, memoryview):
        s = bytes(s)

    if isinstance(s, str):
        return re.sub(r"\bE'", "'", s)
    elif isinstance(s, bytes):
        return re.sub(rb"\bE'", b"'", s)
    else:
        raise TypeError(f"not dealing with {type(s).__name__}: {s}")
