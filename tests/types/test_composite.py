import pytest

from psycopg3.adapt import Format


@pytest.mark.parametrize(
    "rec, want",
    [
        ("", ()),
        # Funnily enough there's no way to represent (None,) in Postgres
        ("null", ()),
        ("null,null", (None, None)),
        ("null, ''", (None, "")),
        (
            "42,'foo','ba,r','ba''z','qu\"x'",
            ("42", "foo", "ba,r", "ba'z", 'qu"x'),
        ),
        (
            "'foo''', '''foo', '\"bar', 'bar\"' ",
            ("foo'", "'foo", '"bar', 'bar"'),
        ),
    ],
)
def test_cast_record(conn, want, rec):
    cur = conn.cursor()
    res = cur.execute(f"select row({rec})").fetchone()[0]
    assert res == want


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_cast_all_chars(conn, fmt_out):
    cur = conn.cursor(binary=fmt_out == Format.BINARY)
    for i in range(1, 256):
        res = cur.execute("select row(chr(%s::int))", (i,)).fetchone()[0]
        assert res == (chr(i),)

    cur.execute(
        "select row(%s)" % ",".join(f"chr({i}::int)" for i in range(1, 256))
    )
    res = cur.fetchone()[0]
    assert res == tuple(map(chr, range(1, 256)))

    s = "".join(map(chr, range(1, 256)))
    res = cur.execute("select row(%s)", [s]).fetchone()[0]
    assert res == (s,)


@pytest.mark.parametrize(
    "rec, want",
    [
        ("", ()),
        ("null", (None,)),  # Unlike text format, this is a thing
        ("null,null", (None, None)),
        ("null, ''", (None, b"")),
        (
            "42,'foo','ba,r','ba''z','qu\"x'",
            (42, b"foo", b"ba,r", b"ba'z", b'qu"x'),
        ),
        (
            "'foo''', '''foo', '\"bar', 'bar\"' ",
            (b"foo'", b"'foo", b'"bar', b'bar"'),
        ),
        (
            "10::int, null::text, 20::float,"
            " null::text, 'foo'::text, 'bar'::bytea ",
            (10, None, 20.0, None, "foo", b"bar"),
        ),
    ],
)
def test_cast_record_binary(conn, want, rec):
    cur = conn.cursor(binary=True)
    res = cur.execute(f"select row({rec})").fetchone()[0]
    assert res == want
    for o1, o2 in zip(res, want):
        assert type(o1) is type(o2)
