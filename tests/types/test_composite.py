import pytest


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


def test_cast_all_chars(conn):
    cur = conn.cursor()
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
