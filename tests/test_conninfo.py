import pytest

from psycopg3.conninfo import make_conninfo, conninfo_to_dict
from psycopg3 import ProgrammingError

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
