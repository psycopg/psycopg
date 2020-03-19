import pytest

from psycopg3.conninfo import make_conninfo, conninfo_to_dict
from psycopg3 import ProgrammingError


@pytest.mark.parametrize(
    "conninfo, kwargs, exp",
    [
        ("", {}, ""),
        ("dbname=foo", {}, "dbname=foo"),
        ("dbname=foo", {"user": "bar"}, "dbname=foo user=bar"),
        ("dbname=foo", {"dbname": "bar"}, "dbname=bar"),
        ("user=bar", {"dbname": "foo bar"}, "dbname='foo bar' user=bar"),
        ("", {"dbname": "foo"}, "dbname=foo"),
        ("", {"dbname": "foo", "user": None}, "dbname=foo"),
        ("", {"dbname": "a'b"}, r"dbname='a\'b'"),
    ],
)
def test_make_conninfo(conninfo, kwargs, exp):
    out = make_conninfo(conninfo, **kwargs)
    assert conninfo_to_dict(out) == conninfo_to_dict(exp)


@pytest.mark.parametrize(
    "conninfo, kwargs",
    [("dbname=foo bar", {}), ("foo=bar", {}), ("dbname=foo", {"bar": "baz"})],
)
def test_make_conninfo_bad(conninfo, kwargs):
    with pytest.raises(ProgrammingError):
        make_conninfo(conninfo, **kwargs)


@pytest.mark.parametrize(
    "conninfo, exp",
    [
        ("", {}),
        ("dbname=foo user=bar", {"dbname": "foo", "user": "bar"}),
        ("dbname='foo bar'", {"dbname": "foo bar"}),
        (r"dbname='a\'b'", {"dbname": "a'b"}),
    ],
)
def test_conninfo_to_dict(conninfo, exp):
    assert conninfo_to_dict(conninfo) == exp
