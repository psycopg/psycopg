import codecs
import pytest

import psycopg
from psycopg import _encodings as encodings


def test_names_normalised():
    for name in encodings._py_codecs.values():
        assert codecs.lookup(name).name == name


@pytest.mark.parametrize(
    "pyenc, pgenc",
    [
        ("ascii", "SQL_ASCII"),
        ("utf8", "UTF8"),
        ("utf-8", "UTF8"),
        ("uTf-8", "UTF8"),
        ("latin9", "LATIN9"),
        ("iso8859-15", "LATIN9"),
    ],
)
def test_py2pg(pyenc, pgenc):
    assert encodings.py2pgenc(pyenc) == pgenc.encode()


@pytest.mark.parametrize(
    "pyenc, pgenc",
    [
        ("ascii", "SQL_ASCII"),
        ("utf-8", "UTF8"),
        ("iso8859-15", "LATIN9"),
    ],
)
def test_pg2py(pyenc, pgenc):
    assert encodings.pg2pyenc(pgenc.encode()) == pyenc


@pytest.mark.parametrize("pgenc", ["MULE_INTERNAL", "EUC_TW"])
def test_pg2py_missing(pgenc):
    with pytest.raises(psycopg.NotSupportedError):
        encodings.pg2pyenc(pgenc.encode())


@pytest.mark.parametrize(
    "conninfo, pyenc",
    [
        ("", "utf-8"),
        ("user=foo, dbname=bar", "utf-8"),
        ("user=foo, dbname=bar, client_encoding=EUC_JP", "euc_jp"),
        ("user=foo, dbname=bar, client_encoding=euc-jp", "euc_jp"),
        ("user=foo, dbname=bar, client_encoding=WAT", "utf-8"),
    ],
)
def test_conninfo_encoding(conninfo, pyenc):
    assert encodings.conninfo_encoding(conninfo) == pyenc
