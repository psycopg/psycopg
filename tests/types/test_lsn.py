from __future__ import annotations

from typing import Any

import pytest

import psycopg

try:
    from psycopg.types.catalog import Lsn
except ImportError:
    # Allow importing the test module with Psycopg < 3.4 installed.
    # (e.g. to run pool compatibility tests).
    Lsn = str  # type: ignore


def test_right_module_defined() -> None:
    assert Lsn.__module__ == "psycopg.types.catalog"


def test_from_zero() -> None:
    assert Lsn(0) == "0/0"


def test_bool() -> None:
    assert not Lsn("0/0")
    assert Lsn("0/1")


def test_from_int_roundtrip() -> None:
    for v in [0, 1, 2**32, 2**32 - 1, 0xABCDEF12_3456789A]:
        assert int(Lsn(v)) == v


@pytest.mark.parametrize("base, delta", [("0/1", -1), ("FFFFFFFF/FFFFFFFE", 1)])
def test_overflow(base: str, delta: int) -> None:
    lsn = Lsn(base)
    lsn + delta
    with pytest.raises(OverflowError):
        lsn + (2 * delta)


def test_from_string_uppercase() -> None:
    assert Lsn("A/3B000000") == "A/3B000000"


def test_normalises_to_uppercase() -> None:
    assert Lsn("a/3b000000") == "A/3B000000"


@pytest.mark.parametrize(
    "s",
    ["", "0", "/", "0/", "/0", "0/0/0", "0/100000000", "100000000/0", "0x1/0"]
    + ["1_0/0", " 1/0", "1/0 ", "+1/0", "-0/0", "g/0", "\uff11/0"],
)
def test_invalid_string(s: str) -> None:
    with pytest.raises(ValueError, match="invalid Lsn"):
        Lsn(s)


def test_max_digits() -> None:
    assert int(Lsn("00000001/0000000A")) == 0x1_0000000A
    assert Lsn("00000001/0000000A") == "1/A"


@pytest.mark.parametrize("value", [1.0, None, b"0/0"])
def test_invalid_type(value: Any) -> None:
    with pytest.raises(TypeError):
        Lsn(value)


def test_is_str_subclass() -> None:
    assert isinstance(Lsn(0), str)


def test_str_operations_work() -> None:
    lsn = Lsn("A/3B000000")
    assert lsn.split("/") == ["A", "3B000000"]
    assert f"{lsn}" == "A/3B000000"


def test_repr() -> None:
    lsn = Lsn("A/3B000000")
    assert repr(lsn) == "Lsn(" + repr("A/3B000000") + ")"


def test_addition() -> None:
    result = Lsn("0/100") + 0x100
    assert result == Lsn("0/200")
    assert isinstance(result, Lsn)


def test_addition_reflected() -> None:
    result = 0x100 + Lsn("0/100")
    assert result == Lsn("0/200")
    assert isinstance(result, Lsn)


@pytest.mark.parametrize("other", [Lsn("0/1"), "0/1", 1.0])
def test_addition_bad_type(other: Any) -> None:
    with pytest.raises(TypeError):
        Lsn("0/100") + other


def test_subtraction() -> None:
    result = Lsn("0/200") - Lsn("0/100")
    assert result == 0x100
    assert type(result) is int
    assert Lsn("0/100") - Lsn("0/200") == -0x100


def test_subtraction_int() -> None:
    result = Lsn("0/200") - 0x100
    assert result == Lsn("0/100")
    assert isinstance(result, Lsn)


@pytest.mark.parametrize("other", ["0/1", 1.0])
def test_subtraction_bad_type(other: Any) -> None:
    with pytest.raises(TypeError):
        Lsn("0/100") - other
    with pytest.raises(TypeError):
        other - Lsn("0/100")


def test_ordering_is_numeric_not_lexical() -> None:
    # "F/0" < "10/0" numerically (0xF < 0x10) but "F" > "1" lexically
    lsn_f = Lsn("F/0")
    lsn_10 = Lsn("10/0")
    assert lsn_f < lsn_10
    assert lsn_10 > lsn_f
    assert lsn_f <= lsn_10
    assert lsn_f <= lsn_f
    assert lsn_10 >= lsn_10


def test_ordering_with_str() -> None:
    assert Lsn("F/0") < "10/0"
    assert "10/0" > Lsn("F/0")
    assert Lsn("A/0") <= "a/0"
    assert Lsn("A/0") >= "a/0"


def test_ordering_with_invalid_str() -> None:
    with pytest.raises(ValueError, match="invalid Lsn"):
        Lsn("0/0") < "foo"


def test_ordering_with_int() -> None:
    with pytest.raises(TypeError):
        Lsn("0/0") < 1


def test_equality() -> None:
    assert Lsn("A/0") == Lsn("A/0")
    assert Lsn("A/0") == "A/0"
    assert "A/0" == Lsn("A/0")
    assert Lsn("A/0") != Lsn("B/0")
    assert not (Lsn("A/0") != "A/0")


def test_equality_normalised() -> None:
    assert Lsn("A/B") == "a/b"
    assert "a/b" == Lsn("A/B")
    assert Lsn("A/B") == "0000000A/0000000B"
    assert not (Lsn("A/B") != "a/b")


def test_equality_other_types() -> None:
    assert Lsn("0/A") != "foo"
    assert Lsn("0/A") != 10


def test_hash() -> None:
    assert hash(Lsn("a/b")) == hash("A/B")
    d: dict[str, int] = {Lsn("A/B"): 1}
    assert d["A/B"] == 1
    assert d[Lsn("a/b")] == 1


def test_lsn_text_loader_registered() -> None:
    text_loaders = psycopg.adapters._loaders[0]
    assert 3220 in text_loaders, "pg_lsn text loader not registered"


def test_lsn_binary_loader_registered() -> None:
    binary_loaders = psycopg.adapters._loaders[1]
    assert 3220 in binary_loaders, "pg_lsn binary loader not registered"


def test_lsn_text_dumper_registered() -> None:
    text_dumpers = psycopg.adapters._dumpers[psycopg.adapt.PyFormat.TEXT]
    assert Lsn in text_dumpers, "Lsn text dumper not registered"


def test_lsn_binary_dumper_registered() -> None:
    binary_dumpers = psycopg.adapters._dumpers[psycopg.adapt.PyFormat.BINARY]
    assert Lsn in binary_dumpers, "Lsn binary dumper not registered"


@pytest.mark.crdb_skip("pg_current_wal_lsn")
@pytest.mark.parametrize("format", psycopg.pq.Format)
def test_lsn_type_from_db(
    conn: psycopg.Connection[Any], format: psycopg.pq.Format
) -> None:
    row = conn.execute("SELECT pg_current_wal_lsn()", binary=bool(format)).fetchone()
    assert row is not None
    lsn = row[0]
    assert isinstance(lsn, Lsn), f"expected Lsn, got {type(lsn)}"
    assert isinstance(lsn, str), "Lsn must be str subclass"
    assert "/" in lsn


@pytest.mark.parametrize("format", psycopg.pq.Format)
def test_lsn_roundtrip_as_parameter(
    conn: psycopg.Connection[Any], format: psycopg.pq.Format
) -> None:
    lsn_in = Lsn("AB/12345678")
    row = conn.execute("SELECT %s", [lsn_in], binary=bool(format)).fetchone()
    assert row is not None
    lsn_out = row[0]
    assert isinstance(lsn_out, Lsn), f"expected Lsn, got {type(lsn_out)}"
    assert lsn_out == lsn_in
