import pytest

from psycopg import errors as e
from psycopg import pq
from psycopg.adapt import Transformer

try:
    from psycopg._copy_base import parse_row_binary
except ImportError:
    # Allow importing this test module (not to run it) with an old psycopg
    # imjplementation (to run the pool tests)
    pass


def test_copy_binary_parse_length_exceeding_data():
    tx = Transformer()
    # One field declaring length 10, but only two bytes ("ab") present.
    data = bytes.fromhex("00010000000a6162")
    with pytest.raises(e.DataError, match="length exceeding data"):
        parse_row_binary(data, tx)


def test_copy_binary_parse_exact_length():
    tx = Transformer()
    tx.set_loader_types([25], pq.Format.TEXT)  # oid 25 = text
    # One field declaring length 2, with exactly two bytes ("ab") present.
    data = bytes.fromhex("0001000000026162")
    assert parse_row_binary(data, tx) == ("ab",)


@pytest.fixture
def tx2():
    tx = Transformer()
    tx.set_loader_types([25, 25], pq.Format.TEXT)
    return tx


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"\x00", id="field-count"),
        pytest.param(b"\x00\x01", id="field-length-missing"),
        pytest.param(b"\x00\x01\x00\x00\x00", id="field-length"),
    ],
)
def test_parse_row_binary_truncated_header(tx2, data):
    with pytest.raises(e.DataError, match="bad copy data"):
        parse_row_binary(data, tx2)


def test_parse_row_binary_length_exceeding_data(tx2):
    # One field announcing 4 bytes but only 2 present.
    data = b"\x00\x01" + b"\x00\x00\x00\x04" + b"ab"
    with pytest.raises(e.DataError, match="length exceeding data"):
        parse_row_binary(data, tx2)


def test_parse_row_binary_ok(tx2):
    data = b"\x00\x02" + b"\x00\x00\x00\x02" + b"ab" + b"\xff\xff\xff\xff"
    assert parse_row_binary(data, tx2) == ("ab", None)
