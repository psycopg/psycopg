import pytest

from psycopg import errors as e
from psycopg import pq
from psycopg.adapt import Transformer
from psycopg._copy_base import parse_row_binary


@pytest.fixture
def tx():
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
def test_parse_row_binary_truncated_header(tx, data):
    with pytest.raises(e.DataError, match="bad copy data"):
        parse_row_binary(data, tx)


def test_parse_row_binary_length_exceeding_data(tx):
    # One field announcing 4 bytes but only 2 present.
    data = b"\x00\x01" + b"\x00\x00\x00\x04" + b"ab"
    with pytest.raises(e.DataError, match="length exceeding data"):
        parse_row_binary(data, tx)


def test_parse_row_binary_ok(tx):
    data = b"\x00\x02" + b"\x00\x00\x00\x02" + b"ab" + b"\xff\xff\xff\xff"
    assert parse_row_binary(data, tx) == ("ab", None)
