import pytest

from psycopg import errors as e
from psycopg import pq
from psycopg.adapt import Transformer

try:
    from psycopg._copy_base import _parse_row_binary
except ImportError:  # pragma: no cover
    # psycopg-pool CI runs against older psycopg releases without _copy_base.
    _parse_row_binary = None

try:
    from psycopg_c._psycopg import parse_row_binary
except ImportError:
    parse_row_binary = None


parsers = [
    pytest.param(
        _parse_row_binary,
        id="python",
        marks=pytest.mark.skipif(
            _parse_row_binary is None, reason="_copy_base not available"
        ),
    ),
    pytest.param(
        parse_row_binary,
        id="c",
        marks=pytest.mark.skipif(
            parse_row_binary is None, reason="C implementation not available"
        ),
    ),
]


@pytest.fixture
def tx():
    tx = Transformer()
    tx.set_loader_types([25, 25], pq.Format.TEXT)
    return tx


@pytest.mark.parametrize("parser", parsers)
@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"\x00", id="field-count"),
        pytest.param(b"\x00\x01", id="field-length-missing"),
        pytest.param(b"\x00\x01\x00\x00\x00", id="field-length"),
    ],
)
def test_parse_row_binary_truncated_header(parser, tx, data):
    with pytest.raises(e.DataError, match="bad copy data"):
        parser(data, tx)


@pytest.mark.parametrize("parser", parsers)
def test_parse_row_binary_length_exceeding_data(parser, tx):
    # One field announcing 4 bytes but only 2 present.
    data = b"\x00\x01" + b"\x00\x00\x00\x04" + b"ab"
    with pytest.raises(e.DataError, match="length exceeding data"):
        parser(data, tx)


@pytest.mark.parametrize("parser", parsers)
def test_parse_row_binary_ok(parser, tx):
    data = b"\x00\x02" + b"\x00\x00\x00\x02" + b"ab" + b"\xff\xff\xff\xff"
    assert parser(data, tx) == ("ab", None)
