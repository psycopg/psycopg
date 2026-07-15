import pytest

from psycopg import errors as e
from psycopg import pq
from psycopg.adapt import Transformer
from psycopg._copy_base import _parse_row_binary


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"\x00", id="field-count"),
        pytest.param(b"\x00\x01\x00\x00\x00", id="field-length"),
    ],
)
def test_parse_row_binary_truncated_header(data: bytes) -> None:
    tx = Transformer()
    tx.set_loader_types([25], pq.Format.TEXT)

    with pytest.raises(e.DataError, match="malformed binary copy row"):
        _parse_row_binary(data, tx)
