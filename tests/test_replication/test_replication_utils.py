from __future__ import annotations

import pytest

from psycopg.replication.replication_utils import lsn_to_string, string_to_lsn


class TestLsnToString:
    def test_zero(self):
        assert lsn_to_string(0) == "0/0"

    def test_small_value(self):
        result = lsn_to_string(1)
        assert "/" in result

    @pytest.mark.parametrize(
        "lsn_str", ["0/1", "0/FFFFFFFF", "1/0", "FF/FF", "12345678/ABCDEF01"]
    )
    def test_round_trip(self, lsn_str):
        assert lsn_to_string(string_to_lsn(lsn_str)) == lsn_str

    def test_known_value(self):
        lsn = 0x0000000100000000
        assert lsn_to_string(lsn) == "1/0"

    def test_high_and_low(self):
        lsn = 0x0000000200000003
        assert lsn_to_string(lsn) == "2/3"

    def test_max_low(self):
        lsn = 0xFFFFFFFF
        result = lsn_to_string(lsn)
        assert result == "0/FFFFFFFF"

    def test_large_value_round_trip(self):
        lsn = (0xAB << 32) | 0xCD
        s = lsn_to_string(lsn)
        assert string_to_lsn(s) == lsn


class TestStringToLsn:
    def test_zero_string(self):
        assert string_to_lsn("0/0") == 0

    def test_simple_value(self):
        assert string_to_lsn("0/1") == 1

    def test_high_part(self):
        assert string_to_lsn("1/0") == 0x0000000100000000

    def test_both_parts(self):
        assert string_to_lsn("2/3") == 0x0000000200000003

    def test_hex_uppercase(self):
        assert string_to_lsn("A/B") == 0x0000000A0000000B

    def test_hex_lowercase(self):
        assert string_to_lsn("a/b") == string_to_lsn("A/B")

    @pytest.mark.parametrize(
        "not_an_lsn", ["0/1/2", "0/FFFFHFFF", "12", "/FF", "12345678/"]
    )
    def test_invalid_format_raises(self, not_an_lsn):
        with pytest.raises(ValueError):
            string_to_lsn(not_an_lsn)

    @pytest.mark.parametrize(
        "lsn_int",
        [
            0,
            1,
            0xFFFFFFFF,
            (1 << 32),
            (0xFF << 32) | 0xFF,
            (0x12345678 << 32) | 0xABCDEF01,
        ],
    )
    def test_round_trip(self, lsn_int):
        assert string_to_lsn(lsn_to_string(lsn_int)) == lsn_int
