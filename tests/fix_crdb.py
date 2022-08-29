from typing import Optional

import pytest

from .utils import VersionCheck
from psycopg.crdb import CrdbConnection


def pytest_configure(config):
    # register libpq marker
    config.addinivalue_line(
        "markers",
        "crdb(version_expr, reason=detail): run/skip the test with matching CockroachDB"
        " (e.g. '>= 21.2.10', '< 22.1', 'skip < 22')",
    )
    config.addinivalue_line(
        "markers",
        "crdb_skip(reason): skip the test for known CockroachDB reasons",
    )


def check_crdb_version(got, mark):
    if mark.name == "crdb":
        assert len(mark.args) <= 1
        assert not (set(mark.kwargs) - {"reason"})
        spec = mark.args[0] if mark.args else "only"
        reason = mark.kwargs.get("reason")
    elif mark.name == "crdb_skip":
        assert len(mark.args) == 1
        assert not mark.kwargs
        reason = mark.args[0]
        assert reason in _crdb_reasons, reason
        spec = _crdb_reason_version.get(reason, "skip")
    else:
        assert False, mark.name

    pred = VersionCheck.parse(spec)
    pred.whose = "CockroachDB"

    msg = pred.get_skip_message(got)
    if not msg:
        return None

    reason = crdb_skip_message(reason)
    if reason:
        msg = f"{msg}: {reason}"

    return msg


# Utility functions which can be imported in the test suite

is_crdb = CrdbConnection.is_crdb


def crdb_skip_message(reason: Optional[str]) -> str:
    msg = ""
    if reason:
        msg = reason
        if _crdb_reasons.get(reason):
            url = (
                "https://github.com/cockroachdb/cockroach/"
                f"issues/{_crdb_reasons[reason]}"
            )
            msg = f"{msg} ({url})"

    return msg


def skip_crdb(*args, reason=None):
    return pytest.param(*args, marks=pytest.mark.crdb("skip", reason=reason))


def crdb_encoding(*args):
    """Mark tests that fail on CockroachDB because of missing encodings"""
    return skip_crdb(*args, reason="encoding")


def crdb_time_precision(*args):
    """Mark tests that fail on CockroachDB because time doesn't support precision"""
    return skip_crdb(*args, reason="time precision")


def crdb_scs_off(*args):
    return skip_crdb(*args, reason="standard_conforming_strings=off")


# mapping from reason description to ticket number
_crdb_reasons = {
    "2-phase commit": 22329,
    "backend pid": 35897,
    "batch statements": 44803,
    "begin_read_only": 87012,
    "binary decimal": 82492,
    "cancel": 41335,
    "cast adds tz": 51692,
    "cidr": 18846,
    "composite": 27792,
    "copy array": 82792,
    "copy canceled": 81559,
    "copy": 41608,
    "cursor invalid name": 84261,
    "cursor with hold": 77101,
    "deferrable": 48307,
    "do": 17511,
    "encoding": 35882,
    "geometric types": 21286,
    "hstore": 41284,
    "infinity date": 41564,
    "interval style": 35807,
    "json array": 23468,
    "large objects": 243,
    "negative interval": 81577,
    "nested array": 32552,
    "no col query": None,
    "notify": 41522,
    "password_encryption": 42519,
    "pg_terminate_backend": 35897,
    "range": 41282,
    "scroll cursor": 77102,
    "server-side cursor": 41412,
    "severity_nonlocalized": 81794,
    "stored procedure": 1751,
}

_crdb_reason_version = {
    "backend pid": "skip < 22",
    "cancel": "skip < 22",
    "server-side cursor": "skip < 22.1.3",
    "severity_nonlocalized": "skip < 22.1.3",
}
