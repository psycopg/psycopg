import pytest

from .utils import check_version


def pytest_configure(config):
    # register libpq marker
    config.addinivalue_line(
        "markers",
        "crdb(version_expr, reason=detail): run the test only with matching CockroachDB"
        " (e.g. '>= 21.2.10', '< 22.1', 'skip')",
    )


def pytest_runtest_setup(item):
    for m in item.iter_markers(name="crdb"):
        if len(m.args) > 1:
            raise TypeError("max one argument expected")
        kwargs_unk = set(m.kwargs) - {"reason"}
        if kwargs_unk:
            raise TypeError(f"unknown keyword arguments: {kwargs_unk}")

        # Copy the want marker on the function so we can check the version
        # after the connection has been created.
        item.function.want_crdb = m.args[0] if m.args else "only"
        item.function.crdb_reason = m.kwargs.get("reason")


def check_crdb_version(got, func):
    """
    Verify if the CockroachDB version is a version accepted.

    This function is called on the tests marked with something like::

        @pytest.mark.crdb(">= 21.1")
        @pytest.mark.crdb("only")
        @pytest.mark.crdb("skip")

    and skips the test if the server version doesn't match what expected.
    """
    want = func.want_crdb
    rv = None

    if got is None:
        if want == "only":
            return "skipping test: CockroachDB only"
    else:
        if want == "only":
            pass
        elif want == "skip":
            rv = "skipping test: not supported on CockroachDB"
        else:
            rv = check_version(got, want, "CockroachDB")

    if rv:
        if func.crdb_reason:
            rv = f"{rv}: {func.crdb_reason}"
            if func.crdb_reason in crdb_reasons:
                url = (
                    "https://github.com/cockroachdb/cockroach/"
                    f"issues/{crdb_reasons[func.crdb_reason]}"
                )
                rv = f"{rv} ({url})"

    return rv


# Utility functions which can be imported in the test suite


def is_crdb(conn):
    if hasattr(conn, "pgconn"):
        conn = conn.pgconn

    return bool(conn.parameter_status(b"crdb_version"))


def skip_crdb(*args, reason=None):
    return pytest.param(*args, marks=pytest.mark.crdb("skip", reason=reason))


def crdb_encoding(*args):
    """Mark tests that fail on CockroachDB because of missing encodings"""
    return skip_crdb(*args, reason="encoding")


def crdb_time_precision(*args):
    """Mark tests that fail on CockroachDB because time doesn't support precision"""
    return skip_crdb(*args, reason="time precision")


# mapping from reason description to ticket number
crdb_reasons = {
    "2-phase commit": 22329,
    "backend pid": 35897,
    "batch statements": 44803,
    "cancel": 41335,
    "cast adds tz": 51692,
    "cidr": 18846,
    "composite": 27792,
    "copy": 41608,
    "cursor with hold": 77101,
    "deferrable": 48307,
    "do": 17511,
    "encoding": 35882,
    "hstore": 41284,
    "infinity date": 41564,
    "interval style": 35807,
    "large objects": 243,
    "named cursor": 41412,
    "nested array": 32552,
    "notify": 41522,
    "password_encryption": 42519,
    "pg_terminate_backend": 35897,
    "range": 41282,
    "scroll cursor": 77102,
    "stored procedure": 1751,
}
