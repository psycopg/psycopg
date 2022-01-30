import gc
import re
import operator

import pytest


def check_libpq_version(got, want):
    """
    Verify if the libpq version is a version accepted.

    This function is called on the tests marked with something like::

        @pytest.mark.libpq(">= 12")

    and skips the test if the requested version doesn't match what's loaded.
    """
    return _check_version(got, want, "libpq")


def check_server_version(got, want):
    """
    Verify if the server version is a version accepted.

    This function is called on the tests marked with something like::

        @pytest.mark.pg(">= 12")

    and skips the test if the server version doesn't match what expected.
    """
    return _check_version(got, want, "server")


def _check_version(got, want, whose_version):
    # convert 90603 to (9, 6, 3), 120003 to (12, 3)
    got, got_fix = divmod(got, 100)
    got_maj, got_min = divmod(got, 100)
    if got_maj >= 10:
        got = (got_maj, got_fix)
    else:
        got = (got_maj, got_min, got_fix)

    # Parse a spec like "> 9.6"
    m = re.match(r"^\s*(>=|<=|>|<)\s*(?:(\d+)(?:\.(\d+)(?:\.(\d+))?)?)?\s*$", want)
    if m is None:
        pytest.fail(f"bad wanted version spec: {want}")

    # convert "9.6" into (9, 6, 0), "10.3" into (10, 3)
    want_maj = int(m.group(2))
    want_min = int(m.group(3) or "0")
    want_fix = int(m.group(4) or "0")
    if want_maj >= 10:
        if want_fix:
            pytest.fail(f"bad version in {want}")
        want = (want_maj, want_min)
    else:
        want = (want_maj, want_min, want_fix)

    op = getattr(operator, {">=": "ge", "<=": "le", ">": "gt", "<": "lt"}[m.group(1)])

    if not op(got, want):
        revops = {">=": "<", "<=": ">", ">": "<=", "<": ">="}
        return (
            f"skipping test: {whose_version} version is {'.'.join(map(str, got))}"
            f" {revops[m.group(1)]} {'.'.join(map(str, want))}"
        )


def gc_collect():
    """
    gc.collect(), but more insisting.
    """
    for i in range(3):
        gc.collect()
