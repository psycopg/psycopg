import pytest

from psycopg import pq


def stream_param(*vals):
    marks = []
    if vals[0] == "parallel":
        marks = [pytest.mark.pg(">=16")]
    return pytest.param(*vals, id=f"streaming_{vals[0]}", marks=marks)


def tname_param(*vals):
    return pytest.param(*vals, id=type(vals[0]).__name__)


def oname_param(*vals):
    return pytest.param(*vals, id=vals[0].__name__)


def tp_param(*vals):
    return pytest.param(*vals, id=f"two_phase_{'enabled' if vals[0] else 'disabled'}")


def format_param(*vals):
    return pytest.param(*vals, id=pq.Format(vals[0]).name)
    return pytest.param(*vals, id=pq.Format(vals[0]).name)


def repl_class_param(*params):
    return pytest.param(
        *params, id=params[0].__name__.removeprefix("Async").replace("Replication", "")
    )


parametrize_no_decoder = pytest.mark.parametrize(
    "decoder", [pytest.param(None, id="no_decoder")]
)
