import pytest
from select import select
import psycopg3
from psycopg3.generators import execute


def test_send_query(pq, pgconn):
    # This test shows how to process an async query in all its glory
    pgconn.nonblocking = 1

    # Long query to make sure we have to wait on send
    pgconn.send_query(
        b"/* %s */ select pg_sleep(0.01); select 1 as foo;"
        % (b"x" * 1_000_000)
    )

    # send loop
    waited_on_send = 0
    while 1:
        f = pgconn.flush()
        if f == 0:
            break

        waited_on_send += 1

        rl, wl, xl = select([pgconn.socket], [pgconn.socket], [])
        assert not (rl and wl)
        if wl:
            continue  # call flush again()
        if rl:
            pgconn.consume_input()
            continue

    # TODO: this check is not reliable, it fails on travis sometimes
    # assert waited_on_send

    # read loop
    results = []
    while 1:
        pgconn.consume_input()
        if pgconn.is_busy():
            select([pgconn.socket], [], [])
            continue
        res = pgconn.get_result()
        if res is None:
            break
        assert res.status == pq.ExecStatus.TUPLES_OK
        results.append(res)

    assert len(results) == 2
    assert results[0].nfields == 1
    assert results[0].fname(0) == b"pg_sleep"
    assert results[0].get_value(0, 0) == b""
    assert results[1].nfields == 1
    assert results[1].fname(0) == b"foo"
    assert results[1].get_value(0, 0) == b"1"


def test_send_query_compact_test(pq, pgconn):
    # Like the above test but use psycopg3 facilities for compactness
    pgconn.send_query(
        b"/* %s */ select pg_sleep(0.01); select 1 as foo;"
        % (b"x" * 1_000_000)
    )
    results = psycopg3.waiting.wait(execute(pgconn))

    assert len(results) == 2
    assert results[0].nfields == 1
    assert results[0].fname(0) == b"pg_sleep"
    assert results[0].get_value(0, 0) == b""
    assert results[1].nfields == 1
    assert results[1].fname(0) == b"foo"
    assert results[1].get_value(0, 0) == b"1"

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.send_query(b"select 1")


def test_send_query_params(pq, pgconn):
    pgconn.send_query_params(b"select $1::int + $2", [b"5", b"3"])
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"8"

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.send_query_params(b"select $1", [b"1"])


def test_send_prepare(pq, pgconn):
    pgconn.send_prepare(b"prep", b"select $1::int + $2::int")
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(b"prep", [b"3", b"5"])
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.get_value(0, 0) == b"8"

    pgconn.finish()
    with pytest.raises(psycopg3.OperationalError):
        pgconn.send_prepare(b"prep", b"select $1::int + $2::int")
    with pytest.raises(psycopg3.OperationalError):
        pgconn.send_query_prepared(b"prep", [b"3", b"5"])


def test_send_prepare_types(pq, pgconn):
    pgconn.send_prepare(b"prep", b"select $1 + $2", [23, 23])
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(b"prep", [b"3", b"5"])
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.get_value(0, 0) == b"8"


def test_send_prepared_binary_in(pq, pgconn):
    val = b"foo\00bar"
    pgconn.send_prepare(b"", b"select length($1::bytea), length($2::bytea)")
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(b"", [val, val], param_formats=[0, 1])
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"3"
    assert res.get_value(0, 1) == b"7"

    with pytest.raises(ValueError):
        pgconn.exec_params(b"select $1::bytea", [val], param_formats=[1, 1])


@pytest.mark.parametrize(
    "fmt, out", [(0, b"\\x666f6f00626172"), (1, b"foo\00bar")]
)
def test_send_prepared_binary_out(pq, pgconn, fmt, out):
    val = b"foo\00bar"
    pgconn.send_prepare(b"", b"select $1::bytea")
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.status == pq.ExecStatus.COMMAND_OK, res.error_message

    pgconn.send_query_prepared(
        b"", [val], param_formats=[1], result_format=fmt
    )
    (res,) = psycopg3.waiting.wait(execute(pgconn))
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == out
