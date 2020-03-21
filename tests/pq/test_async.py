from select import select


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

    assert waited_on_send

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


def test_send_query_compact_test(pq, conn):
    # Like the above test but use psycopg3 facilities for compactness
    conn.pgconn.send_query(
        b"/* %s */ select pg_sleep(0.01); select 1 as foo;"
        % (b"x" * 1_000_000)
    )
    results = conn.wait(conn._exec_gen(conn.pgconn))

    assert len(results) == 2
    assert results[0].nfields == 1
    assert results[0].fname(0) == b"pg_sleep"
    assert results[0].get_value(0, 0) == b""
    assert results[1].nfields == 1
    assert results[1].fname(0) == b"foo"
    assert results[1].get_value(0, 0) == b"1"


def test_send_query_params(pq, conn):
    res = conn.pgconn.send_query_params(b"select $1::int + $2", [b"5", b"3"])
    (res,) = conn.wait(conn._exec_gen(conn.pgconn))
    assert res.status == pq.ExecStatus.TUPLES_OK
    assert res.get_value(0, 0) == b"8"
