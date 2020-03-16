from select import select


def test_send_query(pq, pgconn):
    # This test shows how to process an async query in all its glory
    pgconn.set_non_blocking(1)

    # Long query to make sure we have to wait on send
    pgconn.send_query(
        b"/* %s */ select pg_sleep(0.01); select 1 as foo;"
        % (b"x" * 1_000_000)
    )

    # send loop
    waited_on_send = 0
    while 1:
        f = pgconn.flush()
        assert f != -1
        if f == 0:
            break

        waited_on_send += 1

        rl, wl, xl = select([pgconn.socket], [pgconn.socket], [])
        assert not (rl and wl)
        if wl:
            continue  # call flush again()
        if rl:
            assert pgconn.consume_input() == 1, pgconn.error_message
            continue

    assert waited_on_send

    # read loop
    results = []
    while 1:
        assert pgconn.consume_input() == 1, pgconn.error_message
        if pgconn.is_busy():
            select([pgconn.socket], [], [])
            continue
        res = pgconn.get_result()
        if res is None:
            break
        assert res.status == pq.ExecStatus.PGRES_TUPLES_OK
        results.append(res)

    assert len(results) == 2
    assert results[0].nfields == 1
    assert results[0].fname(0) == b"pg_sleep"
    assert results[0].get_value(0, 0) == b""
    assert results[1].nfields == 1
    assert results[1].fname(0) == b"foo"
    assert results[1].get_value(0, 0) == b"1"
