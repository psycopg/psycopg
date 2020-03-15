#!/usr/bin/env python3


def test_exec_empty(pq, pgconn):
    res = pgconn.exec_(b"")
    assert res.status == pq.ExecStatus.PGRES_EMPTY_QUERY


def test_exec_command(pq, pgconn):
    res = pgconn.exec_("set timezone to utc")
    assert res.status == pq.ExecStatus.PGRES_COMMAND_OK
