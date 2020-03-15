#!/usr/bin/env python3

import pytest


def test_exec_none(pq, pgconn):
    with pytest.raises(TypeError):
        pgconn.exec_(None)


def test_exec_empty(pq, pgconn):
    res = pgconn.exec_(b"")
    assert res.status == pq.ExecStatus.PGRES_EMPTY_QUERY


def test_exec_command(pq, pgconn):
    res = pgconn.exec_(b"set timezone to utc")
    assert res.status == pq.ExecStatus.PGRES_COMMAND_OK


def test_exec_error(pq, pgconn):
    res = pgconn.exec_(b"wat")
    assert res.status == pq.ExecStatus.PGRES_FATAL_ERROR
