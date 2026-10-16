"""Exercise generator finalization while another cursor owns the connection."""

import gc
import sys
import json
import threading

import psycopg


def cursor_iter(cursor):
    try:
        while rows := cursor.fetchmany(1):
            yield rows
    finally:
        cursor.close()


def main(dsn, scenario, autocommit, raw):
    gc.disable()
    gc.collect()
    with psycopg.connect(dsn, autocommit=autocommit) as conn:
        if raw:
            conn.server_cursor_factory = psycopg.RawServerCursor
        old = conn.cursor("gc_abandoned", withhold=autocommit)
        old.execute("select generate_series(1, 3)")
        generator = cursor_iter(old)
        assert next(generator) == [(1,)]
        cycle = [generator]
        cycle.append(cycle)
        del generator, cycle
        collected = False

        def collect(diag=None):
            nonlocal collected
            if not collected:
                collected = True
                gc.collect()
                assert old.closed

        if scenario == "row":

            def factory(cursor):
                def make_row(values):
                    collect()
                    return tuple(values)

                return make_row

            with conn.cursor(
                "gc_active", withhold=autocommit, row_factory=factory
            ) as cur:
                cur.execute("select generate_series(10, 13)")
                assert cur.fetchmany(2) == [(10,), (11,)]
                assert cur.fetchall() == [(12,), (13,)]
        elif scenario == "cross_thread":
            requested = threading.Event()
            finished = threading.Event()
            errors = []
            lock = conn.lock

            class ObservedLock:
                def __getattr__(self, name):
                    return getattr(lock, name)

                def __enter__(self):
                    if threading.current_thread() is worker:
                        requested.set()
                    return lock.__enter__()

                def __exit__(self, *exc):
                    return lock.__exit__(*exc)

            setattr(conn, "lock", ObservedLock())

            def close():
                try:
                    old.close()
                except Exception as exc:
                    errors.append(exc)
                finally:
                    finished.set()

            worker = threading.Thread(target=close, daemon=True)
            with conn.lock:
                worker.start()
                assert requested.wait(3)
                assert not finished.is_set()
                assert not old.closed
            worker.join(3)
            assert finished.is_set()
            assert not errors
            collect()
        else:
            conn.add_notice_handler(collect)
            notice = "do $$ begin raise notice 'collect'; end $$"
            if scenario == "query_error":
                conn.autocommit = False
                try:
                    conn.execute(
                        "do $$ begin raise notice 'collect'; "
                        "raise exception 'application failure'; end $$"
                    )
                except psycopg.errors.RaiseException:
                    pass
                else:
                    raise AssertionError("application exception was lost")
                conn.rollback()
            elif scenario.startswith("pipeline"):
                try:
                    with conn.pipeline():
                        conn.execute(notice)
                        query = conn.execute(
                            "select 1 / 0"
                            if scenario == "pipeline_error"
                            else "select 42"
                        )
                except psycopg.errors.DivisionByZero:
                    assert scenario == "pipeline_error"
                else:
                    assert scenario == "pipeline"
                    assert query.fetchone() == (42,)
            else:
                if scenario == "stale_portal":
                    conn.execute("close gc_abandoned")
                conn.execute(notice)

        assert collected and old.closed
        assert not conn.execute(
            "select name from pg_cursors where name = 'gc_abandoned'"
        ).fetchall()
        assert conn.execute("select 42").fetchone() == (42,)


if __name__ == "__main__":
    main(**json.loads(sys.argv[1]))
