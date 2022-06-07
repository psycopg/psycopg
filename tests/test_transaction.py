import sys
import logging
from threading import Thread, Event

import pytest

import psycopg
from psycopg import Rollback
from psycopg import errors as e

# TODOCRDB: is this the expected behaviour?
crdb_skip_external_observer = pytest.mark.crdb(
    "skip", reason="deadlock on observer connection"
)


@pytest.fixture
def conn(conn, pipeline):
    return conn


@pytest.fixture(autouse=True)
def create_test_table(svcconn):
    """Creates a table called 'test_table' for use in tests."""
    cur = svcconn.cursor()
    cur.execute("drop table if exists test_table")
    cur.execute("create table test_table (id text primary key)")
    yield
    cur.execute("drop table test_table")


def insert_row(conn, value):
    sql = "INSERT INTO test_table VALUES (%s)"
    if isinstance(conn, psycopg.Connection):
        conn.cursor().execute(sql, (value,))
    else:

        async def f():
            cur = conn.cursor()
            await cur.execute(sql, (value,))

        return f()


def inserted(conn):
    """Return the values inserted in the test table."""
    sql = "SELECT * FROM test_table"
    if isinstance(conn, psycopg.Connection):
        rows = conn.cursor().execute(sql).fetchall()
        return set(v for (v,) in rows)
    else:

        async def f():
            cur = conn.cursor()
            await cur.execute(sql)
            rows = await cur.fetchall()
            return set(v for (v,) in rows)

        return f()


def in_transaction(conn):
    if conn.pgconn.transaction_status == conn.TransactionStatus.IDLE:
        return False
    elif conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS:
        return True
    else:
        assert False, conn.pgconn.transaction_status


def get_exc_info(exc):
    """Return the exc info for an exception or a success if exc is None"""
    if not exc:
        return (None,) * 3
    try:
        raise exc
    except exc:
        return sys.exc_info()


class ExpectedException(Exception):
    pass


def test_basic(conn, pipeline):
    """Basic use of transaction() to BEGIN and COMMIT a transaction."""
    assert not in_transaction(conn)
    with conn.transaction():
        if pipeline:
            pipeline.sync()
        assert in_transaction(conn)
    assert not in_transaction(conn)


def test_exposes_associated_connection(conn):
    """Transaction exposes its connection as a read-only property."""
    with conn.transaction() as tx:
        assert tx.connection is conn
        with pytest.raises(AttributeError):
            tx.connection = conn


def test_exposes_savepoint_name(conn):
    """Transaction exposes its savepoint name as a read-only property."""
    with conn.transaction(savepoint_name="foo") as tx:
        assert tx.savepoint_name == "foo"
        with pytest.raises(AttributeError):
            tx.savepoint_name = "bar"


def test_cant_reenter(conn):
    with conn.transaction() as tx:
        pass

    with pytest.raises(TypeError):
        with tx:
            pass


def test_begins_on_enter(conn, pipeline):
    """Transaction does not begin until __enter__() is called."""
    tx = conn.transaction()
    assert not in_transaction(conn)
    with tx:
        if pipeline:
            pipeline.sync()
        assert in_transaction(conn)
    assert not in_transaction(conn)


def test_commit_on_successful_exit(conn):
    """Changes are committed on successful exit from the `with` block."""
    with conn.transaction():
        insert_row(conn, "foo")

    assert not in_transaction(conn)
    assert inserted(conn) == {"foo"}


def test_rollback_on_exception_exit(conn):
    """Changes are rolled back if an exception escapes the `with` block."""
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "foo")
            raise ExpectedException("This discards the insert")

    assert not in_transaction(conn)
    assert not inserted(conn)


@pytest.mark.crdb_skip("pg_terminate_backend")
def test_context_inerror_rollback_no_clobber(conn_cls, conn, pipeline, dsn, caplog):
    if pipeline:
        # Only 'conn' is possibly in pipeline mode, but the transaction and
        # checks are on 'conn2'.
        pytest.skip("not applicable")
    caplog.set_level(logging.WARNING, logger="psycopg")

    with pytest.raises(ZeroDivisionError):
        with conn_cls.connect(dsn) as conn2:
            with conn2.transaction():
                conn2.execute("select 1")
                conn.execute(
                    "select pg_terminate_backend(%s::int)",
                    [conn2.pgconn.backend_pid],
                )
                1 / 0

    assert len(caplog.records) == 1
    rec = caplog.records[0]
    assert rec.levelno == logging.WARNING
    assert "in rollback" in rec.message


@pytest.mark.crdb_skip("copy")
def test_context_active_rollback_no_clobber(conn_cls, dsn, caplog):
    caplog.set_level(logging.WARNING, logger="psycopg")

    conn = conn_cls.connect(dsn)
    try:
        with pytest.raises(ZeroDivisionError):
            with conn.transaction():
                conn.pgconn.exec_(b"copy (select generate_series(1, 10)) to stdout")
                status = conn.info.transaction_status
                assert status == conn.TransactionStatus.ACTIVE
                1 / 0

        assert len(caplog.records) == 1
        rec = caplog.records[0]
        assert rec.levelno == logging.WARNING
        assert "in rollback" in rec.message
    finally:
        conn.close()


def test_interaction_dbapi_transaction(conn):
    insert_row(conn, "foo")

    with conn.transaction():
        insert_row(conn, "bar")
        raise Rollback

    with conn.transaction():
        insert_row(conn, "baz")

    assert in_transaction(conn)
    conn.commit()
    assert inserted(conn) == {"foo", "baz"}


def test_prohibits_use_of_commit_rollback_autocommit(conn):
    """
    Within a Transaction block, it is forbidden to touch commit, rollback,
    or the autocommit setting on the connection, as this would interfere
    with the transaction scope being managed by the Transaction block.
    """
    conn.autocommit = False
    conn.commit()
    conn.rollback()

    with conn.transaction():
        with pytest.raises(e.ProgrammingError):
            conn.autocommit = False
        with pytest.raises(e.ProgrammingError):
            conn.commit()
        with pytest.raises(e.ProgrammingError):
            conn.rollback()

    conn.autocommit = False
    conn.commit()
    conn.rollback()


@pytest.mark.parametrize("autocommit", [False, True])
def test_preserves_autocommit(conn, autocommit):
    """
    Connection.autocommit is unchanged both during and after Transaction block.
    """
    conn.autocommit = autocommit
    with conn.transaction():
        assert conn.autocommit is autocommit
    assert conn.autocommit is autocommit


def test_autocommit_off_but_no_tx_started_successful_exit(conn, svcconn):
    """
    Scenario:
    * Connection has autocommit off but no transaction has been initiated
      before entering the Transaction context
    * Code exits Transaction context successfully

    Outcome:
    * Changes made within Transaction context are committed
    """
    conn.autocommit = False
    assert not in_transaction(conn)
    with conn.transaction():
        insert_row(conn, "new")
    assert not in_transaction(conn)

    # Changes committed
    assert inserted(conn) == {"new"}
    assert inserted(svcconn) == {"new"}


def test_autocommit_off_but_no_tx_started_exception_exit(conn, svcconn):
    """
    Scenario:
    * Connection has autocommit off but no transaction has been initiated
      before entering the Transaction context
    * Code exits Transaction context with an exception

    Outcome:
    * Changes made within Transaction context are discarded
    """
    conn.autocommit = False
    assert not in_transaction(conn)
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "new")
            raise ExpectedException()
    assert not in_transaction(conn)

    # Changes discarded
    assert not inserted(conn)
    assert not inserted(svcconn)


@crdb_skip_external_observer
def test_autocommit_off_and_tx_in_progress_successful_exit(conn, pipeline, svcconn):
    """
    Scenario:
    * Connection has autocommit off but and a transaction is already in
      progress before entering the Transaction context
    * Code exits Transaction context successfully

    Outcome:
    * Changes made within Transaction context are left intact
    * Outer transaction is left running, and no changes are visible to an
      outside observer from another connection.
    """
    conn.autocommit = False
    insert_row(conn, "prior")
    if pipeline:
        pipeline.sync()
    assert in_transaction(conn)
    with conn.transaction():
        insert_row(conn, "new")
    assert in_transaction(conn)
    assert inserted(conn) == {"prior", "new"}
    # Nothing committed yet; changes not visible on another connection
    assert not inserted(svcconn)


@crdb_skip_external_observer
def test_autocommit_off_and_tx_in_progress_exception_exit(conn, pipeline, svcconn):
    """
    Scenario:
    * Connection has autocommit off but and a transaction is already in
      progress before entering the Transaction context
    * Code exits Transaction context with an exception

    Outcome:
    * Changes made before the Transaction context are left intact
    * Changes made within Transaction context are discarded
    * Outer transaction is left running, and no changes are visible to an
      outside observer from another connection.
    """
    conn.autocommit = False
    insert_row(conn, "prior")
    if pipeline:
        pipeline.sync()
    assert in_transaction(conn)
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "new")
            raise ExpectedException()
    assert in_transaction(conn)
    assert inserted(conn) == {"prior"}
    # Nothing committed yet; changes not visible on another connection
    assert not inserted(svcconn)


def test_nested_all_changes_persisted_on_successful_exit(conn, svcconn):
    """Changes from nested transaction contexts are all persisted on exit."""
    with conn.transaction():
        insert_row(conn, "outer-before")
        with conn.transaction():
            insert_row(conn, "inner")
        insert_row(conn, "outer-after")
    assert not in_transaction(conn)
    assert inserted(conn) == {"outer-before", "inner", "outer-after"}
    assert inserted(svcconn) == {"outer-before", "inner", "outer-after"}


def test_nested_all_changes_discarded_on_outer_exception(conn, svcconn):
    """
    Changes from nested transaction contexts are discarded when an exception
    raised in outer context escapes.
    """
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "outer")
            with conn.transaction():
                insert_row(conn, "inner")
            raise ExpectedException()
    assert not in_transaction(conn)
    assert not inserted(conn)
    assert not inserted(svcconn)


def test_nested_all_changes_discarded_on_inner_exception(conn, svcconn):
    """
    Changes from nested transaction contexts are discarded when an exception
    raised in inner context escapes the outer context.
    """
    with pytest.raises(ExpectedException):
        with conn.transaction():
            insert_row(conn, "outer")
            with conn.transaction():
                insert_row(conn, "inner")
                raise ExpectedException()
    assert not in_transaction(conn)
    assert not inserted(conn)
    assert not inserted(svcconn)


def test_nested_inner_scope_exception_handled_in_outer_scope(conn, svcconn):
    """
    An exception escaping the inner transaction context causes changes made
    within that inner context to be discarded, but the error can then be
    handled in the outer context, allowing changes made in the outer context
    (both before, and after, the inner context) to be successfully committed.
    """
    with conn.transaction():
        insert_row(conn, "outer-before")
        with pytest.raises(ExpectedException):
            with conn.transaction():
                insert_row(conn, "inner")
                raise ExpectedException()
        insert_row(conn, "outer-after")
    assert not in_transaction(conn)
    assert inserted(conn) == {"outer-before", "outer-after"}
    assert inserted(svcconn) == {"outer-before", "outer-after"}


def test_nested_three_levels_successful_exit(conn, svcconn):
    """Exercise management of more than one savepoint."""
    with conn.transaction():  # BEGIN
        insert_row(conn, "one")
        with conn.transaction():  # SAVEPOINT s1
            insert_row(conn, "two")
            with conn.transaction():  # SAVEPOINT s2
                insert_row(conn, "three")
    assert not in_transaction(conn)
    assert inserted(conn) == {"one", "two", "three"}
    assert inserted(svcconn) == {"one", "two", "three"}


def test_named_savepoint_escapes_savepoint_name(conn):
    with conn.transaction("s-1"):
        pass
    with conn.transaction("s1; drop table students"):
        pass


def test_named_savepoints_successful_exit(conn, commands):
    """
    Entering a transaction context will do one of these these things:
    1. Begin an outer transaction (if one isn't already in progress)
    2. Begin an outer transaction and create a savepoint (if one is named)
    3. Create a savepoint (if a transaction is already in progress)
       either using the name provided, or auto-generating a savepoint name.

    ...and exiting the context successfully will "commit" the same.
    """
    # Case 1
    # Using Transaction explicitly because conn.transaction() enters the contetx
    assert not commands
    with conn.transaction() as tx:
        assert commands.popall() == ["BEGIN"]
        assert not tx.savepoint_name
    assert commands.popall() == ["COMMIT"]

    # Case 1 (with a transaction already started)
    conn.cursor().execute("select 1")
    assert commands.popall() == ["BEGIN"]
    with conn.transaction() as tx:
        assert commands.popall() == ['SAVEPOINT "_pg3_1"']
        assert tx.savepoint_name == "_pg3_1"
    assert commands.popall() == ['RELEASE "_pg3_1"']
    conn.rollback()
    assert commands.popall() == ["ROLLBACK"]

    # Case 2
    with conn.transaction(savepoint_name="foo") as tx:
        assert commands.popall() == ["BEGIN", 'SAVEPOINT "foo"']
        assert tx.savepoint_name == "foo"
    assert commands.popall() == ["COMMIT"]

    # Case 3 (with savepoint name provided)
    with conn.transaction():
        assert commands.popall() == ["BEGIN"]
        with conn.transaction(savepoint_name="bar") as tx:
            assert commands.popall() == ['SAVEPOINT "bar"']
            assert tx.savepoint_name == "bar"
        assert commands.popall() == ['RELEASE "bar"']
    assert commands.popall() == ["COMMIT"]

    # Case 3 (with savepoint name auto-generated)
    with conn.transaction():
        assert commands.popall() == ["BEGIN"]
        with conn.transaction() as tx:
            assert commands.popall() == ['SAVEPOINT "_pg3_2"']
            assert tx.savepoint_name == "_pg3_2"
        assert commands.popall() == ['RELEASE "_pg3_2"']
    assert commands.popall() == ["COMMIT"]


def test_named_savepoints_exception_exit(conn, commands):
    """
    Same as the previous test but checks that when exiting the context with an
    exception, whatever transaction and/or savepoint was started on enter will
    be rolled-back as appropriate.
    """
    # Case 1
    with pytest.raises(ExpectedException):
        with conn.transaction() as tx:
            assert commands.popall() == ["BEGIN"]
            assert not tx.savepoint_name
            raise ExpectedException
    assert commands.popall() == ["ROLLBACK"]

    # Case 2
    with pytest.raises(ExpectedException):
        with conn.transaction(savepoint_name="foo") as tx:
            assert commands.popall() == ["BEGIN", 'SAVEPOINT "foo"']
            assert tx.savepoint_name == "foo"
            raise ExpectedException
    assert commands.popall() == ["ROLLBACK"]

    # Case 3 (with savepoint name provided)
    with conn.transaction():
        assert commands.popall() == ["BEGIN"]
        with pytest.raises(ExpectedException):
            with conn.transaction(savepoint_name="bar") as tx:
                assert commands.popall() == ['SAVEPOINT "bar"']
                assert tx.savepoint_name == "bar"
                raise ExpectedException
        assert commands.popall() == ['ROLLBACK TO "bar"', 'RELEASE "bar"']
    assert commands.popall() == ["COMMIT"]

    # Case 3 (with savepoint name auto-generated)
    with conn.transaction():
        assert commands.popall() == ["BEGIN"]
        with pytest.raises(ExpectedException):
            with conn.transaction() as tx:
                assert commands.popall() == ['SAVEPOINT "_pg3_2"']
                assert tx.savepoint_name == "_pg3_2"
                raise ExpectedException
        assert commands.popall() == [
            'ROLLBACK TO "_pg3_2"',
            'RELEASE "_pg3_2"',
        ]
    assert commands.popall() == ["COMMIT"]


def test_named_savepoints_with_repeated_names_works(conn):
    """
    Using the same savepoint name repeatedly works correctly, but bypasses
    some sanity checks.
    """
    # Works correctly if no inner transactions are rolled back
    with conn.transaction(force_rollback=True):
        with conn.transaction("sp"):
            insert_row(conn, "tx1")
            with conn.transaction("sp"):
                insert_row(conn, "tx2")
                with conn.transaction("sp"):
                    insert_row(conn, "tx3")
        assert inserted(conn) == {"tx1", "tx2", "tx3"}

    # Works correctly if one level of inner transaction is rolled back
    with conn.transaction(force_rollback=True):
        with conn.transaction("s1"):
            insert_row(conn, "tx1")
            with conn.transaction("s1", force_rollback=True):
                insert_row(conn, "tx2")
                with conn.transaction("s1"):
                    insert_row(conn, "tx3")
            assert inserted(conn) == {"tx1"}
        assert inserted(conn) == {"tx1"}

    # Works correctly if multiple inner transactions are rolled back
    # (This scenario mandates releasing savepoints after rolling back to them.)
    with conn.transaction(force_rollback=True):
        with conn.transaction("s1"):
            insert_row(conn, "tx1")
            with conn.transaction("s1") as tx2:
                insert_row(conn, "tx2")
                with conn.transaction("s1"):
                    insert_row(conn, "tx3")
                    raise Rollback(tx2)
            assert inserted(conn) == {"tx1"}
        assert inserted(conn) == {"tx1"}


def test_force_rollback_successful_exit(conn, svcconn):
    """
    Transaction started with the force_rollback option enabled discards all
    changes at the end of the context.
    """
    with conn.transaction(force_rollback=True):
        insert_row(conn, "foo")
    assert not inserted(conn)
    assert not inserted(svcconn)


def test_force_rollback_exception_exit(conn, svcconn):
    """
    Transaction started with the force_rollback option enabled discards all
    changes at the end of the context.
    """
    with pytest.raises(ExpectedException):
        with conn.transaction(force_rollback=True):
            insert_row(conn, "foo")
            raise ExpectedException()
    assert not inserted(conn)
    assert not inserted(svcconn)


@crdb_skip_external_observer
def test_explicit_rollback_discards_changes(conn, svcconn):
    """
    Raising a Rollback exception in the middle of a block exits the block and
    discards all changes made within that block.

    You can raise any of the following:
     - Rollback (type)
     - Rollback() (instance)
     - Rollback(tx) (instance initialised with reference to the transaction)
    All of these are equivalent.
    """

    def assert_no_rows():
        assert not inserted(conn)
        assert not inserted(svcconn)

    with conn.transaction():
        insert_row(conn, "foo")
        raise Rollback
    assert_no_rows()

    with conn.transaction():
        insert_row(conn, "foo")
        raise Rollback()
    assert_no_rows()

    with conn.transaction() as tx:
        insert_row(conn, "foo")
        raise Rollback(tx)
    assert_no_rows()


@crdb_skip_external_observer
def test_explicit_rollback_outer_tx_unaffected(conn, svcconn):
    """
    Raising a Rollback exception in the middle of a block does not impact an
    enclosing transaction block.
    """
    with conn.transaction():
        insert_row(conn, "before")
        with conn.transaction():
            insert_row(conn, "during")
            raise Rollback
        assert in_transaction(conn)
        assert not inserted(svcconn)
        insert_row(conn, "after")
    assert inserted(conn) == {"before", "after"}
    assert inserted(svcconn) == {"before", "after"}


def test_explicit_rollback_of_outer_transaction(conn):
    """
    Raising a Rollback exception that references an outer transaction will
    discard all changes from both inner and outer transaction blocks.
    """
    with conn.transaction() as outer_tx:
        insert_row(conn, "outer")
        with conn.transaction():
            insert_row(conn, "inner")
            raise Rollback(outer_tx)
        assert False, "This line of code should be unreachable."
    assert not inserted(conn)


@crdb_skip_external_observer
def test_explicit_rollback_of_enclosing_tx_outer_tx_unaffected(conn, svcconn):
    """
    Rolling-back an enclosing transaction does not impact an outer transaction.
    """
    with conn.transaction():
        insert_row(conn, "outer-before")
        with conn.transaction() as tx_enclosing:
            insert_row(conn, "enclosing")
            with conn.transaction():
                insert_row(conn, "inner")
                raise Rollback(tx_enclosing)
        insert_row(conn, "outer-after")

        assert inserted(conn) == {"outer-before", "outer-after"}
        assert not inserted(svcconn)  # Not yet committed
    # Changes committed
    assert inserted(svcconn) == {"outer-before", "outer-after"}


def test_str(conn, pipeline):
    with conn.transaction() as tx:
        if pipeline:
            assert "[INTRANS, pipeline=ON]" in str(tx)
        else:
            assert "[INTRANS]" in str(tx)
        assert "(active)" in str(tx)
        assert "'" not in str(tx)
        with conn.transaction("wat") as tx2:
            if pipeline:
                assert "[INTRANS, pipeline=ON]" in str(tx2)
            else:
                assert "[INTRANS]" in str(tx2)
            assert "'wat'" in str(tx2)

    if pipeline:
        assert "[IDLE, pipeline=ON]" in str(tx)
    else:
        assert "[IDLE]" in str(tx)
    assert "(terminated)" in str(tx)

    with pytest.raises(ZeroDivisionError):
        with conn.transaction() as tx:
            1 / 0

    assert "(terminated)" in str(tx)


@pytest.mark.parametrize("exit_error", [None, ZeroDivisionError, Rollback])
def test_out_of_order_exit(conn, exit_error):
    conn.autocommit = True

    t1 = conn.transaction()
    t1.__enter__()

    t2 = conn.transaction()
    t2.__enter__()

    with pytest.raises(e.ProgrammingError):
        t1.__exit__(*get_exc_info(exit_error))

    with pytest.raises(e.ProgrammingError):
        t2.__exit__(*get_exc_info(exit_error))


@pytest.mark.parametrize("exit_error", [None, ZeroDivisionError, Rollback])
def test_out_of_order_implicit_begin(conn, exit_error):
    conn.execute("select 1")

    t1 = conn.transaction()
    t1.__enter__()

    t2 = conn.transaction()
    t2.__enter__()

    with pytest.raises(e.ProgrammingError):
        t1.__exit__(*get_exc_info(exit_error))

    with pytest.raises(e.ProgrammingError):
        t2.__exit__(*get_exc_info(exit_error))


@pytest.mark.parametrize("exit_error", [None, ZeroDivisionError, Rollback])
def test_out_of_order_exit_same_name(conn, exit_error):
    conn.autocommit = True

    t1 = conn.transaction("save")
    t1.__enter__()
    t2 = conn.transaction("save")
    t2.__enter__()

    with pytest.raises(e.ProgrammingError):
        t1.__exit__(*get_exc_info(exit_error))

    with pytest.raises(e.ProgrammingError):
        t2.__exit__(*get_exc_info(exit_error))


@pytest.mark.parametrize("what", ["commit", "rollback", "error"])
def test_concurrency(conn, what):
    conn.autocommit = True

    evs = [Event() for i in range(3)]

    def worker(unlock, wait_on):
        with pytest.raises(e.ProgrammingError) as ex:
            with conn.transaction():
                unlock.set()
                wait_on.wait()
                conn.execute("select 1")

                if what == "error":
                    1 / 0
                elif what == "rollback":
                    raise Rollback()
                else:
                    assert what == "commit"

        if what == "error":
            assert "transaction rollback" in str(ex.value)
            assert isinstance(ex.value.__context__, ZeroDivisionError)
        elif what == "rollback":
            assert "transaction rollback" in str(ex.value)
            assert isinstance(ex.value.__context__, Rollback)
        else:
            assert "transaction commit" in str(ex.value)

    # Start a first transaction in a thread
    t1 = Thread(target=worker, kwargs={"unlock": evs[0], "wait_on": evs[1]})
    t1.start()
    evs[0].wait()

    # Start a nested transaction in a thread
    t2 = Thread(target=worker, kwargs={"unlock": evs[1], "wait_on": evs[2]})
    t2.start()

    # Terminate the first transaction before the second does
    t1.join()
    evs[2].set()
    t2.join()
