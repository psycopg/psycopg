import sys

import pytest

from psycopg3 import Connection, ProgrammingError, Rollback
from psycopg3.sql import Composable
from psycopg3.transaction import Transaction


@pytest.fixture(autouse=True)
def create_test_table(svcconn):
    """
    Creates a table called 'test_table' for use in tests.
    """
    cur = svcconn.cursor()
    cur.execute("drop table if exists test_table")
    cur.execute("create table test_table (id text primary key)")
    yield
    cur.execute("drop table test_table")


def insert_row(conn, value):
    conn.cursor().execute("INSERT INTO test_table VALUES (%s)", (value,))


def inserted(conn):
    sql = "SELECT * FROM test_table"
    if isinstance(conn, Connection):
        rows = conn.cursor().execute(sql).fetchall()
        return set(v for (v,) in rows)
    else:

        async def f():
            cur = await conn.cursor()
            await cur.execute(sql)
            rows = await cur.fetchall()
            return set(v for (v,) in rows)

        return f()


@pytest.fixture
def commands(monkeypatch):
    """The queue of commands issued internally by connections.

    Not concurrency safe as it mokkeypatches a class method, but good enough
    for tests.
    """
    _orig_exec_command = Connection._exec_command
    L = []

    def _exec_command(self, command):
        if isinstance(command, bytes):
            command = command.decode(self.client_encoding)
        elif isinstance(command, Composable):
            command = command.as_string(self)

        L.insert(0, command)
        _orig_exec_command(self, command)

    monkeypatch.setattr(Connection, "_exec_command", _exec_command)
    yield L


def in_transaction(conn):
    if conn.pgconn.transaction_status == conn.TransactionStatus.IDLE:
        return False
    elif conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS:
        return True
    else:
        assert False, conn.pgconn.transaction_status


class ExpectedException(Exception):
    pass


def some_exc_info():
    try:
        raise ExpectedException()
    except ExpectedException:
        return sys.exc_info()


def test_basic(conn):
    """Basic use of transaction() to BEGIN and COMMIT a transaction."""
    assert not in_transaction(conn)
    with conn.transaction():
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


def test_begins_on_enter(conn):
    """Transaction does not begin until __enter__() is called."""
    tx = conn.transaction()
    assert not in_transaction(conn)
    with tx:
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
        with pytest.raises(ProgrammingError):
            conn.autocommit = False
        with pytest.raises(ProgrammingError):
            conn.commit()
        with pytest.raises(ProgrammingError):
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


def test_autocommit_off_and_tx_in_progress_successful_exit(conn, svcconn):
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
    assert in_transaction(conn)
    with conn.transaction():
        insert_row(conn, "new")
    assert in_transaction(conn)
    assert inserted(conn) == {"prior", "new"}
    # Nothing committed yet; changes not visible on another connection
    assert not inserted(svcconn)


def test_autocommit_off_and_tx_in_progress_exception_exit(conn, svcconn):
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
    # Using Transaction explicitly becase conn.transaction() enters the contetx
    tx = Transaction(conn)
    assert not commands
    tx.__enter__()
    assert commands.pop() == "begin"
    assert not tx.savepoint_name
    tx.__exit__(None, None, None)
    assert commands.pop() == "commit"

    # Case 2
    tx = Transaction(conn, savepoint_name="foo")
    tx.__enter__()
    assert commands.pop() == 'begin; savepoint "foo"'
    assert tx.savepoint_name == "foo"
    tx.__exit__(None, None, None)
    assert commands.pop() == "commit"

    # Case 3 (with savepoint name provided)
    with conn.transaction():
        assert commands.pop() == "begin"
        tx = Transaction(conn, savepoint_name="bar")
        tx.__enter__()
        assert commands.pop() == 'savepoint "bar"'
        assert tx.savepoint_name == "bar"
        tx.__exit__(None, None, None)
        assert commands.pop() == 'release savepoint "bar"'
        assert not commands
    assert commands.pop() == "commit"

    # Case 3 (with savepoint name auto-generated)
    with conn.transaction():
        assert commands.pop() == "begin"
        tx = Transaction(conn)
        tx.__enter__()
        assert commands.pop() == 'savepoint "s1"'
        assert tx.savepoint_name == "s1"
        tx.__exit__(None, None, None)
        assert commands.pop() == 'release savepoint "s1"'
        assert not commands
    assert commands.pop() == "commit"

    assert not commands


def test_named_savepoints_exception_exit(conn, commands):
    """
    Same as the previous test but checks that when exiting the context with an
    exception, whatever transaction and/or savepoint was started on enter will
    be rolled-back as appropriate.
    """
    # Case 1
    tx = Transaction(conn)
    tx.__enter__()
    assert commands.pop() == "begin"
    assert not tx.savepoint_name
    tx.__exit__(*some_exc_info())
    assert commands.pop() == "rollback"

    # Case 2
    tx = Transaction(conn, savepoint_name="foo")
    tx.__enter__()
    assert commands.pop() == 'begin; savepoint "foo"'
    assert tx.savepoint_name == "foo"
    tx.__exit__(*some_exc_info())
    assert commands.pop() == "rollback"

    # Case 3 (with savepoint name provided)
    with conn.transaction():
        assert commands.pop() == "begin"
        tx = Transaction(conn, savepoint_name="bar")
        tx.__enter__()
        assert commands.pop() == 'savepoint "bar"'
        assert tx.savepoint_name == "bar"
        tx.__exit__(*some_exc_info())
        assert (
            commands.pop()
            == 'rollback to savepoint "bar"; release savepoint "bar"'
        )
        assert not commands
    assert commands.pop() == "commit"

    # Case 3 (with savepoint name auto-generated)
    with conn.transaction():
        assert commands.pop() == "begin"
        tx = Transaction(conn)
        tx.__enter__()
        assert commands.pop() == 'savepoint "s1"'
        assert tx.savepoint_name == "s1"
        tx.__exit__(*some_exc_info())
        assert (
            commands.pop()
            == 'rollback to savepoint "s1"; release savepoint "s1"'
        )
        assert not commands
    assert commands.pop() == "commit"

    assert not commands


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

    # Will not (always) catch out-of-order exits
    with conn.transaction(force_rollback=True):
        tx1 = conn.transaction("s1")
        tx2 = conn.transaction("s1")
        tx1.__enter__()
        tx2.__enter__()
        tx1.__exit__(None, None, None)
        tx2.__exit__(None, None, None)


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


@pytest.mark.parametrize("exc_info", [(None, None, None), some_exc_info()])
@pytest.mark.parametrize("name", [None, "s1"])
def test_manual_exit_without_enter_asserts(conn, name, exc_info):
    """
    When user is calling __enter__() and __exit__() manually for some reason,
    provide a helpful error message if they call __exit__() without first
    having called __enter__()
    """
    tx = Transaction(conn, name)
    with pytest.raises(ProgrammingError, match="Out-of-order"):
        tx.__exit__(*exc_info)


@pytest.mark.parametrize("exc_info", [(None, None, None), some_exc_info()])
@pytest.mark.parametrize("name", [None, "s1"])
def test_manual_exit_twice_asserts(conn, name, exc_info):
    """
    When user is calling __enter__() and __exit__() manually for some reason,
    provide a helpful error message if they accidentally call __exit__() twice.
    """
    tx = Transaction(conn, name)
    tx.__enter__()
    tx.__exit__(*exc_info)
    with pytest.raises(ProgrammingError, match="Out-of-order"):
        tx.__exit__(*exc_info)
