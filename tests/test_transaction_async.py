import pytest

from psycopg3 import ProgrammingError, Rollback

from .test_transaction import in_transaction, insert_row, inserted
from .test_transaction import ExpectedException
from .test_transaction import create_test_table  # noqa  # autouse fixture

pytestmark = pytest.mark.asyncio


async def test_basic(aconn):
    """Basic use of transaction() to BEGIN and COMMIT a transaction."""
    assert not in_transaction(aconn)
    async with aconn.transaction():
        assert in_transaction(aconn)
    assert not in_transaction(aconn)


async def test_exposes_associated_connection(aconn):
    """Transaction exposes its connection as a read-only property."""
    async with aconn.transaction() as tx:
        assert tx.connection is aconn
        with pytest.raises(AttributeError):
            tx.connection = aconn


async def test_exposes_savepoint_name(aconn):
    """Transaction exposes its savepoint name as a read-only property."""
    async with aconn.transaction(savepoint_name="foo") as tx:
        assert tx.savepoint_name == "foo"
        with pytest.raises(AttributeError):
            tx.savepoint_name = "bar"


async def test_cant_reenter(aconn):
    async with aconn.transaction() as tx:
        pass

    with pytest.raises(TypeError):
        async with tx:
            pass


async def test_begins_on_enter(aconn):
    """Transaction does not begin until __enter__() is called."""
    tx = aconn.transaction()
    assert not in_transaction(aconn)
    async with tx:
        assert in_transaction(aconn)
    assert not in_transaction(aconn)


async def test_commit_on_successful_exit(aconn):
    """Changes are committed on successful exit from the `with` block."""
    async with aconn.transaction():
        await insert_row(aconn, "foo")

    assert not in_transaction(aconn)
    assert await inserted(aconn) == {"foo"}


async def test_rollback_on_exception_exit(aconn):
    """Changes are rolled back if an exception escapes the `with` block."""
    with pytest.raises(ExpectedException):
        async with aconn.transaction():
            await insert_row(aconn, "foo")
            raise ExpectedException("This discards the insert")

    assert not in_transaction(aconn)
    assert not await inserted(aconn)


async def test_interaction_dbapi_transaction(aconn):
    await insert_row(aconn, "foo")

    async with aconn.transaction():
        await insert_row(aconn, "bar")
        raise Rollback

    async with aconn.transaction():
        await insert_row(aconn, "baz")

    assert in_transaction(aconn)
    await aconn.commit()
    assert await inserted(aconn) == {"foo", "baz"}


async def test_prohibits_use_of_commit_rollback_autocommit(aconn):
    """
    Within a Transaction block, it is forbidden to touch commit, rollback,
    or the autocommit setting on the connection, as this would interfere
    with the transaction scope being managed by the Transaction block.
    """
    await aconn.set_autocommit(False)
    await aconn.commit()
    await aconn.rollback()

    async with aconn.transaction():
        with pytest.raises(ProgrammingError):
            await aconn.set_autocommit(False)
        with pytest.raises(ProgrammingError):
            await aconn.commit()
        with pytest.raises(ProgrammingError):
            await aconn.rollback()

    await aconn.set_autocommit(False)
    await aconn.commit()
    await aconn.rollback()


@pytest.mark.parametrize("autocommit", [False, True])
async def test_preserves_autocommit(aconn, autocommit):
    """
    Connection.autocommit is unchanged both during and after Transaction block.
    """
    await aconn.set_autocommit(autocommit)
    async with aconn.transaction():
        assert aconn.autocommit is autocommit
    assert aconn.autocommit is autocommit


async def test_autocommit_off_but_no_tx_started_successful_exit(
    aconn, svcconn
):
    """
    Scenario:
    * Connection has autocommit off but no transaction has been initiated
      before entering the Transaction context
    * Code exits Transaction context successfully

    Outcome:
    * Changes made within Transaction context are committed
    """
    await aconn.set_autocommit(False)
    assert not in_transaction(aconn)
    async with aconn.transaction():
        await insert_row(aconn, "new")
    assert not in_transaction(aconn)

    # Changes committed
    assert await inserted(aconn) == {"new"}
    assert inserted(svcconn) == {"new"}


async def test_autocommit_off_but_no_tx_started_exception_exit(aconn, svcconn):
    """
    Scenario:
    * Connection has autocommit off but no transaction has been initiated
      before entering the Transaction context
    * Code exits Transaction context with an exception

    Outcome:
    * Changes made within Transaction context are discarded
    """
    await aconn.set_autocommit(False)
    assert not in_transaction(aconn)
    with pytest.raises(ExpectedException):
        async with aconn.transaction():
            await insert_row(aconn, "new")
            raise ExpectedException()
    assert not in_transaction(aconn)

    # Changes discarded
    assert not await inserted(aconn)
    assert not inserted(svcconn)


async def test_autocommit_off_and_tx_in_progress_successful_exit(
    aconn, svcconn
):
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
    await aconn.set_autocommit(False)
    await insert_row(aconn, "prior")
    assert in_transaction(aconn)
    async with aconn.transaction():
        await insert_row(aconn, "new")
    assert in_transaction(aconn)
    assert await inserted(aconn) == {"prior", "new"}
    # Nothing committed yet; changes not visible on another connection
    assert not inserted(svcconn)


async def test_autocommit_off_and_tx_in_progress_exception_exit(
    aconn, svcconn
):
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
    await aconn.set_autocommit(False)
    await insert_row(aconn, "prior")
    assert in_transaction(aconn)
    with pytest.raises(ExpectedException):
        async with aconn.transaction():
            await insert_row(aconn, "new")
            raise ExpectedException()
    assert in_transaction(aconn)
    assert await inserted(aconn) == {"prior"}
    # Nothing committed yet; changes not visible on another connection
    assert not inserted(svcconn)


async def test_nested_all_changes_persisted_on_successful_exit(aconn, svcconn):
    """Changes from nested transaction contexts are all persisted on exit."""
    async with aconn.transaction():
        await insert_row(aconn, "outer-before")
        async with aconn.transaction():
            await insert_row(aconn, "inner")
        await insert_row(aconn, "outer-after")
    assert not in_transaction(aconn)
    assert await inserted(aconn) == {"outer-before", "inner", "outer-after"}
    assert inserted(svcconn) == {"outer-before", "inner", "outer-after"}


async def test_nested_all_changes_discarded_on_outer_exception(aconn, svcconn):
    """
    Changes from nested transaction contexts are discarded when an exception
    raised in outer context escapes.
    """
    with pytest.raises(ExpectedException):
        async with aconn.transaction():
            await insert_row(aconn, "outer")
            async with aconn.transaction():
                await insert_row(aconn, "inner")
            raise ExpectedException()
    assert not in_transaction(aconn)
    assert not await inserted(aconn)
    assert not inserted(svcconn)


async def test_nested_all_changes_discarded_on_inner_exception(aconn, svcconn):
    """
    Changes from nested transaction contexts are discarded when an exception
    raised in inner context escapes the outer context.
    """
    with pytest.raises(ExpectedException):
        async with aconn.transaction():
            await insert_row(aconn, "outer")
            async with aconn.transaction():
                await insert_row(aconn, "inner")
                raise ExpectedException()
    assert not in_transaction(aconn)
    assert not await inserted(aconn)
    assert not inserted(svcconn)


async def test_nested_inner_scope_exception_handled_in_outer_scope(
    aconn, svcconn
):
    """
    An exception escaping the inner transaction context causes changes made
    within that inner context to be discarded, but the error can then be
    handled in the outer context, allowing changes made in the outer context
    (both before, and after, the inner context) to be successfully committed.
    """
    async with aconn.transaction():
        await insert_row(aconn, "outer-before")
        with pytest.raises(ExpectedException):
            async with aconn.transaction():
                await insert_row(aconn, "inner")
                raise ExpectedException()
        await insert_row(aconn, "outer-after")
    assert not in_transaction(aconn)
    assert await inserted(aconn) == {"outer-before", "outer-after"}
    assert inserted(svcconn) == {"outer-before", "outer-after"}


async def test_nested_three_levels_successful_exit(aconn, svcconn):
    """Exercise management of more than one savepoint."""
    async with aconn.transaction():  # BEGIN
        await insert_row(aconn, "one")
        async with aconn.transaction():  # SAVEPOINT s1
            await insert_row(aconn, "two")
            async with aconn.transaction():  # SAVEPOINT s2
                await insert_row(aconn, "three")
    assert not in_transaction(aconn)
    assert await inserted(aconn) == {"one", "two", "three"}
    assert inserted(svcconn) == {"one", "two", "three"}


async def test_named_savepoint_escapes_savepoint_name(aconn):
    async with aconn.transaction("s-1"):
        pass
    async with aconn.transaction("s1; drop table students"):
        pass


async def test_named_savepoints_successful_exit(aconn, acommands):
    """
    Entering a transaction context will do one of these these things:
    1. Begin an outer transaction (if one isn't already in progress)
    2. Begin an outer transaction and create a savepoint (if one is named)
    3. Create a savepoint (if a transaction is already in progress)
       either using the name provided, or auto-generating a savepoint name.

    ...and exiting the context successfully will "commit" the same.
    """
    commands = acommands

    # Case 1
    # Using Transaction explicitly becase conn.transaction() enters the contetx
    async with aconn.transaction() as tx:
        assert commands.popall() == ["begin"]
        assert not tx.savepoint_name
    assert commands.popall() == ["commit"]

    # Case 1 (with a transaction already started)
    await aconn.cursor().execute("select 1")
    assert commands.popall() == ["begin"]
    async with aconn.transaction() as tx:
        assert commands.popall() == ['savepoint "_pg3_1"']
        assert tx.savepoint_name == "_pg3_1"

    assert commands.popall() == ['release "_pg3_1"']
    await aconn.rollback()
    assert commands.popall() == ["rollback"]

    # Case 2
    async with aconn.transaction(savepoint_name="foo") as tx:
        assert commands.popall() == ['begin; savepoint "foo"']
        assert tx.savepoint_name == "foo"
    assert commands.popall() == ["commit"]

    # Case 3 (with savepoint name provided)
    async with aconn.transaction():
        assert commands.popall() == ["begin"]
        async with aconn.transaction(savepoint_name="bar") as tx:
            assert commands.popall() == ['savepoint "bar"']
            assert tx.savepoint_name == "bar"
        assert commands.popall() == ['release "bar"']
    assert commands.popall() == ["commit"]

    # Case 3 (with savepoint name auto-generated)
    async with aconn.transaction():
        assert commands.popall() == ["begin"]
        async with aconn.transaction() as tx:
            assert commands.popall() == ['savepoint "_pg3_2"']
            assert tx.savepoint_name == "_pg3_2"
        assert commands.popall() == ['release "_pg3_2"']
    assert commands.popall() == ["commit"]


async def test_named_savepoints_exception_exit(aconn, acommands):
    """
    Same as the previous test but checks that when exiting the context with an
    exception, whatever transaction and/or savepoint was started on enter will
    be rolled-back as appropriate.
    """
    commands = acommands

    # Case 1
    with pytest.raises(ExpectedException):
        async with aconn.transaction() as tx:
            assert commands.popall() == ["begin"]
            assert not tx.savepoint_name
            raise ExpectedException
    assert commands.popall() == ["rollback"]

    # Case 2
    with pytest.raises(ExpectedException):
        async with aconn.transaction(savepoint_name="foo") as tx:
            assert commands.popall() == ['begin; savepoint "foo"']
            assert tx.savepoint_name == "foo"
            raise ExpectedException
    assert commands.popall() == ["rollback"]

    # Case 3 (with savepoint name provided)
    async with aconn.transaction():
        assert commands.popall() == ["begin"]
        with pytest.raises(ExpectedException):
            async with aconn.transaction(savepoint_name="bar") as tx:
                assert commands.popall() == ['savepoint "bar"']
                assert tx.savepoint_name == "bar"
                raise ExpectedException
        assert commands.popall() == ['rollback to "bar"; release "bar"']
    assert commands.popall() == ["commit"]

    # Case 3 (with savepoint name auto-generated)
    async with aconn.transaction():
        assert commands.popall() == ["begin"]
        with pytest.raises(ExpectedException):
            async with aconn.transaction() as tx:
                assert commands.popall() == ['savepoint "_pg3_2"']
                assert tx.savepoint_name == "_pg3_2"
                raise ExpectedException
        assert commands.popall() == ['rollback to "_pg3_2"; release "_pg3_2"']
    assert commands.popall() == ["commit"]


async def test_named_savepoints_with_repeated_names_works(aconn):
    """
    Using the same savepoint name repeatedly works correctly, but bypasses
    some sanity checks.
    """
    # Works correctly if no inner transactions are rolled back
    async with aconn.transaction(force_rollback=True):
        async with aconn.transaction("sp"):
            await insert_row(aconn, "tx1")
            async with aconn.transaction("sp"):
                await insert_row(aconn, "tx2")
                async with aconn.transaction("sp"):
                    await insert_row(aconn, "tx3")
        assert await inserted(aconn) == {"tx1", "tx2", "tx3"}

    # Works correctly if one level of inner transaction is rolled back
    async with aconn.transaction(force_rollback=True):
        async with aconn.transaction("s1"):
            await insert_row(aconn, "tx1")
            async with aconn.transaction("s1", force_rollback=True):
                await insert_row(aconn, "tx2")
                async with aconn.transaction("s1"):
                    await insert_row(aconn, "tx3")
            assert await inserted(aconn) == {"tx1"}
        assert await inserted(aconn) == {"tx1"}

    # Works correctly if multiple inner transactions are rolled back
    # (This scenario mandates releasing savepoints after rolling back to them.)
    async with aconn.transaction(force_rollback=True):
        async with aconn.transaction("s1"):
            await insert_row(aconn, "tx1")
            async with aconn.transaction("s1") as tx2:
                await insert_row(aconn, "tx2")
                async with aconn.transaction("s1"):
                    await insert_row(aconn, "tx3")
                    raise Rollback(tx2)
            assert await inserted(aconn) == {"tx1"}
        assert await inserted(aconn) == {"tx1"}

    # Will not (always) catch out-of-order exits
    async with aconn.transaction(force_rollback=True):
        tx1 = aconn.transaction("s1")
        tx2 = aconn.transaction("s1")
        await tx1.__aenter__()
        await tx2.__aenter__()
        await tx1.__aexit__(None, None, None)
        await tx2.__aexit__(None, None, None)


async def test_force_rollback_successful_exit(aconn, svcconn):
    """
    Transaction started with the force_rollback option enabled discards all
    changes at the end of the context.
    """
    async with aconn.transaction(force_rollback=True):
        await insert_row(aconn, "foo")
    assert not await inserted(aconn)
    assert not inserted(svcconn)


async def test_force_rollback_exception_exit(aconn, svcconn):
    """
    Transaction started with the force_rollback option enabled discards all
    changes at the end of the context.
    """
    with pytest.raises(ExpectedException):
        async with aconn.transaction(force_rollback=True):
            await insert_row(aconn, "foo")
            raise ExpectedException()
    assert not await inserted(aconn)
    assert not inserted(svcconn)


async def test_explicit_rollback_discards_changes(aconn, svcconn):
    """
    Raising a Rollback exception in the middle of a block exits the block and
    discards all changes made within that block.

    You can raise any of the following:
     - Rollback (type)
     - Rollback() (instance)
     - Rollback(tx) (instance initialised with reference to the transaction)
    All of these are equivalent.
    """

    async def assert_no_rows():
        assert not await inserted(aconn)
        assert not inserted(svcconn)

    async with aconn.transaction():
        await insert_row(aconn, "foo")
        raise Rollback
    await assert_no_rows()

    async with aconn.transaction():
        await insert_row(aconn, "foo")
        raise Rollback()
    await assert_no_rows()

    async with aconn.transaction() as tx:
        await insert_row(aconn, "foo")
        raise Rollback(tx)
    await assert_no_rows()


async def test_explicit_rollback_outer_tx_unaffected(aconn, svcconn):
    """
    Raising a Rollback exception in the middle of a block does not impact an
    enclosing transaction block.
    """
    async with aconn.transaction():
        await insert_row(aconn, "before")
        async with aconn.transaction():
            await insert_row(aconn, "during")
            raise Rollback
        assert in_transaction(aconn)
        assert not inserted(svcconn)
        await insert_row(aconn, "after")
    assert await inserted(aconn) == {"before", "after"}
    assert inserted(svcconn) == {"before", "after"}


async def test_explicit_rollback_of_outer_transaction(aconn):
    """
    Raising a Rollback exception that references an outer transaction will
    discard all changes from both inner and outer transaction blocks.
    """
    async with aconn.transaction() as outer_tx:
        await insert_row(aconn, "outer")
        async with aconn.transaction():
            await insert_row(aconn, "inner")
            raise Rollback(outer_tx)
        assert False, "This line of code should be unreachable."
    assert not await inserted(aconn)


async def test_explicit_rollback_of_enclosing_tx_outer_tx_unaffected(
    aconn, svcconn
):
    """
    Rolling-back an enclosing transaction does not impact an outer transaction.
    """
    async with aconn.transaction():
        await insert_row(aconn, "outer-before")
        async with aconn.transaction() as tx_enclosing:
            await insert_row(aconn, "enclosing")
            async with aconn.transaction():
                await insert_row(aconn, "inner")
                raise Rollback(tx_enclosing)
        await insert_row(aconn, "outer-after")

        assert await inserted(aconn) == {"outer-before", "outer-after"}
        assert not inserted(svcconn)  # Not yet committed
    # Changes committed
    assert inserted(svcconn) == {"outer-before", "outer-after"}


async def test_str(aconn):
    async with aconn.transaction() as tx:
        assert "[INTRANS]" in str(tx)
        assert "(active)" in str(tx)
        assert "'" not in str(tx)
        async with aconn.transaction("wat") as tx2:
            assert "[INTRANS]" in str(tx2)
            assert "'wat'" in str(tx2)

    assert "[IDLE]" in str(tx)
    assert "(terminated)" in str(tx)
