.. currentmodule:: psycopg3

.. index:: Transactions management
.. index:: InFailedSqlTransaction
.. index:: idle in transaction

.. _transactions:

Transaction management
======================

`!psycopg3` has a behaviour that may result surprising compared to
:program:`psql`: by default, any database operation will start a new
transaction. As a consequence, changes made by any cursor of the connection
will not be visible until `Connection.commit()` is called, and will be
discarded by `Connection.rollback()`. The following operation on the same
connection will start a new transaction.

If a database operation fails, the server will refuse further commands, until
a `~rollback()` is called.

.. hint::

    If a database operation fails with an error message such as
    *InFailedSqlTransaction: current transaction is aborted, commands ignored
    until end of transaction block*, it means that **a previous operation
    failed** and the database session is in a state of error. You need to call
    `!rollback()` if you want to keep on using the same connection.


.. _autocommit:

Autocommit transactions
-----------------------

The manual commit requirement can be suspended using `~Connection.autocommit`,
either as connection attribute or as `~psycopg3.Connection.connect()`
parameter. This may be required to run operations that cannot be executed
inside a transaction, such as :sql:`CREATE DATABASE`, :sql:`VACUUM`,
:sql:`CALL` on `stored procedures`__ using transaction control.

.. __: https://www.postgresql.org/docs/current/xproc.html

.. warning::

    By default even a simple :sql:`SELECT` will start a transaction: in
    long-running programs, if no further action is taken, the session will
    remain *idle in transaction*, an undesirable condition for several
    reasons (locks are held by the session, tables bloat...). For long lived
    scripts, either make sure to terminate a transaction as soon as possible or
    use an `~Connection.autocommit` connection.


.. _transaction-block:

Transaction blocks
------------------

A more transparent way to make sure that transactions are finalised at the
right time is to use ``with`` `Connection.transaction()` to create a
transaction block. When the block is entered a transaction is started; when
leaving the block the transaction is committed, or it is rolled back if an
exception is raised inside the block.

For instance, an hypothetical but extremely secure bank may have the following
code to avoid that no accident between the following two lines leaves the
accounts unbalanced:

.. code:: python

    with conn.transaction():
        move_money(conn, account1, -100)
        move_money(conn, account2, +100)

    # The transaction is now committed

But because the bank is, like, *extremely secure*, they also verify that no
account goes negative:

.. code:: python

    def move_money(conn, account, amount):
        new_balance = add_to_balance(conn, account, amount)
        if new_balance < 0:
            raise ValueError("account balance cannot go negative")

In case this function raises an exception, be it the `!ValueError` in the
example or any other exception expected or not, the transaction will be rolled
back, and the exception will propagate out of the `with` block, further down
the call stack.

Transaction blocks can also be nested (internal transaction blocks are
implemented using SAVEPOINT__): an exception raised inside an inner block
has a chance of being handled and not completely fail outer operations. The
following is an example where a series of operations interact with the
database: operations are allowed to fail, plus we also want to store the
number of operations successfully processed.

.. __: https://www.postgresql.org/docs/current/sql-savepoint.html

.. code:: python

    with conn.transaction() as tx1:
        num_ok = 0
        for operation in operations:
            try:
                with conn.transaction() as tx2:
                    unreliable_operation(conn, operation)
            except Exception:
                logger.exception(f"{operation} failed")
            else:
                num_ok += 1

        save_number_of_successes(conn, num_ok)

If `!unreliable_operation()` causes an error, including an operation causing a
database error, all its changes will be reverted. The exception bubbles up
outside the block: in the example it is intercepted by the ``try`` so that the
loop can complete. The outermost block is unaffected (unless other errors
happen there).

You can also write code to explicitly roll back any currently active
transaction block, by raising the `Rollback` exception. The exception "jumps"
to the end of a transaction block, rolling back its transaction but allowing
the program execution to continue from there. By default the exception rolls
back the innermost transaction block, but any current block can be specified
as the target. In the following example, an hypothetical `!CancelCommand`
may stop the processing and cancel any operation previously performed,
but not entirely committed yet.

.. code:: python

    from psycopg3 import Rollback

    with conn.transaction() as outer_tx:
        for command in commands():
            with conn.transaction() as inner_tx:
                if isinstance(command, CancelCommand):
                    raise Rollback(outer_tx)
            process_command(command)

    # If `Rollback` is raised, it would propagate only up to this block,
    # and the program would continue from here with no exception.
