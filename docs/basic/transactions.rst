.. currentmodule:: psycopg

.. index:: Transactions management
.. index:: InFailedSqlTransaction
.. index:: idle in transaction

.. _transactions:

Transactions management
=======================

Psycopg has a behaviour that may seem surprising compared to
:program:`psql`: by default, any database operation will start a new
transaction. As a consequence, changes made by any cursor of the connection
will not be visible until `Connection.commit()` is called, and will be
discarded by `Connection.rollback()`. The following operation on the same
connection will start a new transaction.

If a database operation fails, the server will refuse further commands, until
a `~rollback()` is called.

If the connection is closed with a transaction open, no COMMIT command is sent
to the server, which will then discard the connection. Certain middleware (such
as PgBouncer) will also discard a connection left in transaction state, so, if
possible you will want to commit or rollback a connection before finishing
working with it.

An example of what will happen, the first time you will use Psycopg (and to be
disappointed by it), is likely:

.. code:: python

    conn = psycopg.connect()

    # Creating a cursor doesn't start a transaction or affect the connection
    # in any way.
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM my_table")
    # This function call executes:
    # - BEGIN
    # - SELECT count(*) FROM my_table
    # So now a transaction has started.

    # If your program spends a long time in this state, the server will keep
    # a connection "idle in transaction", which is likely something undesired

    cur.execute("INSERT INTO data VALUES (%s)", ("Hello",))
    # This statement is executed inside the transaction

    conn.close()
    # No COMMIT was sent: the INSERT was discarded.

There are a few things going wrong here, let's see how they can be improved.

One obvious problem after the run above is that, firing up :program:`psql`,
you will see no new record in the table ``data``. One way to fix the problem
is to call `!conn.commit()` before closing the connection. Thankfully, if you
use the :ref:`connection context <with-connection>`, Psycopg will commit the
connection at the end of the block (or roll it back if the block is exited
with an exception):

The code modified using a connection context will result in the following
sequence of database statements:

.. code-block:: python
    :emphasize-lines: 1

    with psycopg.connect() as conn:

        cur = conn.cursor()

        cur.execute("SELECT count(*) FROM my_table")
        # This function call executes:
        # - BEGIN
        # - SELECT count(*) FROM my_table
        # So now a transaction has started.

        cur.execute("INSERT INTO data VALUES (%s)", ("Hello",))
        # This statement is executed inside the transaction

    # No exception at the end of the block:
    # COMMIT is executed.

This way we don't have to remember to call neither `!close()` nor `!commit()`
and the database operations actually have a persistent effect. The code might
still do something you don't expect: keep a transaction from the first
operation to the connection closure. You can have a finer control over the
transactions using an :ref:`autocommit transaction <autocommit>` and/or
:ref:`transaction contexts <transaction-context>`.

.. warning::

    By default even a simple :sql:`SELECT` will start a transaction: in
    long-running programs, if no further action is taken, the session will
    remain *idle in transaction*, an undesirable condition for several
    reasons (locks are held by the session, tables bloat...). For long lived
    scripts, either make sure to terminate a transaction as soon as possible or
    use an `~Connection.autocommit` connection.

.. hint::

    If a database operation fails with an error message such as
    *InFailedSqlTransaction: current transaction is aborted, commands ignored
    until end of transaction block*, it means that **a previous operation
    failed** and the database session is in a state of error. You need to call
    `~Connection.rollback()` if you want to keep on using the same connection.


.. _autocommit:

Autocommit transactions
-----------------------

The manual commit requirement can be suspended using `~Connection.autocommit`,
either as connection attribute or as `~psycopg.Connection.connect()`
parameter. This may be required to run operations that cannot be executed
inside a transaction, such as :sql:`CREATE DATABASE`, :sql:`VACUUM`,
:sql:`CALL` on `stored procedures`__ using transaction control.

.. __: https://www.postgresql.org/docs/current/xproc.html

With an autocommit transaction, the above sequence of operation results in:

.. code-block:: python
    :emphasize-lines: 1

    with psycopg.connect(autocommit=True) as conn:

        cur = conn.cursor()

        cur.execute("SELECT count(*) FROM my_table")
        # This function call now only executes:
        # - SELECT count(*) FROM my_table
        # and no transaction starts.

        cur.execute("INSERT INTO data VALUES (%s)", ("Hello",))
        # The result of this statement is persisted immediately by the database

    # The connection is closed at the end of the block but, because it is not
    # in a transaction state, no COMMIT is executed.

An autocommit transaction behaves more as someone coming from :program:`psql`
would expect. This has a beneficial performance effect, because less queries
are sent and less operations are performed by the database. The statements,
however, are not executed in an atomic transaction; if you need to execute
certain operations inside a transaction, you can achieve that with an
autocommit connection too, using an explicit :ref:`transaction block
<transaction-context>`.


.. _transaction-context:

Transaction contexts
--------------------

A more transparent way to make sure that transactions are finalised at the
right time is to use `!with` `Connection.transaction()` to create a
transaction context. When the context is entered, a transaction is started;
when leaving the context the transaction is committed, or it is rolled back if
an exception is raised inside the block.

Continuing the example above, if you want to use an autocommit connection but
still wrap selected groups of commands inside an atomic transaction, you can
use a `!transaction()` context:

.. code-block:: python
    :emphasize-lines: 8

    with psycopg.connect(autocommit=True) as conn:

        cur = conn.cursor()

        cur.execute("SELECT count(*) FROM my_table")
        # The connection is autocommit, so no BEGIN executed.

        with conn.transaction():
            # BEGIN is executed, a transaction started

            cur.execute("INSERT INTO data VALUES (%s)", ("Hello",))
            cur.execute("INSERT INTO times VALUES (now())")
            # These two operation run atomically in the same transaction

        # COMMIT is executed at the end of the block.
        # The connection is in idle state again.

    # The connection is closed at the end of the block.


Note that connection blocks can also be used with non-autocommit connections:
in this case you still need to pay attention to eventual transactions started
automatically. If an operation starts an implicit transaction, a
`!transaction()` block will only manage :ref:`a savepoint sub-transaction
<nested-transactions>`, leaving the caller to deal with the main transaction,
as explained in :ref:`transactions`:

.. code:: python

    conn = psycopg.connect()

    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM my_table")
    # This function call executes:
    # - BEGIN
    # - SELECT count(*) FROM my_table
    # So now a transaction has started.

    with conn.transaction():
        # The block starts with a transaction already open, so it will execute
        # - SAVEPOINT

        cur.execute("INSERT INTO data VALUES (%s)", ("Hello",))

    # The block was executing a sub-transaction so on exit it will only run:
    # - RELEASE SAVEPOINT
    # The transaction is still on.

    conn.close()
    # No COMMIT was sent: the INSERT was discarded.

If a `!transaction()` block starts when no transaction is active then it will
manage a proper transaction. In essence, a transaction context tries to leave
a connection in the state it found it, and leaves you to deal with the wider
context.

.. hint::
    The interaction between non-autocommit transactions and transaction
    contexts is probably surprising. Although the non-autocommit default is
    what's demanded by the DBAPI, the personal preference of several experienced
    developers is to:

    - use a connection block: ``with psycopg.connect(...) as conn``;
    - use an autocommit connection, either passing `!autocommit=True` as
      `!connect()` parameter or setting the attribute ``conn.autocommit =
      True``;
    - use `!with conn.transaction()` blocks to manage transactions only where
      needed.


.. _nested-transactions:

Nested transactions
^^^^^^^^^^^^^^^^^^^

Transaction blocks can be also nested (internal transaction blocks are
implemented using SAVEPOINT__): an exception raised inside an inner block
has a chance of being handled and not completely fail outer operations. The
following is an example where a series of operations interact with the
database: operations are allowed to fail; at the end we also want to store the
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
outside the block: in the example it is intercepted by the `!try` so that the
loop can complete. The outermost block is unaffected (unless other errors
happen there).

You can also write code to explicitly roll back any currently active
transaction block, by raising the `Rollback` exception. The exception "jumps"
to the end of a transaction block, rolling back its transaction but allowing
the program execution to continue from there. By default the exception rolls
back the innermost transaction block, but any current block can be specified
as the target. In the following example, a hypothetical `!CancelCommand`
may stop the processing and cancel any operation previously performed,
but not entirely committed yet.

.. code:: python

    from psycopg import Rollback

    with conn.transaction() as outer_tx:
        for command in commands():
            with conn.transaction() as inner_tx:
                if isinstance(command, CancelCommand):
                    raise Rollback(outer_tx)
                process_command(command)

    # If `Rollback` is raised, it would propagate only up to this block,
    # and the program would continue from here with no exception.


.. _transaction-characteristics:

Transaction characteristics
---------------------------

You can set `transaction parameters`__ for the transactions that Psycopg
handles. They affect the transactions started implicitly by non-autocommit
transactions and the ones started explicitly by `Connection.transaction()` for
both autocommit and non-autocommit transactions.

.. Warning::

    Transaction parameters :ref:`don't affect autocommit connections
    <transaction-characteristics-and-autocommit>`, unless a `!transaction()`
    block is explicitly used.

Leaving these parameters as `!None` will use the server's default behaviour
(which is controlled by server settings such as
default_transaction_isolation__).

.. __: https://www.postgresql.org/docs/current/sql-set-transaction.html
.. __: https://www.postgresql.org/docs/current/runtime-config-client.html
       #GUC-DEFAULT-TRANSACTION-ISOLATION

In order to set these parameters you can use the connection attributes
`~Connection.isolation_level`, `~Connection.read_only`,
`~Connection.deferrable`. For async connections you must use the equivalent
`~AsyncConnection.set_isolation_level()` method and similar. The parameters
can only be changed if there isn't a transaction already active on the
connection.

.. warning::

   Applications running at `~IsolationLevel.REPEATABLE_READ` or
   `~IsolationLevel.SERIALIZABLE` isolation level are exposed to serialization
   failures. `In certain concurrent update cases`__, PostgreSQL will raise an
   exception looking like::

        psycopg2.errors.SerializationFailure: could not serialize access
        due to concurrent update

   In this case the application must be prepared to repeat the operation that
   caused the exception.

   .. __: https://www.postgresql.org/docs/current/transaction-iso.html
          #XACT-REPEATABLE-READ


.. index::
    pair: Two-phase commit; Transaction

.. _two-phase-commit:

Two-Phase Commit protocol support
---------------------------------

.. versionadded:: 3.1

Psycopg exposes the two-phase commit features available in PostgreSQL
implementing the `two-phase commit extensions`__ proposed by the DBAPI.

The DBAPI model of two-phase commit is inspired by the `XA specification`__,
according to which transaction IDs are formed from three components:

- a format ID (non-negative 32 bit integer)
- a global transaction ID (string not longer than 64 bytes)
- a branch qualifier (string not longer than 64 bytes)

For a particular global transaction, the first two components will be the same
for all the resources. Every resource will be assigned a different branch
qualifier.

According to the DBAPI specification, a transaction ID is created using the
`Connection.xid()` method. Once you have a transaction id, a distributed
transaction can be started with `Connection.tpc_begin()`, prepared using
`~Connection.tpc_prepare()` and completed using `~Connection.tpc_commit()` or
`~Connection.tpc_rollback()`.  Transaction IDs can also be retrieved from the
database using `~Connection.tpc_recover()` and completed using the above
`!tpc_commit()` and `!tpc_rollback()`.

PostgreSQL doesn't follow the XA standard though, and the ID for a PostgreSQL
prepared transaction can be any string up to 200 characters long. Psycopg's
`Xid` objects can represent both XA-style transactions IDs (such as the ones
created by the `!xid()` method) and PostgreSQL transaction IDs identified by
an unparsed string.

The format in which the Xids are converted into strings passed to the
database is the same employed by the `PostgreSQL JDBC driver`__: this should
allow interoperation between tools written in Python and in Java. For example
a recovery tool written in Python would be able to recognize the components of
transactions produced by a Java program.

For further details see the documentation for the :ref:`tpc-methods`.

.. __: https://www.python.org/dev/peps/pep-0249/#optional-two-phase-commit-extensions
.. __: https://publications.opengroup.org/c193
.. __: https://jdbc.postgresql.org/
