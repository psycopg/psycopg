.. index::
    pair: psycopg2; Differences

Differences from psycopg2
=========================

`!psycopg3` uses the common DBAPI structure of many other database adapter and
tries to behave as close as possible to `!psycopg2`. There are however a few
differences to be aware of.


Server-side binding
-------------------

`!psycopg3` sends the query and the parameters to the server separately,
instead of merging them client-side. PostgreSQL may behave slightly
differently in this case, usually throwing an error and suggesting to use an
explicit cast.

.. code:: python

    cur.execute("select '[10,20,30]'::jsonb -> 1").fetchone()
    # returns (20,)

    cur.execute("select '[10,20,30]'::jsonb -> %s", [1]).fetchone()
    # raises an exception:
    # UndefinedFunction: operator does not exist: jsonb -> numeric

    cur.execute("select '[10,20,30]'::jsonb -> %s::int", [1]).fetchone()
    # returns (20,)

PostgreSQL will also reject the execution of several queries at once
(separated by semicolon), if they contain parameters. If parameters are used
you should use distinct `execute()` calls; otherwise you may consider merging
the query client-side, using `psycopg3.sql` module.


Different adaptation system
---------------------------

The adaptation system has been completely rewritten, in order to address
server-side parameters adaptation, but also to consider performance,
flexibility, ease of customization.

Builtin data types should work as expected; if you have wrapped a custom data
type you should check the `<ref> Adaptation` topic.


Other differences
-----------------

When the connection is used as context manager, at the end of the context the
connection will be closed. In psycopg2 only the transaction is closed, so a
connection can be used in several contexts, but the behaviour is surprising
for people used to several other Python classes wrapping resources, such as
files.


What's new in psycopg3
======================

- `asyncio` support.
- Several data types are adapted out-of-the-box: uuid, network, range, bytea,
  array of any supported type are dealt with automatically.
- Access to the low-level libpq functions.
