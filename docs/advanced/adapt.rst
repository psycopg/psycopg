.. currentmodule:: psycopg.adapt

.. _adaptation:

Data adaptation configuration
=============================

The adaptation system is at the core of Psycopg and allows to customise the
way Python objects are converted to PostgreSQL when a query is performed and
how PostgreSQL values are converted to Python objects when query results are
returned.

.. note::
    For a high-level view of the conversion of types between Python and
    PostgreSQL please look at :ref:`query-parameters`. Using the objects
    described in this page is useful if you intend to *customise* the
    adaptation rules.

- Adaptation configuration is performed by changing the
  `~psycopg.proto.AdaptContext.adapters` object of objects implementing the
  `~psycopg.proto.AdaptContext` protocols, for instance `~psycopg.Connection`
  or `~psycopg.Cursor`.

- Every context object derived from another context inherits its adapters
  mapping: cursors created from a connection inherit the connection's
  configuration. Connections obtain an adapters map from the global map
  exposed as `psycopg.adapters`: changing the content of this object will
  affect every connection created afterwards.

  .. image:: ../pictures/adapt.svg
     :align: center

- The `!adapters` attribute are `AdaptersMap` instances, and contain the
  mapping from Python types and `~psycopg.proto.Dumper` classes, and from
  PostgreSQL oids to `~psycopg.proto.Loader` classes. Changing this mapping
  (e.g. writing and registering your own adapters, or using a different
  configuration of builtin adapters) affects how types are converted between
  Python and PostgreSQL.

  - Dumpers (objects implementing the `~psycopg.proto.Dumper` protocol) are
    the objects used to perform the conversion from a Python object to a bytes
    sequence in a format understood by PostgreSQL. The string returned
    *shouldn't be quoted*: the value will be passed to the database using
    functions such as :pq:`PQexecParams()` so quoting and quotes escaping is
    not necessary. The dumper usually also suggests the server what type to
    use, via its `~psycopg.proto.Dumper.oid` attribute.

  - Loaders (objects implementing the `~psycopg.proto.Loader` protocol) are
    the objects used to perform the opposite operation: reading a bytes
    sequence from PostgreSQL and create a Python object out of it.

  - Dumpers and loaders are instantiated on demand by a `~Transformer` object
    when a query is executed.


Example: handling infinity date
-------------------------------

Suppose you want to work with the "infinity" date which is available in
PostgreSQL but not handled by Python:

.. code:: python

    >>> conn.execute("'infinity'::date").fetchone()
    Traceback (most recent call last):
       ...
    psycopg.DataError: Python date doesn't support years after 9999: got infinity

One possibility would be to store Python's `datetime.date.max` to PostgreSQL
infinity. For this, let's create a subclass for the dumper and the loader and
register them in the working scope (globally or just on a connection or
cursor):

.. code:: python

    from datetime import date

    # Subclass existing adapters so that the base case is handled normally.
    from psycopg.types.datetime import DateLoader, DateDumper

    class InfDateDumper(DateDumper):
        def dump(self, obj):
            if obj == date.max:
                return b"infinity"
            else:
                return super().dump(obj)

    class InfDateLoader(DateLoader):
        def load(self, data):
            if data == b"infinity":
                return date.max
            else:
                return super().load(data)

    # The new classes can be registered globally, on a connection, on a cursor
    cur.adapters.register_dumper(date, InfDateDumper)
    cur.adapters.register_loader("date", InfDateLoader)

    cur.execute("SELECT %s::text, %s::text", [date(2020, 12, 31), date.max]).fetchone()
    # ('2020-12-31', 'infinity')
    cur.execute("select '2020-12-31'::date, 'infinity'::date").fetchone()
    # (datetime.date(2020, 12, 31), datetime.date(9999, 12, 31))


Example: PostgreSQL numeric to Python float
-------------------------------------------

.. admonition:: TODO

    Write it


Dumpers and loaders life cycle
------------------------------

Registering dumpers and loaders will instruct Psycopg to use them
in the queries to follow, in the context where they have been registered.

When a query is performed on a `~psycopg.Cursor`, a
`~psycopg.adapt.Transformer` object is created as a local context to manage
conversions during the query, instantiating the required dumpers and loaders
and dispatching the values to convert to the right instance.

- The `!Transformer` copies the adapters configuration from the `!Cursor`,
  thus inheriting all the changes made to the global `psycopg.adapters`
  configuration, the current `!Connection`, the `!Cursor`.

- For every Python type passed as query argument, the `!Transformer` will
  instantiate a `!Dumper`. Usually all the objects of the same type will be
  converted by the same dumper; certain dumpers may be used in more than one
  instance, because the same Python type maps to more than one PostgreSQL type
  (for instance, a Python `int` might be better dumped as a PostgreSQL
  :sql:`integer`, :sql:`bigint`, :sql:`smallint` according to its value).

- According to the placeholder used (``%s``, ``%b``, ``%t``), Psycopg may pick
  a binary or a text dumper. When using the ``%s`` "`~PyFormat.AUTO`" format,
  if the same type has both a text and a binary dumper registered, the last
  one registered by `~AdaptersMap.register_dumper()` will be used.

- Sometimes, just the Python type is not enough to infer the best PostgreSQL
  type to use (for instance the PostgreSQL type of a Python list depends on
  the objects it contains, whether to use an :sql:`integer` or :sql:`bigint`
  depends on the number size...) In these cases the mechanism provided by
  `~psycopg.proto.Dumper.get_key()` and `~psycopg.proto.Dumper.upgrade()` is
  used.

- For every OID returned by the query, the `!Transformer` will instantiate a
  `!Loader`. All the values with the same OID will be converted by the same
  loader.

- Recursive types (e.g. Python lists, PostgreSQL arrays and composite types)
  will use the same adaptation rules.

As a consequence it is possible to perform certain choices only once per query
(e.g. looking up the connection encoding) and then call a fast-path operation
for each value to convert.

Querying will fail if a Python object for which there isn't a `!Dumper`
registered (for the right `~psycopg.pq.Format`) is used as query parameter.
If the query returns a data type whose OID doesn't have a `!Loader`, the
value will be returned as a string (or bytes string for binary types).
