.. currentmodule:: psycopg.types

.. _psycopg.types:

`!types` -- Types information and adapters
==========================================

.. module:: psycopg.types

The `!psycopg.types` package exposes the concrete implementation of `Loader`
and `Dumper` to manage builtin objects, together with objects to describe
PostgreSQL types and wrappers to help or customise the types conversion.


Types information
-----------------

The `TypeInfo` object describes simple information about a PostgreSQL data
type, such as its name, oid and array oid. The class can be used to query a
database for custom data types: this allows for instance to load automatically
arrays of a custom type, once a loader for the base type has been registered.

The `!TypeInfo` object doesn't instruct Psycopg to convert a PostgreSQL type
into a Python type: this is the role of a `~psycopg.abc.Loader`. However it
can extend the behaviour of the adapters: if you create a loader for
`!MyType`, using `TypeInfo` you will be able to manage seamlessly arrays of
`!MyType` or ranges and composite types using it as a subtype.

.. seealso:: :ref:`adaptation` describes about how to convert from Python
    types to PostgreSQL types and back.

.. code:: python

    from psycopg.adapt import Loader
    from psycopg.types import TypeInfo

    t = TypeInfo.fetch(conn, "mytype")
    t.register(conn)

    for record in conn.execute("SELECT mytypearray FROM mytable"):
        # records will return lists of "mytype" as string

    class MyTypeLoader(Loader):
        def load(self, data):
            # parse the data and return a MyType instance

    conn.adapters.register_loader("mytype", MyTypeLoader)

    for record in conn.execute("SELECT mytypearray FROM mytable"):
        # records will return lists of MyType instances


.. autoclass:: TypeInfo

    .. method:: fetch(conn, name)
        :classmethod:

    .. method:: fetch(aconn, name)
        :classmethod:
        :async:
        :noindex:

        Query a system catalog to read information about a type.

        :param conn: the connection to query
        :type conn: ~psycopg.Connection or ~psycopg.AsyncConnection
        :param name: the name of the type to query. It can include a schema
            name.
        :type name: `!str` or `~psycopg.sql.Identifier`
        :return: a `!TypeInfo` object (or subclass) populated with the type
            information, `!None` if not found.

        If the connection is async the function will behave as a coroutine and
        the caller will need to `await` on it to get the result::

            t = await TypeInfo.fetch(aconn, "mytype")

    .. automethod:: register

        :param context: the context where the type is registered, for instance
            a `~psycopg.Connection` or `~psycopg.Cursor`. `!None` registers
            the `!TypeInfo` globally.
        :type context: Optional[~psycopg.abc.AdaptContext]

        Registering the `TypeInfo` in a context allows the adapters of that
        context to look up type information: for instance it allows to
        recognise automatically arrays of that type and load them from the
        database as a list of the base type.


For recursive types, Psycopg offers a few `!TypeInfo` subclasses which can be
used to extract more complete information, for instance
`~psycopg.types.composite.CompositeInfo` and `~psycopg.types.range.RangeInfo`.

`!TypeInfo` objects are collected in `TypesRegistry` instances, which help type
information lookup. Every `~psycopg.adapt.AdaptersMap` expose its type map on
its `~psycopg.adapt.AdaptersMap.types` attribute.

.. autoclass:: TypesRegistry

   `!TypeRegistry` instances are typically exposed by
   `~psycopg.adapt.AdaptersMap` objects in adapt contexts such as
   `~psycopg.Connection` or `~psycopg.Cursor` (e.g. `!conn.adapters.types`).

   The global registry, from which the others inherit from, is available as
   `psycopg.adapters`\ ``.types``.


.. _numeric-wrappers:

Numeric wrappers
----------------

.. autoclass:: psycopg.types.numeric.Int2
.. autoclass:: psycopg.types.numeric.Int4
.. autoclass:: psycopg.types.numeric.Int8
.. autoclass:: psycopg.types.numeric.Oid
.. autoclass:: psycopg.types.numeric.Float4
.. autoclass:: psycopg.types.numeric.Float8

These wrappers can be used to force to dump Python numeric values to a certain
PostgreSQL type. This is rarely needed, usually the automatic rules do the
right thing. One case when they are needed is :ref:`copy-binary`.


.. admonition:: TODO

    Document the various objects wrappers

    - Range


.. _json-adapters:

JSON adapters
-------------

See :ref:`adapt-json` for details.

.. currentmodule:: psycopg.types.json

.. autoclass:: Json
.. autoclass:: Jsonb

Wrappers to signal to convert *obj* to a json or jsonb PostgreSQL value.

Any object supported by the underlying `!dumps()` function can be wrapped.

If a *dumps* function is passed to the wrapper, use it to dump the wrapped
object. Otherwise use the function specified by `set_json_dumps()`.


.. autofunction:: set_json_dumps
.. autofunction:: set_json_loads
