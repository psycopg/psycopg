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

    for record in conn.execute("select mytypearray from mytable"):
        # records will return lists of "mytype" as string

    class MyTypeLoader(Loader):
        def load(self, data):
            # parse the data and return a MyType instance

    conn.adapters.register_loader("mytype", MyTypeLoader)

    for record in conn.execute("select mytypearray from mytable"):
        # records will return lists of MyType instances


.. autoclass:: TypeInfo

    .. automethod:: fetch
    .. automethod:: fetch_async
    .. automethod:: register

        The *context* can be a `~psycopg.Connection` or `~psycopg.Cursor`.
        Specifying no context will register the `!TypeInfo` globally.

        Registering the `TypeInfo` in a context allows the adapters of that
        context to look up type information: for instance it allows to
        recognise automatically arrays of that type and load them from the
        database as a list of the base type (how the base type is converted to
        Python is demanded to a `Loader`.


.. autoclass:: TypesRegistry


The following `!TypeInfo` subclasses allow to fetch more specialised
information from certain class of PostgreSQL types and to create more
specialised adapters configurations.


.. autoclass:: psycopg.types.composite.CompositeInfo

    .. automethod:: register

        Using `!CompositeInfo.register()` will also register a specialised
        loader to fetch the composite type as a Python named tuple, or a
        custom object if *factory* is specified.


.. autoclass:: psycopg.types.range.RangeInfo

    .. automethod:: register

        Using `!RangeInfo.register()` will also register a specialised loaders
        and dumpers. For instance, if you create a PostgreSQL range on the
        type :sql:`inet`, loading these object with the database will use the
        loader for the :sql:`inet` type to parse the range bounds - either the
        builtin ones or any one you might have configured.

        The type information will also be used by the `Range` dumper so that
        if you dump a `!Range(address1, address2)` object it will use the
        correct oid for your :sql:`inetrange` type.


Objects wrappers
----------------

.. admonition:: TODO

    Document the various objects wrappers

    - Int2, Int4, Int8, ...
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
