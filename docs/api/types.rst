.. currentmodule:: psycopg.types

.. _psycopg.types:

`!types` -- Types information and adapters
==========================================

.. module:: psycopg.types

The `!psycopg.types` package exposes:

- objects to describe PostgreSQL types, such as `TypeInfo`, `TypesRegistry`,
  to help or :ref:`customise the types conversion <adaptation>`;

- concrete implementations of `~psycopg.abc.Loader` and `~psycopg.abc.Dumper`
  protocols to :ref:`handle builtin data types <types-adaptation>`;

- helper objects to represent PostgreSQL data types which :ref:`don't have a
  straightforward Python representation <extra-adaptation>`, such as
  `~range.Range`.


Types information
-----------------

The `TypeInfo` object describes simple information about a PostgreSQL data
type, such as its name, oid and array oid. `!TypeInfo` subclasses may hold more
information, for instance the components of a composite type.

You can use `TypeInfo.fetch()` to query information from a database catalog,
which is then used by helper functions, such as
`~psycopg.types.hstore.register_hstore()`, to register adapters on types whose
OID is not known upfront or to create more specialised adapters.

The `!TypeInfo` object doesn't instruct Psycopg to convert a PostgreSQL type
into a Python type: this is the role of a `~psycopg.abc.Loader`. However it
can extend the behaviour of other adapters: if you create a loader for
`!MyType`, using the `TypeInfo` information, Psycopg will be able to manage
seamlessly arrays of `!MyType` or ranges and composite types using `!MyType`
as a subtype.

.. seealso:: :ref:`adaptation` describes how to convert from Python objects to
    PostgreSQL types and back.

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

        If the connection is async, `!fetch()` will behave as a coroutine and
        the caller will need to `!await` on it to get the result::

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


In order to get information about dynamic PostgreSQL types, Psycopg offers a
few `!TypeInfo` subclasses, whose `!fetch()` method can extract more complete
information about the type, such as `~psycopg.types.composite.CompositeInfo`,
`~psycopg.types.range.RangeInfo`, `~psycopg.types.multirange.MultirangeInfo`,
`~psycopg.types.enum.EnumInfo`.

`!TypeInfo` objects are collected in `TypesRegistry` instances, which help type
information lookup. Every `~psycopg.adapt.AdaptersMap` exposes its type map on
its `~psycopg.adapt.AdaptersMap.types` attribute.

.. autoclass:: TypesRegistry

   `!TypeRegistry` instances are typically exposed by
   `~psycopg.adapt.AdaptersMap` objects in adapt contexts such as
   `~psycopg.Connection` or `~psycopg.Cursor` (e.g. `!conn.adapters.types`).

   The global registry, from which the others inherit from, is available as
   `psycopg.adapters`\ `!.types`.

   .. automethod:: __getitem__

       .. code:: python

           >>> import psycopg

           >>> psycopg.adapters.types["text"]
           <TypeInfo: text (oid: 25, array oid: 1009)>

           >>> psycopg.adapters.types[23]
           <TypeInfo: int4 (oid: 23, array oid: 1007)>

   .. automethod:: get

   .. automethod:: get_oid

       .. code:: python

           >>> psycopg.adapters.types.get_oid("text[]")
           1009

   .. automethod:: get_by_subtype


.. _json-adapters:

JSON adapters
-------------

See :ref:`adapt-json` for details.

.. currentmodule:: psycopg.types.json

.. autoclass:: Json
.. autoclass:: Jsonb

Wrappers to signal to convert `!obj` to a json or jsonb PostgreSQL value.

Any object supported by the underlying `!dumps()` function can be wrapped.

If a `!dumps` function is passed to the wrapper, use it to dump the wrapped
object. Otherwise use the function specified by `set_json_dumps()`.


.. autofunction:: set_json_dumps
.. autofunction:: set_json_loads


.. _numeric-wrappers:

Numeric wrappers
-----------------

.. currentmodule:: psycopg.types.numeric

Wrappers to force a Python number to be dumped as a specific PostgreSQL
type, overriding the automatic choice described in :ref:`adapt-numbers`.

.. code:: python

    >>> from psycopg.types.numeric import Int8
    >>> conn.execute("SELECT pg_typeof(%s)", [Int8(1)]).fetchone()[0]
    'bigint'

.. autoclass:: Int2
.. autoclass:: Int4
.. autoclass:: Int8
.. autoclass:: IntNumeric
.. autoclass:: Float4
.. autoclass:: Float8
.. autoclass:: Oid


.. _range-api:

Range adapters
--------------

See :ref:`adapt-range` for details.

.. currentmodule:: psycopg.types.range

.. autoclass:: Range

    This Python type is only used to pass and retrieve range values to and
    from PostgreSQL and doesn't attempt to replicate the PostgreSQL range
    features: it doesn't perform normalization and doesn't implement all the
    operators__ supported by the database.

    PostgreSQL will perform normalisation on `!Range` objects used as query
    parameters, so, when they are fetched back, they will be found in the
    normal form (for instance ranges on integers will have `[)` bounds).

    .. __: https://www.postgresql.org/docs/current/static/functions-range.html#RANGE-OPERATORS-TABLE

    `!Range` objects are immutable, hashable, and support the `!in` operator
    (checking if an element is within the range). They can be tested for
    equivalence. Empty ranges evaluate to `!False` in a boolean context,
    nonempty ones evaluate to `!True`.

    `!Range` objects have the following attributes:

    .. autoattribute:: isempty
    .. autoattribute:: lower
    .. autoattribute:: upper
    .. autoattribute:: lower_inc
    .. autoattribute:: upper_inc
    .. autoattribute:: lower_inf
    .. autoattribute:: upper_inf

.. autoclass:: RangeInfo

    `!RangeInfo` is a `~psycopg.types.TypeInfo` subclass: check its
    documentation for generic details, especially the
    `~psycopg.types.TypeInfo.fetch()` method.

.. autofunction:: register_range


.. _multirange-api:

Multirange adapters
-------------------

See :ref:`adapt-multirange` for details.

.. currentmodule:: psycopg.types.multirange

.. autoclass:: Multirange

    This Python type is only used to pass and retrieve multirange values to
    and from PostgreSQL and doesn't attempt to replicate the PostgreSQL
    multirange features: overlapping items are not merged, empty ranges are
    not discarded, the items are not ordered, the behaviour of `multirange
    operators`__ is not replicated in Python.

    PostgreSQL will perform normalisation on `!Multirange` objects used as
    query parameters, so, when they are fetched back, they will be found
    ordered, with overlapping ranges merged, etc.

    .. __: https://www.postgresql.org/docs/current/static/functions-range.html#MULTIRANGE-OPERATORS-TABLE

    `!Multirange` objects are a `~collections.abc.MutableSequence` and are
    totally ordered: they behave pretty much like a list of `!Range`. Like
    Range, they are `~typing.Generic` on the subtype of their range, so you
    can declare a variable to be `!Multirange[date]` and mypy will complain if
    you try to add it a `Range[Decimal]`.

.. autoclass:: MultirangeInfo

    `!MultirangeInfo` is a `~psycopg.types.TypeInfo` subclass: check its
    documentation for general details, especially the
    `~psycopg.types.TypeInfo.fetch()` method.

.. autofunction:: register_multirange
