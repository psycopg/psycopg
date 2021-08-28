.. currentmodule:: psycopg

.. index::
    single: Adaptation
    pair: Objects; Adaptation
    single: Data types; Adaptation

.. _extra-adaptation:

Adapting other PostgreSQL types
===============================

PostgreSQL offers other data types which don't map to native Python types.
Psycopg offers wrappers and conversion functions to allow their use.


.. _adapt-range:

Range adaptation
----------------

PostgreSQL `range types`__ are a family of data types representing a range of
value between two elements. The type of the element is called the range
*subtype*. PostgreSQL offers a few built-in range types and allows the
definition of custom ones.

.. __: https://www.postgresql.org/docs/current/rangetypes.html

All the PostgreSQL range types are loaded as the `~psycopg.types.range.Range`
Python type, which is a `~typing.Generic` type and can hold bounds of
different types.

.. autoclass:: psycopg.types.range.Range

    This Python type is only used to pass and retrieve range values to and
    from PostgreSQL and doesn't attempt to replicate the PostgreSQL range
    features: it doesn't perform normalization and doesn't implement all the
    operators__ supported by the database.

    .. __: https://www.postgresql.org/docs/current/static/functions-range.html#RANGE-OPERATORS-TABLE

    `!Range` objects are immutable, hashable, and support the ``in`` operator
    (checking if an element is within the range). They can be tested for
    equivalence. Empty ranges evaluate to `!False` in boolean context,
    nonempty evaluate to `!True`.

    `!Range` objects have the following attributes:

    .. autoattribute:: isempty
    .. autoattribute:: lower
    .. autoattribute:: upper
    .. autoattribute:: lower_inc
    .. autoattribute:: upper_inc
    .. autoattribute:: lower_inf
    .. autoattribute:: upper_inf

The built-in range objects are adapted automatically: if a `!Range` objects
contains `~datetime.date` bounds, it is dumped using the :sql:`daterange` OID,
and of course :sql:`daterange` values are loaded back as `!Range[date]`.

If you create your own range type you can use `~psycopg.types.range.RangeInfo`
and `~psycopg.types.range.register_range()` to associate the range type with
its subtype and make it work like the builtin ones.

.. autoclass:: psycopg.types.range.RangeInfo

   `~RangeInfo` is a `~psycopg.types.TypeInfo` subclass: check its
   documentation for generic details.

.. autofunction:: psycopg.types.range.register_range

Example::

    >>> conn.execute("create type strrange as range (subtype = text)")

    >>> info = RangeInfo.fetch(conn, "strrange")
    >>> register_range(info, conn)

    >>> conn.execute("select pg_typeof(%s)", [Range("a", "z")]).fetchone()[0]
    'strrange'

    >>> conn.execute("select '[a,z]'::strrange").fetchone()[0]
     Range('a', 'z', '[]')
