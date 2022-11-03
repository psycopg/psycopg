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


.. index::
    pair: Composite types; Data types
    pair: tuple; Adaptation
    pair: namedtuple; Adaptation

.. _adapt-composite:

Composite types casting
-----------------------

Psycopg can adapt PostgreSQL composite types (either created with the |CREATE
TYPE|_ command or implicitly defined after a table row type) to and from
Python tuples, `~collections.namedtuple`, or any other suitable object
configured.

.. |CREATE TYPE| replace:: :sql:`CREATE TYPE`
.. _CREATE TYPE: https://www.postgresql.org/docs/current/static/sql-createtype.html

Before using a composite type it is necessary to get information about it
using the `~psycopg.types.composite.CompositeInfo` class and to register it
using `~psycopg.types.composite.register_composite()`.

.. autoclass:: psycopg.types.composite.CompositeInfo

   `!CompositeInfo` is a `~psycopg.types.TypeInfo` subclass: check its
   documentation for the generic usage, especially the
   `~psycopg.types.TypeInfo.fetch()` method.

   .. attribute:: python_type

       After `register_composite()` is called, it will contain the python type
       mapping to the registered composite.

.. autofunction:: psycopg.types.composite.register_composite

   After registering, fetching data of the registered composite will invoke
   `!factory` to create corresponding Python objects.

   If no factory is specified, a `~collection.namedtuple` is created and used
   to return data.

   If the `!factory` is a type (and not a generic callable), then dumpers for
   that type are created and registered too, so that passing objects of that
   type to a query will adapt them to the registered type.

Example::

    >>> from psycopg.types.composite import CompositeInfo, register_composite

    >>> conn.execute("CREATE TYPE card AS (value int, suit text)")

    >>> info = CompositeInfo.fetch(conn, "card")
    >>> register_composite(info, conn)

    >>> my_card = info.python_type(8, "hearts")
    >>> my_card
    card(value=8, suit='hearts')

    >>> conn.execute(
    ...     "SELECT pg_typeof(%(card)s), (%(card)s).suit", {"card": my_card}
    ...     ).fetchone()
    ('card', 'hearts')

    >>> conn.execute("SELECT (%s, %s)::card", [1, "spades"]).fetchone()[0]
    card(value=1, suit='spades')


Nested composite types are handled as expected, provided that the type of the
composite components are registered as well::

    >>> conn.execute("CREATE TYPE card_back AS (face card, back text)")

    >>> info2 = CompositeInfo.fetch(conn, "card_back")
    >>> register_composite(info2, conn)

    >>> conn.execute("SELECT ((8, 'hearts'), 'blue')::card_back").fetchone()[0]
    card_back(face=card(value=8, suit='hearts'), back='blue')


.. index::
    pair: range; Data types

.. _adapt-range:

Range adaptation
----------------

PostgreSQL `range types`__ are a family of data types representing a range of
values between two elements. The type of the element is called the range
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

The built-in range objects are adapted automatically: if a `!Range` objects
contains `~datetime.date` bounds, it is dumped using the :sql:`daterange` OID,
and of course :sql:`daterange` values are loaded back as `!Range[date]`.

If you create your own range type you can use `~psycopg.types.range.RangeInfo`
and `~psycopg.types.range.register_range()` to associate the range type with
its subtype and make it work like the builtin ones.

.. autoclass:: psycopg.types.range.RangeInfo

   `!RangeInfo` is a `~psycopg.types.TypeInfo` subclass: check its
   documentation for generic details, especially the
   `~psycopg.types.TypeInfo.fetch()` method.

.. autofunction:: psycopg.types.range.register_range

Example::

    >>> from psycopg.types.range import Range, RangeInfo, register_range

    >>> conn.execute("CREATE TYPE strrange AS RANGE (SUBTYPE = text)")
    >>> info = RangeInfo.fetch(conn, "strrange")
    >>> register_range(info, conn)

    >>> conn.execute("SELECT pg_typeof(%s)", [Range("a", "z")]).fetchone()[0]
    'strrange'

    >>> conn.execute("SELECT '[a,z]'::strrange").fetchone()[0]
    Range('a', 'z', '[]')


.. index::
    pair: range; Data types

.. _adapt-multirange:

Multirange adaptation
---------------------

Since PostgreSQL 14, every range type is associated with a multirange__, a
type representing a disjoint set of ranges. A multirange is
automatically available for every range, built-in and user-defined.

.. __: https://www.postgresql.org/docs/current/rangetypes.html

All the PostgreSQL range types are loaded as the
`~psycopg.types.multirange.Multirange` Python type, which is a mutable
sequence of `~psycopg.types.range.Range` elements.

.. autoclass:: psycopg.types.multirange.Multirange

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

Like for `~psycopg.types.range.Range`, built-in multirange objects are adapted
automatically: if a `!Multirange` object contains `!Range` with
`~datetime.date` bounds, it is dumped using the :sql:`datemultirange` OID, and
:sql:`datemultirange` values are loaded back as `!Multirange[date]`.

If you have created your own range type you can use
`~psycopg.types.multirange.MultirangeInfo` and
`~psycopg.types.multirange.register_multirange()` to associate the resulting
multirange type with its subtype and make it work like the builtin ones.

.. autoclass:: psycopg.types.multirange.MultirangeInfo

   `!MultirangeInfo` is a `~psycopg.types.TypeInfo` subclass: check its
   documentation for generic details, especially the
   `~psycopg.types.TypeInfo.fetch()` method.

.. autofunction:: psycopg.types.multirange.register_multirange

Example::

    >>> from psycopg.types.multirange import \
    ...     Multirange, MultirangeInfo, register_multirange
    >>> from psycopg.types.range import Range

    >>> conn.execute("CREATE TYPE strrange AS RANGE (SUBTYPE = text)")
    >>> info = MultirangeInfo.fetch(conn, "strmultirange")
    >>> register_multirange(info, conn)

    >>> rec = conn.execute(
    ...     "SELECT pg_typeof(%(mr)s), %(mr)s",
    ...     {"mr": Multirange([Range("a", "q"), Range("l", "z")])}).fetchone()

    >>> rec[0]
    'strmultirange'
    >>> rec[1]
    Multirange([Range('a', 'z', '[)')])


.. index::
    pair: hstore; Data types
    pair: dict; Adaptation

.. _adapt-hstore:

Hstore adaptation
-----------------

The |hstore|_ data type is a key-value store embedded in PostgreSQL. It
supports GiST or GIN indexes allowing search by keys or key/value pairs as
well as regular BTree indexes for equality, uniqueness etc.

.. |hstore| replace:: :sql:`hstore`
.. _hstore: https://www.postgresql.org/docs/current/static/hstore.html

Psycopg can convert Python `!dict` objects to and from |hstore| structures.
Only dictionaries with string keys and values are supported. `!None` is also
allowed as value but not as a key.

In order to use the |hstore| data type it is necessary to load it in a
database using:

.. code:: none

    =# CREATE EXTENSION hstore;

Because |hstore| is distributed as a contrib module, its oid is not well
known, so it is necessary to use `!TypeInfo`\.\
`~psycopg.types.TypeInfo.fetch()` to query the database and get its oid. The
resulting object can be passed to
`~psycopg.types.hstore.register_hstore()` to configure dumping `!dict` to
|hstore| and parsing |hstore| back to `!dict`, in the context where the
adapter is registered.

.. autofunction:: psycopg.types.hstore.register_hstore

Example::

    >>> from psycopg.types import TypeInfo
    >>> from psycopg.types.hstore import register_hstore

    >>> info = TypeInfo.fetch(conn, "hstore")
    >>> register_hstore(info, conn)

    >>> conn.execute("SELECT pg_typeof(%s)", [{"a": "b"}]).fetchone()[0]
    'hstore'

    >>> conn.execute("SELECT 'foo => bar'::hstore").fetchone()[0]
    {'foo': 'bar'}


.. index::
    pair: geometry; Data types
    single: PostGIS; Data types

.. _adapt-shapely:

Geometry adaptation using Shapely
---------------------------------

When using the PostGIS_ extension, it can be useful to retrieve geometry_
values and have them automatically converted to Shapely_ instances. Likewise,
you may want to store such instances in the database and have the conversion
happen automatically.

.. warning::
    Psycopg doesn't have a dependency on the ``shapely`` package: you should
    install the library as an additional dependency of your project.

.. warning::
    This module is experimental and might be changed in the future according
    to users' feedback.

.. _PostGIS: https://postgis.net/
.. _geometry: https://postgis.net/docs/geometry.html
.. _Shapely: https://github.com/Toblerity/Shapely
.. _shape: https://shapely.readthedocs.io/en/stable/manual.html#shapely.geometry.shape

Since PostgGIS is an extension, the :sql:`geometry` type oid is not well
known, so it is necessary to use `!TypeInfo`\.\
`~psycopg.types.TypeInfo.fetch()` to query the database and find it. The
resulting object can be passed to `~psycopg.types.shapely.register_shapely()`
to configure dumping `shape`_ instances to :sql:`geometry` columns and parsing
:sql:`geometry` data back to `!shape` instances, in the context where the
adapters are registered.

.. function:: psycopg.types.shapely.register_shapely

    Register Shapely dumper and loaders.

    After invoking this function on an adapter, the queries retrieving
    PostGIS geometry objects will return Shapely's shape object instances
    both in text and binary mode.

    Similarly, shape objects can be sent to the database.

    This requires the Shapely library to be installed.

    :param info: The object with the information about the geometry type.
    :param context: The context where to register the adapters. If `!None`,
        register it globally.

    .. note::

        Registering the adapters doesn't affect objects already created, even
        if they are children of the registered context. For instance,
        registering the adapter globally doesn't affect already existing
        connections.

Example::

    >>> from psycopg.types import TypeInfo
    >>> from psycopg.types.shapely import register_shapely
    >>> from shapely.geometry import Point

    >>> info = TypeInfo.fetch(conn, "geometry")
    >>> register_shapely(info, conn)

    >>> conn.execute("SELECT pg_typeof(%s)", [Point(1.2, 3.4)]).fetchone()[0]
    'geometry'

    >>> conn.execute("""
    ... SELECT ST_GeomFromGeoJSON('{
    ...     "type":"Point",
    ...     "coordinates":[-48.23456,20.12345]}')
    ... """).fetchone()[0]
    <shapely.geometry.multipolygon.MultiPolygon object at 0x7fb131f3cd90>

Notice that, if the geometry adapters are registered on a specific object (a
connection or cursor), other connections and cursors will be unaffected::

    >>> conn2 = psycopg.connect(CONN_STR)
    >>> conn2.execute("""
    ... SELECT ST_GeomFromGeoJSON('{
    ...     "type":"Point",
    ...     "coordinates":[-48.23456,20.12345]}')
    ... """).fetchone()[0]
    '0101000020E61000009279E40F061E48C0F2B0506B9A1F3440'

