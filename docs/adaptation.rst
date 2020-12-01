.. currentmodule:: psycopg3.adapt

.. _adaptation:

Data adaptation configuration
=============================

The adaptation system is at the core of psycopg3 and allows to customise the
way Python objects are converted to PostgreSQL when a query is performed and
how PostgreSQL values are converted to Python objects when query results are
returned.

.. note::
    For a high-level view of the conversion of types between Python and
    PostgreSQL please look at :ref:`query-parameters`. Using the objects
    described in this page is useful if you intend to *customise* the
    adaptation rules.

The `Dumper` is the base object to perform conversion from a Python object to
a `!bytes` string understood by PostgreSQL. The string returned *shouldn't be
quoted*: the value will be passed to the database using functions such as
:pq:`PQexecParams()` so quoting and quotes escaping is not necessary.

The `Loader` is the base object to perform the opposite operation: to read a
`!bytes` string from PostgreSQL and create a Python object.

`!Dumper` and `!Loader` are abstract classes: concrete classes must implement
the `~Dumper.dump()` and `~Loader.load()` methods. `!psycopg3` provides
implementation for several builtin Python and PostgreSQL types.

.. admonition:: TODO

    Document the builtin adapters, where are they?


Dumpers and loaders configuration
---------------------------------

Dumpers and loaders can be registered on different scopes: globally, per
`~psycopg3.Connection`, per `~psycopg3.Cursor`, so that adaptation rules can
be customised for specific needs within the same application: in order to do
so you can use the *context* parameter of `Dumper.register()` and
`Loader.register()`.

.. note::

    `!register()` is a class method on the base class, so if you
    subclass `!Dumper` or `!Loader` you should call the ``.register()`` on the
    class you created.

If the data type has also a distinct binary format which you may want to use
from psycopg (as documented in :ref:`binary-data`) you may want to implement
binary loaders and dumpers, whose only difference from text dumper is that
they must be registered using ``format=Format.BINARY`` in `Dumper.register()`
and `Loader.register()`.

.. admonition:: TODO

    - Example: infinity date customisation
    - Example: numeric to float


Dumpers and loaders life cycle
------------------------------

Registering dumpers and loaders will instruct `!psycopg3` to use them
in the queries to follow, in the context where they have been registered.

When a query is performed, a `Transformer` object will be used to instantiate
dumpers and loaders as requested and to dispatch the values to convert
to the right instance:

- The `!Trasformer` will look up the most specific adapter: one registered on
  the `~psycopg3.Cursor` if available, then one registered on the
  `~psycopg3.Connection`, finally a global one.

- For every Python type passed as query argument there will be a `!Dumper`
  instantiated. All the objects of the same type will use the same loader.

- For every OID returned by a query there will be a `!Loader` instantiated.
  All the values with the same OID will be converted by the same loader.

- Recursive types (e.g. Python lists, PostgreSQL arrays and composite types)
  will use the same adaptation rules.

As a consequence it is possible to perform certain choices only once per query
(e.g. looking up the connection encoding) and then call a fast-path operation
for each value to convert.

Querying will fail if a Python object for which there isn't a `!Dumper`
registered (for the right `~psycopg3.pq.Format`) is used as query parameter.
If the query returns a data type whose OID doesn't have a `!Loader`, the
value will be returned as a string (or bytes string for binary types).


Objects involved in types adaptation
------------------------------------

.. admonition:: TODO

    move to API section

.. autoclass:: Dumper(src, context=None)

    This is an abstract base class: subclasses *must* implement the `dump()`
    method. They *may* implement `oid` (as attribute or property) in order to
    override the oid type oid; if not PostgreSQL will try to infer the type
    from the context, but this may fail in some contexts and may require a
    cast.

    :param src: The type that will be managed by this dumper.
    :type src: type
    :param context: The context where the transformation is performed. If not
        specified the conversion might be inaccurate, for instance it will not
        be possible to know the connection encoding or the server date format.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    .. automethod:: dump

        The format returned by dump shouldn't contain quotes or escaped
        values.

    .. automethod:: quote

        By default return the `dump()` value quoted and sanitised, so
        that the result can be used to build a SQL string. This works well
        for most types and you won't likely have to implement this method in a
        subclass.

        .. tip::

            This method will be used by `~psycopg3.sql.Literal` to convert a
            value client-side.

        This method only makes sense for text dumpers; the result of calling
        it on a binary dumper is undefined. It might scratch your car, or burn
        your cake. Don't tell me I didn't warn you.

    .. autoattribute:: oid
        :annotation: int

        .. admonition:: todo

            Document how to find type OIDs in a database.

    .. automethod:: register(src, context=None, format=Format.TEXT)

        You should call this method on the `Dumper` subclass you create,
        passing the Python type you want to dump as *src*.

        :param src: The type to manage.
        :type src: `!type` or `!str`
        :param context: Where the dumper should be used. If `!None` the dumper
            will be used globally.
        :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`
        :param format: Register the dumper for text or binary adaptation
        :type format: `~psycopg3.pq.Format`

        If *src* is specified as string it will be lazy-loaded, so that it
        will be possible to register it without importing it before. In this
        case it should be the fully qualified name of the object (e.g.
        ``"uuid.UUID"``).


.. autoclass:: Loader(oid, context=None)

    This is an abstract base class: subclasses *must* implement the `load()`
    method.

    :param oid: The type that will be managed by this dumper.
    :type oid: int
    :param context: The context where the transformation is performed. If not
        specified the conversion might be inaccurate, for instance it will not
        be possible to know the connection encoding or the server date format.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    .. automethod:: load

    .. automethod:: register(oid, context=None, format=Format.TEXT)

        You should call this method on the `Loader` subclass you create,
        passing the OID of the type you want to load as *oid* parameter.

        :param oid: The PostgreSQL OID to manage.
        :type oid: `!int`
        :param context: Where the loader should be used. If `!None` the loader
            will be used globally.
        :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`
        :param format: Register the loader for text or binary adaptation
        :type format: `~psycopg3.pq.Format`


.. autoclass:: Transformer(context=None)

    :param context: The context where the transformer should operate.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    TODO: finalise the interface of this object
