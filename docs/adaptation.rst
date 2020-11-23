.. _adaptation:

.. module:: psycopg3.adapt


``psycopg3.adapt`` -- Data adaptation configuration
===================================================

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
the `~Dumper.dump()` and `~Loader.load()` method. `!psycopg3` provides
implementation for several builtin Python and PostgreSQL types.


.. rubric:: Dumpers and loaders configuration

Dumpers and loaders can be registered on different scopes: globally, per
`~psycopg3.Connection`, per `~psycopg3.Cursor`, so that adaptation rules can
be customised for specific needs within the same application: in order to do
so you can use the *context* parameter of `~Dumper.register()` and similar
methods.

Dumpers and loaders might need to handle data in text and binary format,
according to how they are registered (e.g. with `~Dumper.register()` or
`~Dumper.register_binary()`). For most types the format is different so there
will have to be two different classes.


.. rubric:: Dumpers and loaders life cycle

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

.. autoclass:: Dumper(src, context=None)

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

        By default will return the `dump()` value quoted and sanitised, so
        that the result can be used to build a SQL string. For instance, the
        method will be used by `~psycopg3.sql.Literal` to convert a value
        client-side.

        This method only makes sense for text dumpers; the result of calling
        it on a binary dumper is undefined. It might scratch your car, or burn
        your cake. Don't tell me I didn't warn you.

    .. autoattribute:: oid
        :annotation: int

    .. automethod:: register(src, context=None)

        :param src: The type to manage.
        :type src: `!type` or `!str`
        :param context: Where the dumper should be used. If `!None` the dumper
            will be used globally.
        :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

        If *src* is specified as string it will be lazy-loaded, so that it
        will be possible to register it without importing it before. In this
        case it should be the fully qualified name of the object (e.g.
        ``"uuid.UUID"``).

    .. automethod:: register_binary(src, context=None)

        In order to convert a value in binary you can use a ``%b`` placeholder
        in the query instead of ``%s``.

        Parameters as the same as in `register()`.


.. autoclass:: Loader(oid, context=None)

    :param oid: The type that will be managed by this dumper.
    :type oid: int
    :param context: The context where the transformation is performed. If not
        specified the conversion might be inaccurate, for instance it will not
        be possible to know the connection encoding or the server date format.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    .. automethod:: load

    .. automethod:: register(oid, context=None)

        :param oid: The PostgreSQL OID to manage.
        :type oid: `!int`
        :param context: Where the loader should be used. If `!None` the loader
            will be used globally.
        :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    .. automethod:: register_binary(oid, context=None)

        Parameters as the same as in `register()`.


.. autoclass:: Transformer(context=None)

    :param context: The context where the transformer should operate.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    TODO: finalise the interface of this object
