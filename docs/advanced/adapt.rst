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

- The `~psycopg3.types.TypeInfo` object allows to query type information from
  a database, which can be used by the adapters: for instance to make them
  able to decode arrays of base types or composite types.

- The `Dumper` is the base object to perform conversion from a Python object
  to a `!bytes` string understood by PostgreSQL. The string returned
  *shouldn't be quoted*: the value will be passed to the database using
  functions such as :pq:`PQexecParams()` so quoting and quotes escaping is not
  necessary.

- The `Loader` is the base object to perform the opposite operation: to read a
  `!bytes` string from PostgreSQL and create a Python object.

`!Dumper` and `!Loader` are abstract classes: concrete classes must implement
the `~Dumper.dump()` and `~Loader.load()` methods. `!psycopg3` provides
implementation for several builtin Python and PostgreSQL types.

Psycopg provides adapters for several builtin types, which can be used as the
base to build more complex ones: they all live in the `psycopg3.types`
package.


Dumpers and loaders configuration
---------------------------------

Dumpers and loaders can be registered on different scopes: globally, per
`~psycopg3.Connection`, per `~psycopg3.Cursor`, so that adaptation rules can
be customised for specific needs within the same application: in order to do
so you can use the *context* parameter of `Dumper.register()` and
`Loader.register()`.

When a `!Connection` is created, it inherits the global adapters
configuration; when a `!Cursor` is created it inherits its `!Connection`
configuration.

.. note::

    `!register()` is a class method on the base class, so if you
    subclass `!Dumper` or `!Loader` you should call the ``.register()`` on the
    class you created.

For example, suppose you want to work with the "infinity" date which is
available in PostgreSQL but not handled by Python:

.. code:: python

    >>> conn.execute("'infinity'::date").fetchone()
    Traceback (most recent call last):
       ...
    psycopg3.DataError: Python date doesn't support years after 9999: got infinity

One possibility would be to store Python's `datetime.date.max` to PostgreSQL
infinity. For this, let's create a subclass for the dumper and the loader and
register them in the working scope (globally or just on a connection or
cursor):

.. code:: python

    from datetime import date

    from psycopg3.oids import postgres_types as builtins
    from psycopg3.types import DateLoader, DateDumper

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

    InfDateDumper.register(date, cur)
    InfDateLoader.register(builtins["date"].oid, cur)

    cur.execute("SELECT %s::text, %s::text", [date(2020, 12, 31), date.max]).fetchone()
    # ('2020-12-31', 'infinity')
    cur.execute("select '2020-12-31'::date, 'infinity'::date").fetchone()
    # (datetime.date(2020, 12, 31), datetime.date(9999, 12, 31))

.. admonition:: TODO

    - Example: numeric to float


Dumpers and loaders life cycle
------------------------------

Registering dumpers and loaders will instruct `!psycopg3` to use them
in the queries to follow, in the context where they have been registered.

When a query is performed on a `!Cursor`, a `Transformer` object is created
as a local context to manage conversions during the query, instantiating the
required dumpers and loaders and dispatching the values to convert to the
right instance.

- The `!Transformer` copies the adapters configuration from the `!Cursor`, thus
  inheriting all the changes made to the global configuration, the current
  `!Connection`, the `!Cursor`.

- For every Python type passed as query argument, the `!Transformer` will
  instantiate a `!Dumper`. Usually all the objects of the same type will be
  converted by the same dumper; certain dumpers may be used in more than one
  instance, because the same Python type maps to more than one PostgreSQL type
  (for instance, a Python `int` might be better dumped as a PostgreSQL
  :sql:`integer`, :sql:`bigint`, :sql:`smallint` according to its value).

- For every OID returned by the query, the `!Transformer` will instantiate a
  `!Loader`. All the values with the same OID will be converted by the same
  loader.

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


.. autoclass:: Format
    :members:


.. autoclass:: Dumper(cls, context=None)

    This is an abstract base class: subclasses *must* implement the `dump()`
    method and specify the `format`.
    They *may* implement `oid` (as attribute or property) in order to
    override the oid type oid; if not PostgreSQL will try to infer the type
    from the context, but this may fail in some contexts and may require a
    cast.

    :param cls: The type that will be managed by this dumper.
    :type cls: type
    :param context: The context where the transformation is performed. If not
        specified the conversion might be inaccurate, for instance it will not
        be possible to know the connection encoding or the server date format.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    .. attribute:: format
        :type: pq.Format

        The format this class dumps, `~Format.TEXT` or `~Format.BINARY`.
        This is a class attribute.


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

        .. admonition:: todo

            Document how to find type OIDs in a database.

    .. automethod:: register(cls, context=None)

        You should call this method on the `Dumper` subclass you create,
        passing the Python type you want to dump as *cls*.

        :param cls: The type to manage.
        :type cls: `!type` or `!str`
        :param context: Where the dumper should be used. If `!None` the dumper
            will be used globally.
        :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

        If *cls* is specified as string it will be lazy-loaded, so that it
        will be possible to register it without importing it before. In this
        case it should be the fully qualified name of the object (e.g.
        ``"uuid.UUID"``).


.. autoclass:: Loader(oid, context=None)

    This is an abstract base class: subclasses *must* implement the `load()`
    method and specify a `format`.

    :param oid: The type that will be managed by this dumper.
    :type oid: int
    :param context: The context where the transformation is performed. If not
        specified the conversion might be inaccurate, for instance it will not
        be possible to know the connection encoding or the server date format.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    .. attribute:: format
        :type: Format

        The format this class can load, `~Format.TEXT` or `~Format.BINARY`.
        This is a class attribute.

    .. automethod:: load

    .. automethod:: register(oid, context=None)

        You should call this method on the `Loader` subclass you create,
        passing the OID of the type you want to load as *oid* parameter.

        :param oid: The PostgreSQL OID to manage.
        :type oid: `!int`
        :param context: Where the loader should be used. If `!None` the loader
            will be used globally.
        :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`


.. autoclass:: Transformer(context=None)

    :param context: The context where the transformer should operate.
    :type context: `~psycopg3.Connection`, `~psycopg3.Cursor`, or `Transformer`

    TODO: finalise the interface of this object
