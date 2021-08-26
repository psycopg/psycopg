.. currentmodule:: psycopg

.. index::
    single: Adaptation
    pair: Objects; Adaptation
    single: Data types; Adaptation

.. _types-adaptation:

Adaptation between Python and PostgreSQL types
==============================================

Many standard Python types are adapted into SQL and returned as Python
objects when a query is executed.

The following table shows the default mapping between Python and PostgreSQL
types. In case you need to customise the conversion you should take a look at
:ref:`adaptation`.

TODO: complete table

.. only:: html

  .. table::
    :class: data-types

    +--------------------+-------------------------+--------------------------+
    | Python             | PostgreSQL              | See also                 |
    +====================+=========================+==========================+
    | `!bool`            | :sql:`bool`             | :ref:`adapt-bool`        |
    +--------------------+-------------------------+--------------------------+
    | `!float`           | | :sql:`real`           | :ref:`adapt-numbers`     |
    |                    | | :sql:`double`         |                          |
    +--------------------+-------------------------+                          |
    | `!int`             | | :sql:`smallint`       |                          |
    |                    | | :sql:`integer`        |                          |
    |                    | | :sql:`bigint`         |                          |
    |                    | | :sql:`numeric`        |                          |
    +--------------------+-------------------------+                          |
    | `~decimal.Decimal` | :sql:`numeric`          |                          |
    +--------------------+-------------------------+--------------------------+
    | | `!str`           | | :sql:`varchar`        | :ref:`adapt-string`      |
    | |                  | | :sql:`text`           |                          |
    +--------------------+-------------------------+--------------------------+
    | | `bytes`          | :sql:`bytea`            | :ref:`adapt-binary`      |
    | | `bytearray`      |                         |                          |
    | | `memoryview`     |                         |                          |
    +--------------------+-------------------------+--------------------------+
    | `!date`            | :sql:`date`             | :ref:`adapt-date`        |
    +--------------------+-------------------------+                          |
    | `!time`            | | :sql:`time`           |                          |
    |                    | | :sql:`timetz`         |                          |
    +--------------------+-------------------------+                          |
    | `!datetime`        | | :sql:`timestamp`      |                          |
    |                    | | :sql:`timestamptz`    |                          |
    +--------------------+-------------------------+                          |
    | `!timedelta`       | :sql:`interval`         |                          |
    +--------------------+-------------------------+--------------------------+
    | `!list`            | :sql:`ARRAY`            | :ref:`adapt-list`        |
    +--------------------+-------------------------+--------------------------+
    | | `!tuple`         | Composite types         |:ref:`adapt-composite`    |
    | | `!namedtuple`    |                         |                          |
    +--------------------+-------------------------+--------------------------+
    | `!dict`            | :sql:`hstore`           | :ref:`adapt-hstore`      |
    +--------------------+-------------------------+--------------------------+
    | Psycopg's `!Range` | :sql:`range`            | :ref:`adapt-range`       |
    +--------------------+-------------------------+--------------------------+
    | Anything\ |tm|     | :sql:`json`             | :ref:`adapt-json`        |
    +--------------------+-------------------------+--------------------------+
    | `~uuid.UUID`       | :sql:`uuid`             | :ref:`adapt-uuid`        |
    +--------------------+-------------------------+--------------------------+
    | `ipaddress`        | | :sql:`inet`           | :ref:`adapt-network`     |
    | objects            | | :sql:`cidr`           |                          |
    +--------------------+-------------------------+--------------------------+

.. |tm| unicode:: U+2122


.. index::
    pair: Boolean; Adaptation

.. _adapt-bool:

Booleans adaptation
-------------------

Python `bool` values `!True` and `!False` are converted to the equivalent
`PostgreSQL boolean type`__::

    >>> cur.execute("SELECT %s, %s", (True, False))
    # equivalent to "SELECT true, false"

.. __: https://www.postgresql.org/docs/current/datatype-boolean.html


.. index::
    single: Adaptation; numbers
    single: Integer; Adaptation
    single: Float; Adaptation
    single: Decimal; Adaptation

.. _adapt-numbers:

Numbers adaptation
------------------

.. seealso::

    - `PostgreSQL numeric types
      <https://www.postgresql.org/docs/current/static/datatype-numeric.html>`__

- Python `int` values can be converted to PostgreSQL :sql:`smallint`,
  :sql:`integer`, :sql:`bigint`, or :sql:`numeric`, according to their numeric
  value. Psycopg will choose the smallest data type available, because
  PostgreSQL can automatically cast a type up (e.g. passing a `smallint` where
  PostgreSQL expect an `integer` is gladly accepted) but will not cast down
  automatically (e.g. if a function has an :sql:`integer` argument, passing it
  a :sql:`bigint` value will fail, even if the value is 1).

- Python `float` values are converted to PostgreSQL :sql:`float8`.

- Python `~decimal.Decimal` values are converted to PostgreSQL :sql:`numeric`.

On the way back, smaller types (:sql:`int2`, :sql:`int4`, :sql:`flaot4`) are
promoted to the larger Python counterpart.

If you need a more precise control of how `!int` or `!float` are converted,
The `psycopg.types.numeric` module contains a few :ref:`wrappers
<numeric-wrappers>` which can be used to convince Psycopg to cast the values
to a specific PostgreSQL type::

    >>> conn.execute("select pg_typeof(%s), pg_typeof(%s)", (42, Int4(42))).fetchone()
    ('smallint', 'integer')

These wrappers are rarely needed, because PostgreSQL cast rules and Psycopg
choices usually do the right thing. One use case where they are useful is
:ref:`copy-binary`.

.. note::

    Sometimes you may prefer to receive :sql:`numeric` data as `!float`
    instead, for performance reason or ease of manipulation: you can configure
    an adapter to :ref:`cast PostgreSQL numeric to Python float <faq-float>`.
    This of course may imply a loss of precision.


.. index::
    pair: Strings; Adaptation
    single: Unicode; Adaptation
    pair: Encoding; SQL_ASCII

.. _adapt-string:

Strings adaptation
------------------

.. seealso::

    - `PostgreSQL character types
      <https://www.postgresql.org/docs/current/datatype-character.html>`__

Python `str` are converted to PostgreSQL string syntax, and PostgreSQL types
such as :sql:`text` and :sql:`varchar` are converted back to Python `!str`:

.. code:: python

    conn = psycopg.connect()
    conn.execute(
        "INSERT INTO menu (id, entry) VALUES (%s, %s)",
        (1, "Crème Brûlée at 4.99€"))
    conn.execute("SELECT entry FROM menu WHERE id = 1").fetchone()[0]
    'Crème Brûlée at 4.99€'

PostgreSQL databases `have an encoding`__, and `the session has an encoding`__
too, exposed in the `Connection.client_encoding` attribute. If your database
and connection are in UTF-8 encoding you will likely have no problem,
otherwise you will have to make sure that your application only deals with the
non-ASCII chars that the database can handle; failing to do so may result in
encoding/decoding errors:

.. __: https://www.postgresql.org/docs/current/sql-createdatabase.html
.. __: https://www.postgresql.org/docs/current/multibyte.html

.. code:: python

    # The encoding is set at connection time according to the db configuration
    conn.client_encoding
    'utf-8'

    # The Latin-9 encoding can manage some European accented letters
    # and the Euro symbol
    conn.client_encoding = 'latin9'
    conn.execute("SELECT entry FROM menu WHERE id = 1").fetchone()[0]
    'Crème Brûlée at 4.99€'

    # The Latin-1 encoding doesn't have a representation for the Euro symbol
    conn.client_encoding = 'latin1'
    conn.execute("SELECT entry FROM menu WHERE id = 1").fetchone()[0]
    # Traceback (most recent call last)
    # ...
    # UntranslatableCharacter: character with byte sequence 0xe2 0x82 0xac
    # in encoding "UTF8" has no equivalent in encoding "LATIN1"

In rare cases you may have strings with unexpected encodings in the database.
Using the ``SQL_ASCII`` client encoding (or setting
`~Connection.client_encoding` ``= "ascii"``) will disable decoding of the data
coming from the database, which will be returned as `bytes`:

.. code:: python

    conn.client_encoding = "ascii"
    conn.execute("SELECT entry FROM menu WHERE id = 1").fetchone()[0]
    b'Cr\xc3\xa8me Br\xc3\xbbl\xc3\xa9e at 4.99\xe2\x82\xac'

Alternatively you can cast the unknown encoding data to :sql:`bytea` to
retrieve it as bytes, leaving other strings unaltered: see :ref:`adapt-binary`

Note that PostgreSQL text cannot contain the ``0x00`` byte. If you need to
store Python strings that may contain binary zeros you should use a
:sql:`bytea` field.


.. index::
    single: bytea; Adaptation
    single: bytes; Adaptation
    single: bytearray; Adaptation
    single: memoryview; Adaptation
    single: Binary string

.. _adapt-binary:

Binary adaptation
-----------------

Python types representing binary objects (`bytes`, `bytearray`, `memoryview`)
are converted by default to :sql:`bytea` fields. By default data received is
returned as `!bytes`.

.. admonition:: todo

    Make sure bytearry/memoryview work and are compsable with
    arrays/composite

If you are storing large binary data in bytea fields (such as binary documents
or images) you should probably use the binary format to pass and return
values, otherwise binary data will undergo `ASCII escaping`__, taking some CPU
time and more bandwidth. See :ref:`binary-data` for details.

.. __: https://www.postgresql.org/docs/current/datatype-binary.html


.. _adapt-date:

Date/time types adaptation
--------------------------

.. seealso::

    - `PostgreSQL date/time types
      <https://www.postgresql.org/docs/current/datatype-datetime.html>`__

- Python `~datetime.date` objects are converted to PostgreSQL :sql:`date`.
- Python `~datetime.datetime` objects are converted to PostgreSQL
  :sql:`timestamp` (if they don't have a `!tzinfo` set) or :sql:`timestamptz`
  (if they do).
- Python `~datetime.time` objects are converted to PostgreSQL :sql:`time`
  (if they don't have a `!tzinfo` set) or :sql:`timetz` (if they do).
- Python `~datetime.timedelta` objects are converted to PostgreSQL
  :sql:`interval`.

PostgreSQL :sql:`timestamptz` values are returned with a timezone set to the
`connection TimeZone setting`__, which is available as a Python
`~zoneinfo.ZoneInfo` object in the `!Connection.info`.\ `~ConnectionInfo.timezone`
attribute::

    >>> cnn.info.timezone
    zoneinfo.ZoneInfo(key='Europe/London')

    >>> cnn.execute("select '2048-07-08 12:00'::timestamptz").fetchone()[0]
    datetime.datetime(2048, 7, 8, 12, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/London'))


.. __: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-TIMEZONE


.. _adapt-json:

JSON adaptation
---------------

Psycopg can map between Python objects and PostgreSQL `json/jsonb
types`__, allowing to customise the load and dump function used.

.. __: https://www.postgresql.org/docs/current/datatype-json.html

Because several Python objects could be considered JSON (dicts, lists,
scalars, even date/time if using a dumps function customised to use them),
Psycopg requires you to wrap the object to dump as JSON into a wrapper:
either `psycopg.types.json.Json` or `~psycopg.types.json.Jsonb`.

.. code:: python

    from psycopg.types.json import Jsonb

    thing = {"foo": ["bar", 42]}
    conn.execute("INSERT INTO mytable VALUES (%s)", [Jsonb(thing)])

By default Psycopg uses the standard library `json.dumps` and `json.loads`
functions to serialize and de-serialize Python objects to JSON. If you want to
customise how serialization happens, for instance changing serialization
parameters or using a different JSON library, you can specify your own
functions using the `psycopg.types.json.set_json_dumps()` and
`~psycopg.types.json.set_json_loads()` functions, to apply either globally or
to a specific context (connection or cursor).

.. code:: python

    from functools import partial
    from psycopg.types.json import Jsonb, set_json_dumps, set_json_loads
    import ujson

    # Use a faster dump function
    set_json_dumps(ujson.dumps)

    # Return floating point values as Decimal, just in one connection
    set_json_loads(partial(json.loads, parse_float=Decimal), conn)

    conn.execute("SELECT %s", [Jsonb({"value": 123.45})]).fetchone()[0]
    # {'value': Decimal('123.45')}

If you need an even more specific dump customisation only for certain objects
(including different configurations in the same query) you can specify a
*dumps* parameter in the
`~psycopg.types.json.Json`/`~psycopg.types.json.Jsonb` wrapper, which will
take precedence over what specified by `!set_json_dumps()`.

.. code:: python

    from uuid import UUID, uuid4

    class UUIDEncoder(json.JSONEncoder):
        """A JSON encoder which can dump UUID."""
        def default(self, obj):
            if isinstance(obj, UUID):
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    uuid_dumps = partial(json.dumps, cls=UUIDEncoder)
    obj = {"uuid": uuid4()}
    cnn.execute("INSERT INTO objs VALUES %s", [Json(obj, dumps=uuid_dumps)])
    # will insert: {'uuid': '0a40799d-3980-4c65-8315-2956b18ab0e1'}


.. _adapt-list:
.. _adapt-composite:
.. _adapt-hstore:
.. _adapt-range:
.. _adapt-uuid:
.. _adapt-network:

TODO adaptation
----------------

.. admonition:: TODO

    Document the other types
