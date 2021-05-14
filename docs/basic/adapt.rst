.. currentmodule:: psycopg

.. index::
    single: Adaptation
    pair: Objects; Adaptation
    single: Data types; Adaptation

.. _types-adaptation:

Adapting basic Python types
===========================

Many standard Python types are adapted into SQL and returned as Python
objects when a query is executed.

Converting the following data types between Python and PostgreSQL works
out-of-the-box and doesn't require any configuration. In case you need to
customise the conversion you should take a look at :ref:`adaptation`.


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
too, exposed in the `!Connection.info.`\ `~ConnectionInfo.encoding`
attribute. If your database and connection are in UTF-8 encoding you will
likely have no problem, otherwise you will have to make sure that your
application only deals with the non-ASCII chars that the database can handle;
failing to do so may result in encoding/decoding errors:

.. __: https://www.postgresql.org/docs/current/sql-createdatabase.html
.. __: https://www.postgresql.org/docs/current/multibyte.html

.. code:: python

    # The encoding is set at connection time according to the db configuration
    conn.info.encoding
    'utf-8'

    # The Latin-9 encoding can manage some European accented letters
    # and the Euro symbol
    conn.execute("SET client_encoding TO LATIN9")
    conn.execute("SELECT entry FROM menu WHERE id = 1").fetchone()[0]
    'Crème Brûlée at 4.99€'

    # The Latin-1 encoding doesn't have a representation for the Euro symbol
    conn.execute("SET client_encoding TO LATIN1")
    conn.execute("SELECT entry FROM menu WHERE id = 1").fetchone()[0]
    # Traceback (most recent call last)
    # ...
    # UntranslatableCharacter: character with byte sequence 0xe2 0x82 0xac
    # in encoding "UTF8" has no equivalent in encoding "LATIN1"

In rare cases you may have strings with unexpected encodings in the database.
Using the ``SQL_ASCII`` client encoding  will disable decoding of the data
coming from the database, which will be returned as `bytes`:

.. code:: python

    conn.execute("SET client_encoding TO SQL_ASCII")
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

    >>> conn.info.timezone
    zoneinfo.ZoneInfo(key='Europe/London')

    >>> conn.execute("select '2048-07-08 12:00'::timestamptz").fetchone()[0]
    datetime.datetime(2048, 7, 8, 12, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/London'))

.. note::
    PostgreSQL :sql:`timestamptz` doesn't store "a timestamp with a timezone
    attached": it stores a timestamp always in UTC, which is converted, on
    output, to the connection TimeZone setting::

    >>> conn.execute("SET TIMEZONE to 'Europe/Rome'")  # UTC+2 in summer

    >>> conn.execute("SELECT '2042-07-01 12:00Z'::timestamptz").fetchone()[0]  # UTC input
    datetime.datetime(2042, 7, 1, 14, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/Rome'))

    Check out the `PostgreSQL documentation about timezones`__ for all the
    details.

    .. __: https://www.postgresql.org/docs/current/datatype-datetime.html
           #DATATYPE-TIMEZONES

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

Lists adaptation
----------------

Python `list` objects are adapted to `PostgreSQL arrays`__ and back. Only
lists containing objects of the same type can be dumped to PostgreSQL (but the
list may contain `!None` elements).

.. __: https://www.postgresql.org/docs/current/arrays.html

.. note::

    If you have a list of values which you want to use with the :sql:`IN`
    operator... don't. It won't work (neither with a list nor with a tuple)::

        >>> conn.execute("SELECT * FROM mytable WHERE id IN %s", [[10,20,30]])
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        psycopg.errors.SyntaxError: syntax error at or near "$1"
        LINE 1: SELECT * FROM mytable WHERE id IN $1
                                                  ^
    
    What you want to do instead is to use the `'= ANY()' expression`__ and pass
    the values as a list (not a tuple).

        >>> conn.execute("SELECT * FROM mytable WHERE id = ANY(%s)", [[10,20,30]])

    This has also the advantage of working with an empty list, whereas ``IN
    ()`` is not valid SQL.

    .. __: https://www.postgresql.org/docs/current/functions-comparisons.html
            #id-1.5.8.30.16


.. _adapt-uuid:

UUID adaptation
---------------

Python `uuid.UUID` objects are adapted to PostgreSQL `UUID type`__ and back::

    >>> conn.execute("select gen_random_uuid()").fetchone()[0]
    UUID('97f0dd62-3bd2-459e-89b8-a5e36ea3c16c')

    >>> from uuid import uuid4
    >>> conn.execute("select gen_random_uuid() = %s", [uuid4()]).fetchone()[0]
    False  # long shot

.. __: https://www.postgresql.org/docs/current/datatype-uuid.html


.. _adapt-network:

Network data types adaptation
-----------------------------

Objects from the `ipaddress` module are converted to PostgreSQL `network
address types`__:

- `~ipaddress.IPv4Address`, `~ipaddress.IPv4Interface` objects are converted
  to the PostgreSQL :sql:`inet` type. On the way back, :sql:`inet` values
  indicating a single address are converted to `!IPv4Address`, otherwise they
  are converted to `!IPv4Interface`

- `~ipaddress.IPv4Network` objects are converted to the :sql:`cidr` type and
  back.

- `~ipaddress.IPv6Address`, `~ipaddress.IPv6Interface`,
  `~ipaddress.IPv6Network` objects follow the same rules, with IPv6
  :sql:`inet` and :sql:`cidr` values.

.. __: https://www.postgresql.org/docs/current/datatype-net-types.html#DATATYPE-CIDR

.. code:: python

    >>> conn.execute("select '192.168.0.1'::inet, '192.168.0.1/24'::inet").fetchone()
    (IPv4Address('192.168.0.1'), IPv4Interface('192.168.0.1/24'))

    >>> conn.execute("select '::ffff:1.2.3.0/120'::cidr").fetchone()[0]
    IPv6Network('::ffff:102:300/120')
