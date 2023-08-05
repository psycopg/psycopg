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

.. versionchanged:: 3.2
    `numpy.bool_` values can be dumped too.


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

On the way back, smaller types (:sql:`int2`, :sql:`int4`, :sql:`float4`) are
promoted to the larger Python counterpart.

.. note::

    Sometimes you may prefer to receive :sql:`numeric` data as `!float`
    instead, for performance reason or ease of manipulation: you can configure
    an adapter to :ref:`cast PostgreSQL numeric to Python float
    <adapt-example-float>`. This of course may imply a loss of precision.

.. versionchanged:: 3.2

   NumPy integer__ and `floating point`__ values can be dumped too.

.. __: https://numpy.org/doc/stable/reference/arrays.scalars.html#integer-types
.. __: https://numpy.org/doc/stable/reference/arrays.scalars.html#floating-point-types


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


.. _date-time-limits:

Dates and times limits in Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PostgreSQL date and time objects can represent values that cannot be
represented by the Python `datetime` objects:

- dates and timestamps after the year 9999, the special value "infinity";
- dates and timestamps before the year 1, the special value "-infinity";
- the time 24:00:00.

Loading these values will raise a `~psycopg.DataError`.

If you need to handle these values you can define your own mapping (for
instance mapping every value greater than `datetime.date.max` to `!date.max`,
or the time 24:00 to 00:00) and write a subclass of the default loaders
implementing the added capability; please see :ref:`this example
<adapt-example-inf-date>` for a reference.


.. index::
    single: DateStyle
    single: IntervalStyle

.. _datestyle:

DateStyle and IntervalStyle limits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Loading :sql:`timestamp with time zone` in text format is only supported if
the connection DateStyle__ is set to `ISO` format; time and time zone
representation in other formats is ambiguous.

.. __: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE

Furthermore, at the time of writing, the only supported value for
IntervalStyle__ is ``postgres``; loading :sql:`interval` data in text format
with a different setting is not supported.

.. __: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-INTERVALSTYLE

If your server is configured with different settings by default, you can
obtain a connection in a supported style using the ``options`` connection
parameter; for example::

   >>> conn = psycopg.connect(options="-c datestyle=ISO,YMD")
   >>> conn.execute("show datestyle").fetchone()[0]
   # 'ISO, YMD'

These GUC parameters only affects loading in text format; loading timestamps
or intervals in :ref:`binary format <binary-data>` is not affected by
DateStyle or IntervalStyle.


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
`!dumps` parameter in the
`~psycopg.types.json.Json`/`~psycopg.types.json.Jsonb` wrapper, which will
take precedence over what is specified by `!set_json_dumps()`.

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


.. _adapt-enum:

Enum adaptation
---------------

.. versionadded:: 3.1

Psycopg can adapt Python `~enum.Enum` subclasses into PostgreSQL enum types
(created with the |CREATE TYPE AS ENUM|_ command).

.. |CREATE TYPE AS ENUM| replace:: :sql:`CREATE TYPE ... AS ENUM (...)`
.. _CREATE TYPE AS ENUM: https://www.postgresql.org/docs/current/static/datatype-enum.html

In order to set up a bidirectional enum mapping, you should get information
about the PostgreSQL enum using the `~types.enum.EnumInfo` class and
register it using `~types.enum.register_enum()`. The behaviour of unregistered
and registered enums is different.

- If the enum is not registered with `register_enum()`:

  - Pure `!Enum` classes are dumped as normal strings, using their member
    names as value. The unknown oid is used, so PostgreSQL should be able to
    use this string in most contexts (such as an enum or a text field).

    .. versionchanged:: 3.1
        In previous version dumping pure enums is not supported and raise a
        "cannot adapt" error.

  - Mix-in enums are dumped according to their mix-in type (because a `class
    MyIntEnum(int, Enum)` is more specifically an `!int` than an `!Enum`, so
    it's dumped by default according to `!int` rules).

  - PostgreSQL enums are loaded as Python strings. If you want to load arrays
    of such enums you will have to find their OIDs using `types.TypeInfo.fetch()`
    and register them using `~types.TypeInfo.register()`.

- If the enum is registered (using `~types.enum.EnumInfo`\ `!.fetch()` and
  `~types.enum.register_enum()`):

  - Enums classes, both pure and mixed-in, are dumped by name.

  - The registered PostgreSQL enum is loaded back as the registered Python
    enum members.

.. autoclass:: psycopg.types.enum.EnumInfo

   `!EnumInfo` is a subclass of `~psycopg.types.TypeInfo`: refer to the
   latter's documentation for generic usage, especially the
   `~psycopg.types.TypeInfo.fetch()` method.

   .. attribute:: labels

       After `~psycopg.types.TypeInfo.fetch()`, it contains the labels defined
       in the PostgreSQL enum type.

   .. attribute:: enum

       After `register_enum()` is called, it will contain the Python type
       mapping to the registered enum.

.. autofunction:: psycopg.types.enum.register_enum

   After registering, fetching data of the registered enum will cast
   PostgreSQL enum labels into corresponding Python enum members.

   If no `!enum` is specified, a new `Enum` is created based on
   PostgreSQL enum labels.

Example::

    >>> from enum import Enum, auto
    >>> from psycopg.types.enum import EnumInfo, register_enum

    >>> class UserRole(Enum):
    ...     ADMIN = auto()
    ...     EDITOR = auto()
    ...     GUEST = auto()

    >>> conn.execute("CREATE TYPE user_role AS ENUM ('ADMIN', 'EDITOR', 'GUEST')")

    >>> info = EnumInfo.fetch(conn, "user_role")
    >>> register_enum(info, conn, UserRole)

    >>> some_editor = info.enum.EDITOR
    >>> some_editor
    <UserRole.EDITOR: 2>

    >>> conn.execute(
    ...     "SELECT pg_typeof(%(editor)s), %(editor)s",
    ...     {"editor": some_editor}
    ... ).fetchone()
    ('user_role', <UserRole.EDITOR: 2>)

    >>> conn.execute(
    ...     "SELECT ARRAY[%s, %s]",
    ...     [UserRole.ADMIN, UserRole.GUEST]
    ... ).fetchone()
    [<UserRole.ADMIN: 1>, <UserRole.GUEST: 3>]

If the Python and the PostgreSQL enum don't match 1:1 (for instance if members
have a different name, or if more than one Python enum should map to the same
PostgreSQL enum, or vice versa), you can specify the exceptions using the
`!mapping` parameter.

`!mapping` should be a dictionary with Python enum members as keys and the
matching PostgreSQL enum labels as values, or a list of `(member, label)`
pairs with the same meaning (useful when some members are repeated). Order
matters: if an element on either side is specified more than once, the last
pair in the sequence will take precedence::

    # Legacy roles, defined in medieval times.
    >>> conn.execute(
    ...     "CREATE TYPE abbey_role AS ENUM ('ABBOT', 'SCRIBE', 'MONK', 'GUEST')")

    >>> info = EnumInfo.fetch(conn, "abbey_role")
    >>> register_enum(info, conn, UserRole, mapping=[
    ...     (UserRole.ADMIN, "ABBOT"),
    ...     (UserRole.EDITOR, "SCRIBE"),
    ...     (UserRole.EDITOR, "MONK")])

    >>> conn.execute("SELECT '{ABBOT,SCRIBE,MONK,GUEST}'::abbey_role[]").fetchone()[0]
    [<UserRole.ADMIN: 1>,
     <UserRole.EDITOR: 2>,
     <UserRole.EDITOR: 2>,
     <UserRole.GUEST: 3>]

    >>> conn.execute("SELECT %s::text[]", [list(UserRole)]).fetchone()[0]
    ['ABBOT', 'MONK', 'GUEST']

A particularly useful case is when the PostgreSQL labels match the *values* of
a `!str`\-based Enum. In this case it is possible to use something like ``{m:
m.value for m in enum}`` as mapping::

    >>> class LowercaseRole(str, Enum):
    ...     ADMIN = "admin"
    ...     EDITOR = "editor"
    ...     GUEST = "guest"

    >>> conn.execute(
    ...     "CREATE TYPE lowercase_role AS ENUM ('admin', 'editor', 'guest')")

    >>> info = EnumInfo.fetch(conn, "lowercase_role")
    >>> register_enum(
    ...     info, conn, LowercaseRole, mapping={m: m.value for m in LowercaseRole})

    >>> conn.execute("SELECT 'editor'::lowercase_role").fetchone()[0]
    <LowercaseRole.EDITOR: 'editor'>
