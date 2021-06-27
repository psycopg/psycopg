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
types:

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
    | | `!int`           | | :sql:`smallint`       |                          |
    | |                  | | :sql:`integer`        |                          |
    |                    | | :sql:`bigint`         |                          |
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

Python `int` values are converted to PostgreSQL :sql:`bigint` (a.k.a.
:sql:`int8`). Note that this could create some problems:

- Python `!int` is unbounded. If you are inserting numbers larger than 2^63
  (so your target table is `numeric`, or you'll get an overflow on
  arrival...) you should convert them to `~decimal.Decimal`.

- Certain PostgreSQL functions and operators, such a :sql:`date + int`
  expect an :sql:`integer` (aka :sql:`int4`): passing them a :sql:`bigint`
  may cause an error::

      cur.execute("select current_date + %s", [1])
      # UndefinedFunction: operator does not exist: date + bigint

  In this case you should add an :sql:`::int` cast to your query or use the
  `~psycopg.types.numeric.Int4` wrapper::

      cur.execute("select current_date + %s::int", [1])

      cur.execute("select current_date + %s", [Int4(1)])

  .. admonition:: TODO

      document Int* wrappers

Python `float` values are converted to PostgreSQL :sql:`float8`.

Python `~decimal.Decimal` values are converted to PostgreSQL :sql:`numeric`.

On the way back, smaller types (:sql:`int2`, :sql:`int4`, :sql:`flaot4`) are
promoted to the larger Python counterpart.

.. note::

    Sometimes you may prefer to receive :sql:`numeric` data as `!float`
    instead, for performance reason or ease of manipulation: you can configure
    an adapter to :ref:`cast PostgreSQL numeric to Python float <faq-float>`.
    This of course may imply a loss of precision.

.. seealso::

    - `PostgreSQL numeric types
      <https://www.postgresql.org/docs/current/static/datatype-numeric.html>`__
    - `Musings about numeric adaptation choices
      <https://www.varrazzo.com/blog/2020/11/07/psycopg3-adaptation/>`__


.. index::
    pair: Strings; Adaptation
    single: Unicode; Adaptation
    pair: Encoding; SQL_ASCII

.. _adapt-string:

Strings adaptation
------------------

Python `str` is converted to PostgreSQL string syntax, and PostgreSQL types
such as :sql:`text` and :sql:`varchar` are converted back to Python `!str`:

.. code:: python

    conn = psycopg.connect()
    conn.execute(
        "insert into strtest (id, data) values (%s, %s)",
        (1, "Crème Brûlée at 4.99€"))
    conn.execute("select data from strtest where id = 1").fetchone()[0]
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
    conn.execute("select data from strtest where id = 1").fetchone()[0]
    'Crème Brûlée at 4.99€'

    # The Latin-1 encoding doesn't have a representation for the Euro symbol
    conn.client_encoding = 'latin1'
    conn.execute("select data from strtest where id = 1").fetchone()[0]
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
    conn.execute("select data from strtest where id = 1").fetchone()[0]
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


.. _adapt-json:

JSON adaptation
---------------

Psycopg can map between Python objects and PostgreSQL `json/jsonb
types`__, allowing to customise the load and dump function used.

.. __: https://www.postgresql.org/docs/current/datatype-json.html

Because several Python objects could be considered JSON (dicts, lists,
scalars, even date/time if using a dumps function customised to use them),
Psycopg requires you to wrap what you want to dump as JSON into a wrapper:
either `psycopg.types.json.Json` or `~psycopg.types.json.Jsonb`.

.. code:: python

    from psycopg.types.json import Jsonb

    thing = {"foo": ["bar", 42]}
    conn.execute("insert into mytable values (%s)", [Jsonb(thing)])

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

    conn.execute("select %s", [Jsonb({"value": 123.45})]).fetchone()[0]
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
    cnn.execute("insert into objs values %s", [Json(obj, dumps=uuid_dumps)])
    # will insert: {'uuid': '0a40799d-3980-4c65-8315-2956b18ab0e1'}


.. _adapt-date:
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
