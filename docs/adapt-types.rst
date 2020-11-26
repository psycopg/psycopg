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
    +--------------------+-------------------------+                          |
    | | `!bytes`         | :sql:`bytea`            |                          |
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
  `~psycopg3.types.Int4` wrapper::

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


.. _adapt-string:
.. _adapt-date:
.. _adapt-list:
.. _adapt-composite:
.. _adapt-hstore:
.. _adapt-range:
.. _adapt-json:
.. _adapt-uuid:
.. _adapt-network:

TODO adaptation
----------------

.. admonition:: TODO

    Document the other types
