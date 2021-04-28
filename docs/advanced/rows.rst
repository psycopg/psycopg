.. currentmodule:: psycopg3

.. index:: row factories

.. _row-factories:

Row factories
=============

Cursor's `fetch*` methods return tuples of column values by default. This can
be changed to adapt the needs of the programmer by using custom row factories.

A row factory is a callable that accepts a cursor object and returns another
callable accepting a `values` tuple and returning a row in the desired form.
This can be implemented as a class, for instance:

.. code:: python

   from typing import Any, Sequence
   from psycopg3 import AnyCursor

   class DictRowFactory:
       def __init__(self, cursor: AnyCursor[dict[str, Any]]):
           self.fields = [c.name for c in cursor.description]

       def __call__(self, values: Sequence[Any]) -> dict[str, Any]:
           return dict(zip(self.fields, values))

or as a plain function:

.. code:: python

   def dict_row_factory(
       cursor: AnyCursor[dict[str, Any]]
   ) -> Callable[[Sequence[Any]], dict[str, Any]]:
       fields = [c.name for c in cursor.description]

       def make_row(values: Sequence[Any]) -> dict[str, Any]:
           return dict(zip(fields, values))

       return make_row

These can then be used by specifying a `row_factory` argument in
`Connection.connect()`, `Connection.cursor()`, or by writing to
`Connection.row_factory` attribute.

.. code:: python

    conn = psycopg3.connect(row_factory=DictRowFactory)
    cur = conn.execute("SELECT first_name, last_name, age FROM persons")
    person = cur.fetchone()
    print(f"{person['first_name']} {person['last_name']}")

Later usages of `row_factory` override earlier definitions; for instance,
the `row_factory` specified at `Connection.connect()` can be overridden by
passing another value at `Connection.cursor()`.

.. note:: By declaring type annotations on the row factory used in
   `Connection` or `Cursor`, rows retrieved by `.fetch*()` calls will have the
   correct return type (i.e. a `dict[str, Any]` in previous example) and your
   code can be type checked with a static analyzer such as mypy.

Available row factories
-----------------------

The module `psycopg3.rows` provides the implementation for a few row factories:

.. currentmodule:: psycopg3.rows

.. autofunction:: tuple_row
.. autofunction:: dict_row
.. autofunction:: namedtuple_row
