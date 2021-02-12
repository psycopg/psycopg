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

   class DictRowFactory:
       def __init__(self, cursor):
           self.cursor = cursor

       def __call__(self, values):
           fields = (c.name for c in self.cursor.description)
           return dict(zip(fields, values))

or as a plain function:

.. code:: python

   def dict_row_factory(cursor):
       def make_row(values):
           fields = (c.name for c in cursor.description)
           return dict(zip(fields, values))

       return make_row

These can then be used by specifying a `row_factory` argument in
`Connection.connect()`, `Connection.cursor()`, `Cursor.execute()` and
`Connection.execute()` or by writting to `Connection.row_factory` attribute.

.. code:: python

    conn = psycopg3.connect(row_factory=DictRowFactory)
    cur = conn.execute("SELECT first_name, last_name, age FROM persons")
    person = cur.fetchone()
    print(f"{person['first_name']} {person['last_name']}")

Later usages of `row_factory` override earlier definitions; for instance,
the `row_factory` specified at `Connection.connect()` can be overridden by
passing another value at `Connection.cursor()`.

Available row factories
-----------------------

Module `psycopg3.rows` contains available row factories:

.. currentmodule:: psycopg3.rows

.. autofunction:: dict_row
