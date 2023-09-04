.. currentmodule:: psycopg

.. index:: row factories

.. _row-factories:

Row factories
=============

Cursor's `fetch*` methods, by default, return the records received from the
database as tuples. This can be changed to better suit the needs of the
programmer by using custom *row factories*.

The module `psycopg.rows` exposes several row factories ready to be used. For
instance, if you want to return your records as dictionaries, you can use
`~psycopg.rows.dict_row`::

    >>> from psycopg.rows import dict_row

    >>> conn = psycopg.connect(DSN, row_factory=dict_row)

    >>> conn.execute("select 'John Doe' as name, 33 as age").fetchone()
    {'name': 'John Doe', 'age': 33}

The `!row_factory` parameter is supported by the `~Connection.connect()`
method and the `~Connection.cursor()` method. Later usage of `!row_factory`
overrides a previous one. It is also possible to change the
`Connection.row_factory` or `Cursor.row_factory` attributes to change what
they return::

    >>> cur = conn.cursor(row_factory=dict_row)
    >>> cur.execute("select 'John Doe' as name, 33 as age").fetchone()
    {'name': 'John Doe', 'age': 33}

    >>> from psycopg.rows import namedtuple_row
    >>> cur.row_factory = namedtuple_row
    >>> cur.execute("select 'John Doe' as name, 33 as age").fetchone()
    Row(name='John Doe', age=33)

If you want to return objects of your choice you can use a row factory
*generator*, for instance `~psycopg.rows.class_row` or
`~psycopg.rows.args_row`, or you can :ref:`write your own row factory
<row-factory-create>`::

    >>> from dataclasses import dataclass

    >>> @dataclass
    ... class Person:
    ...     name: str
    ...     age: int
    ...     weight: Optional[int] = None

    >>> from psycopg.rows import class_row
    >>> cur = conn.cursor(row_factory=class_row(Person))
    >>> cur.execute("select 'John Doe' as name, 33 as age").fetchone()
    Person(name='John Doe', age=33, weight=None)


.. index::
    single: Row Maker
    single: Row Factory

.. _row-factory-create:

Creating new row factories
--------------------------

A *row factory* is a callable that accepts a `Cursor` object and returns
another callable, a *row maker*, which takes raw data (as a sequence of
values) and returns the desired object.

The role of the row factory is to inspect a query result (it is called after a
query is executed and properties such as `~Cursor.description` and
`~Cursor.pgresult` are available on the cursor) and to prepare a callable
which is efficient to call repeatedly (because, for instance, the names of the
columns are extracted, sanitised, and stored in local variables).

Formally, these objects are represented by the `~psycopg.rows.RowFactory` and
`~psycopg.rows.RowMaker` protocols.

`~RowFactory` objects can be implemented as a class, for instance:

.. code:: python

   from typing import Any, Sequence
   from psycopg import Cursor

   class DictRowFactory:
       def __init__(self, cursor: Cursor[Any]):
           self.fields = [c.name for c in cursor.description]

       def __call__(self, values: Sequence[Any]) -> dict[str, Any]:
           return dict(zip(self.fields, values))

or as nested functions:

.. code:: python

   def dict_row_factory(cursor: Cursor[Any]) -> RowMaker[dict[str, Any]]:
       fields = [c.name for c in cursor.description]

       def make_row(values: Sequence[Any]) -> dict[str, Any]:
           return dict(zip(fields, values))

       return make_row

These can then be used by specifying a `row_factory` argument in
`Connection.connect()`, `Connection.cursor()`, or by setting the
`Connection.row_factory` attribute.

.. code:: python

    conn = psycopg.connect(row_factory=DictRowFactory)
    cur = conn.execute("SELECT first_name, last_name, age FROM persons")
    person = cur.fetchone()
    print(f"{person['first_name']} {person['last_name']}")
