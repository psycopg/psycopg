.. _psycopg.rows:

`rows` -- row factory implementations
=====================================

.. module:: psycopg.rows

The module exposes a few generic `~psycopg.RowFactory` implementation, which
can be used to retrieve data from the database in more complex structures than
the basic tuples.

Check out :ref:`row-factory-create` for information about how to use these objects.

.. autofunction:: tuple_row
.. autofunction:: dict_row
.. autofunction:: namedtuple_row
.. autofunction:: class_row

    This is not a row factory, but rather a factory of row factories.
    Specifying `!row_factory=class_row(MyClass)` will create connections and
    cursors returning `!MyClass` objects on fetch.

    Example::

        from dataclasses import dataclass
        import psycopg
        from psycopg.rows import class_row

        @dataclass
        class Person:
            first_name: str
            last_name: str
            age: int = None

        conn = psycopg.connect()
        cur = conn.cursor(row_factory=class_row(Person))

        cur.execute("select 'John' as first_name, 'Smith' as last_name").fetchone()
        # Person(first_name='John', last_name='Smith', age=None)

.. autofunction:: args_row
.. autofunction:: kwargs_row


Formal rows protocols
---------------------

These objects can be used to describe your own rows adapter for static typing
checks, such as mypy_.

.. _mypy: https://mypy.readthedocs.io/


.. autoclass:: psycopg.rows.RowMaker()

   .. method:: __call__(values: Sequence[Any]) -> Row

        Convert a sequence of values from the database to a finished object.


.. autoclass:: psycopg.rows.RowFactory()

   .. method:: __call__(cursor: Cursor[Row]) -> RowMaker[Row]

        Inspect the result on a cursor and return a `RowMaker` to convert rows.

.. autoclass:: psycopg.rows.AsyncRowFactory()

.. autoclass:: psycopg.rows.BaseRowFactory()

Note that it's easy to implement an object implementing both `!RowFactory` and
`!AsyncRowFactory`: usually, everything you need to implement a row factory is
to access the cursor's `~psycopg.Cursor.description`, which is provided by
both the cursor flavours.
