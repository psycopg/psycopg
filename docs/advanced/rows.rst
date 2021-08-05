.. currentmodule:: psycopg

.. index:: row factories

.. _row-factories:

Row factories
=============

Cursor's `fetch*` methods return tuples of column values by default. This can
be changed to adapt the needs of the programmer by using custom *row
factories*.

A row factory (formally implemented by the `~psycopg.rows.RowFactory`
protocol) is a callable that accepts a `Cursor` object and returns another
callable (formally the `~psycopg.rows.RowMaker` protocol) accepting a
`values` tuple and returning a row in the desired form.

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
to access `~Cursor.description`, which is provided by both the cursor flavours.

`~RowFactory` objects can be implemented as a class, for instance:

.. code:: python

   from typing import Any, Sequence
   from psycopg import Cursor

   class DictRowFactory:
       def __init__(self, cursor: Cursor[dict[str, Any]]):
           self.fields = [c.name for c in cursor.description]

       def __call__(self, values: Sequence[Any]) -> dict[str, Any]:
           return dict(zip(self.fields, values))

or as a plain function:

.. code:: python

   def dict_row_factory(cursor: Cursor[dict[str, Any]]) -> RowMaker[dict[str, Any]]:
       fields = [c.name for c in cursor.description]

       def make_row(values: Sequence[Any]) -> dict[str, Any]:
           return dict(zip(fields, values))

       return make_row

These can then be used by specifying a `row_factory` argument in
`Connection.connect()`, `Connection.cursor()`, or by writing to
`Connection.row_factory` attribute.

.. code:: python

    conn = psycopg.connect(row_factory=DictRowFactory)
    cur = conn.execute("SELECT first_name, last_name, age FROM persons")
    person = cur.fetchone()
    print(f"{person['first_name']} {person['last_name']}")

Later usages of `row_factory` override earlier definitions; for instance,
the `row_factory` specified at `Connection.connect()` can be overridden by
passing another value at `Connection.cursor()`.


Available row factories
-----------------------

The module `psycopg.rows` provides the implementation for a few row factories:

.. currentmodule:: psycopg.rows

.. autofunction:: tuple_row
.. autofunction:: dict_row
.. autofunction:: namedtuple_row
.. autofunction:: class_row
.. autofunction:: args_row
.. autofunction:: kwargs_row

    This is not a row factory, but rather a factory of row factories.
    Specifying ``row_factory=class_row(MyClass)`` will create connections and
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


Use with a static analyzer
--------------------------

The `~psycopg.Connection` and `~psycopg.Cursor` classes are `generic
types`__: the parameter `!Row` is passed by the ``row_factory`` argument (of
the `~Connection.connect()` and the `~Connection.cursor()` method) and it
controls what type of record is returned by the fetch methods of the cursors.
The default `tuple_row()` returns a generic tuple as return type (`Tuple[Any,
...]`). This information can be used for type checking using a static analyzer
such as mypy_.

.. _mypy: https://mypy.readthedocs.io/
.. __: https://mypy.readthedocs.io/en/stable/generics.html

.. code:: python

   conn = psycopg.connect()
   # conn type is psycopg.Connection[Tuple[Any, ...]]

   dconn = psycopg.connect(row_factory=dict_row)
   # dconn type is psycopg.Connection[Dict[str, Any]]

   cur = conn.cursor()
   # cur type is psycopg.Cursor[Tuple[Any, ...]]

   dcur = conn.cursor(row_factory=dict_row)
   dcur = dconn.cursor()
   # dcur type is psycopg.Cursor[Dict[str, Any]] in both cases

   rec = cur.fetchone()
   # rec type is Optional[Tuple[Any, ...]]

   drec = dcur.fetchone()
   # drec type is Optional[Dict[str, Any]]


Example: returning records as Pydantic models
---------------------------------------------

Using Pydantic_ it is possible to enforce static typing at runtime. Using a
Pydantic model factory the code can be checked statically using mypy and
querying the database will raise an exception if the rows returned is not
compatible with the model.

.. _Pydantic: https://pydantic-docs.helpmanual.io/

The following example can be checked with ``mypy --strict`` without reporting
any issue. Pydantic will also raise a runtime error in case the
`!PersonFactory` is used with a query that returns incompatible data.

.. code:: python

    from datetime import date
    from typing import Optional

    import psycopg
    from psycopg.rows import class_row
    from pydantic import BaseModel

    class Person(BaseModel):
        id: int
        first_name: str
        last_name: str
        dob: Optional[date]

    def fetch_person(id: int) -> Person:
        with psycopg.connect() as conn:
            with conn.cursor(row_factory=class_row(Person)) as cur:
                cur.execute(
                    """
                    SELECT id, first_name, last_name, dob
                    FROM (VALUES
                        (1, 'John', 'Doe', '2000-01-01'::date),
                        (2, 'Jane', 'White', NULL)
                    ) AS data (id, first_name, last_name, dob)
                    WHERE id = %(id)s;
                    """,
                    {"id": id},
                )
                obj = cur.fetchone()

                # reveal_type(obj) would return 'Optional[Person]' here

                if not obj:
                    raise KeyError(f"person {id} not found")

                # reveal_type(obj) would return 'Person' here

                return obj

    for id in [1, 2]:
        p = fetch_person(id)
        if p.dob:
            print(f"{p.first_name} was born in {p.dob.year}")
        else:
            print(f"Who knows when {p.first_name} was born")
