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

   .. method:: __call__(cursor: AsyncCursor[Row]) -> RowMaker[Row]

        Inspect the result on a cursor and return a `RowMaker` to convert rows.

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
.. autodata:: TupleRow

.. autofunction:: dict_row
.. autodata:: DictRow

.. autofunction:: namedtuple_row

.. autofunction:: class_row

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
querying the database will raise an exception if the resultset is not
compatible with the model.

.. _Pydantic: https://pydantic-docs.helpmanual.io/

The following example can be checked with ``mypy --strict`` without reporting
any issue. Pydantic will also raise a runtime error in case the
`!PersonFactory` is used with a query that returns incompatible data.

.. code:: python

    from datetime import date
    from typing import Any, Optional, Sequence

    import psycopg
    from pydantic import BaseModel

    class Person(BaseModel):
        id: int
        first_name: str
        last_name: str
        dob: Optional[date]

    class PersonFactory:
        def __init__(self, cur: psycopg.Cursor[Person]):
            assert cur.description
            self.fields = [c.name for c in cur.description]

        def __call__(self, values: Sequence[Any]) -> Person:
            return Person(**dict(zip(self.fields, values)))

    def fetch_person(id: int) -> Person:
        conn = psycopg.connect()
        cur = conn.cursor(row_factory=PersonFactory)
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
        rec = cur.fetchone()
        if not rec:
            raise KeyError(f"person {id} not found")
        return rec

    for id in [1, 2]:
        p = fetch_person(id)
        if p.dob:
            print(f"{p.first_name} was born in {p.dob.year}")
        else:
            print(f"Who knows when {p.first_name} was born")


Another level of generic
^^^^^^^^^^^^^^^^^^^^^^^^

Note that, in the example above, the `!PersonFactory` implementation has
nothing specific to the `!Person` class, apart from the returned type itself.
This suggests that it's actually possible to create a... factory of factories:
a function that, given a Pydantic model, returns a RowFactory that can be used
to annotate connections and cursor statically.

In the example above, the `!PersonFactory` class can be implemented as a
function:

.. code:: python

    def person_factory(cursor: Cursor[Person]) -> RowMaker[Person]:
        assert cursor.description
        fields = [c.name for c in cursor.description]

        def person_factory_(values: Sequence[Any]) -> Person:
            return Person(**dict(zip(fields, values)))

        return person_factory_

The function `!person_factory()` is a `!RowFactory[Person]`. We can introduce
a generic `M`, which can be any Pydantic model, and write a function returning
`!RowFactory[M]`:

.. code:: python

    M = TypeVar("M", bound=BaseModel)

    def model_factory(model: Type[M]) -> RowFactory[M]:
        def model_factory_(cursor: Cursor[M]) -> RowMaker[M]:
            assert cursor.description
            fields = [c.name for c in cursor.description]

            def model_factory__(values: Sequence[Any]) -> M:
                return model(**dict(zip(fields, values)))

            return model_factory__

        return model_factory_

which can be used to declare the types of connections and cursors:

.. code:: python

    conn = psycopg.connect()
    cur = conn.cursor(row_factory=model_factory(Person))
    x = cur.fetchone()
    # the type of x is Optional[Person]
