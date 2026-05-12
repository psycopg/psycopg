.. _psycopg.logical_rows:

`logical_rows` -- logical row factory implementations
=====================================================

.. module:: psycopg.replication.logical_output_plugins.logical_rows

The module exposes a few generic
`~psycopg.replication.logical_output_plugins.logical_rows.LogicalRowFactory`
implementations, which can be used to retrieve relation rows in a logical replication
stream in more complex structures than the basic tuples.

Check out :ref:`logical-row-factories` for information about how
to use these objects.

.. autofunction:: tuple_row

    Example::

        >>> decoder = PgOutputDecoder(row_factory=tuple_row)
        >>> logical_cur.start_replication(decoder=decoder)
        >>> conn.execute("INSERT INTO mytable (num, data) VALUES (10, 'hello')")
        >>> msg = logical_cur.read_message(return_keepalive_messages=False)
        >>> msg.payload.new_tuple
        (10, 'hello')

.. autofunction:: dict_row

    Example::

        >>> decoder = PgOutputDecoder(row_factory=tuple_row)
        >>> logical_cur.start_replication(decoder=decoder)
        >>> conn.execute("INSERT INTO mytable (num, data) VALUES (10, 'hello')")
        >>> msg = logical_cur.read_message(return_keepalive_messages=False)
        >>> msg.payload.new_tuple
        {'foo': 10, 'bar': 'hello'}

.. autofunction:: namedtuple_row

    Example::

        >>> decoder = PgOutputDecoder(row_factory=tuple_row)
        >>> logical_cur.start_replication(decoder=decoder)
        >>> conn.execute("INSERT INTO mytable (num, data) VALUES (10, 'hello')")
        >>> msg = logical_cur.read_message(return_keepalive_messages=False)
        >>> msg.payload.new_tuple
        Row(foo=10, bar='hello')


.. autofunction:: class_row

    This is not a row factory, but rather a factory of row factories.
    Specifying `!row_factory=class_row(MyClass)` will produce logical messages
    using `!MyClass` objects for rows.

    .. note::
        This may not be particularly useful for logical decoding, since
        the rows could come from arbitrary tables with differing schemas.
        Look at `kwargs_row` for dispatching these to appropriate
        classes.

.. autofunction:: args_row

.. autofunction:: kwargs_row


Formal logical rows protocols
-----------------------------

These objects can be used to describe your own rows adapter for static typing
checks, such as mypy_.

.. _mypy: https://mypy.readthedocs.io/


.. autoclass:: psycopg.replication.logical_output_plugins.logical_rows.LogicalRowMaker()

    .. method:: __call__(values: Sequence[Any]) -> LogicalRow

        Convert a sequence of values from the database to a finished object.


.. autoclass:: psycopg.replication.logical_output_plugins.logical_rows.LogicalRowFactory()

    .. method:: __call__(decoder: LogicalRowFactoryXLogDataDecoder[LogicalRow], relation_id: int) -> LogicalRowMaker[LogicalRow])

        Inspect the state of the decoder and the `relation_id` and return a
        `LogicalRowMaker` to convert rows.  The relation applying to the row can be
        retrieved by calling `LogicalRowFactoryXLogDataDecoder.get_relation(relation_id)`.
