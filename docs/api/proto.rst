`proto` -- Psycopg abstract classes
===================================

TODO: rename to abc

The module exposes Psycopg definitions which can be used for static type
checking.

.. module:: psycopg.proto

.. autoclass:: Dumper(cls, context=None)

    :param cls: The type that will be managed by this dumper.
    :type cls: type
    :param context: The context where the transformation is performed. If not
        specified the conversion might be inaccurate, for instance it will not
        be possible to know the connection encoding or the server date format.
    :type context: `AdaptContext` or None

    A partial implementation of this protocol (implementing everyting except
    `dump()`) is available as `psycopg.adapt.Dumper`.

    .. attribute:: format
        :type: pq.Format

        The format this class dumps, `~Format.TEXT` or `~Format.BINARY`.
        This is a class attribute.

    .. automethod:: dump

        The format returned by dump shouldn't contain quotes or escaped
        values.

    .. automethod:: quote

        .. tip::

            This method will be used by `~psycopg.sql.Literal` to convert a
            value client-side.

        This method only makes sense for text dumpers; the result of calling
        it on a binary dumper is undefined. It might scratch your car, or burn
        your cake. Don't tell me I didn't warn you.

    .. autoattribute:: oid

        If the oid is not specified, PostgreSQL will try to infer the type
        from the context, but this may fail in some contexts and may require a
        cast (e.g. specifying :samp:`%s::{type}` for its placeholder).

        .. admonition:: todo

            Document how to find type OIDs in a database.

    .. automethod:: get_key
    .. automethod:: upgrade


.. autoclass:: Loader(oid, context=None)

    :param oid: The type that will be managed by this dumper.
    :type oid: int
    :param context: The context where the transformation is performed. If not
        specified the conversion might be inaccurate, for instance it will not
        be possible to know the connection encoding or the server date format.
    :type context: `AdaptContext` or None

    A partial implementation of this protocol (implementing everyting except
    `load()`) is available as `psycopg.adapt.Loader`.

    .. attribute:: format
        :type: Format

        The format this class can load, `~Format.TEXT` or `~Format.BINARY`.
        This is a class attribute.

    .. automethod:: load


.. autoclass:: AdaptContext
    :members:
