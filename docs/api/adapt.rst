`adapt` -- Types adaptation
===========================

.. module:: psycopg.adapt

The `!psycopg.adapt` module exposes a set of objects useful for the
configuration of *data adaptation*, which is the conversion of Python objects
to PostgreSQL data types and back.

These objects are useful if you need to configure data adaptation, i.e.
if you need to change the default way that Psycopg converts between types or
if you want to adapt custom data types and objects. You don't need this object
in the normal use of Psycopg.

See :ref:`adaptation` for an overview of the Psycopg adaptation system.

.. _abstract base class: https://docs.python.org/glossary.html#term-abstract-base-class


Dumpers and loaders
-------------------

.. autoclass:: Dumper(cls, context=None)

    This is an `abstract base class`_, partially implementing the
    `~psycopg.abc.Dumper` protocol. Subclasses *must* at least implement the
    `.dump()` method and optionally override other members.

    .. automethod:: dump

    .. attribute:: format
        :type: psycopg.pq.Format
        :value: TEXT

        Class attribute. Set it to `~psycopg.pq.Format.BINARY` if the class
        `dump()` methods converts the object to binary format.

    .. automethod:: quote

    .. automethod:: get_key

    .. automethod:: upgrade


.. autoclass:: Loader(oid, context=None)

    This is an `abstract base class`_, partially implementing the
    `~psycopg.abc.Loader` protocol. Subclasses *must* at least implement the
    `.load()` method and optionally override other members.

    .. automethod:: load

    .. attribute:: format
        :type: psycopg.pq.Format
        :value: TEXT

        Class attribute. Set it to `~psycopg.pq.Format.BINARY` if the class
        `load()` methods converts the object from binary format.


Other objects used in adaptations
---------------------------------

.. autoclass:: PyFormat
    :members:


.. autoclass:: AdaptersMap

   .. seealso:: :ref:`adaptation` for an explanation about how contexts are
       connected.

   .. automethod:: register_dumper
   .. automethod:: register_loader

   .. attribute:: types

       The object where to look up for types information (such as the mapping
       between type names and oids in the specified context).

       :type: `~psycopg.types.TypesRegistry`

   .. automethod:: get_dumper
   .. automethod:: get_dumper_by_oid
   .. automethod:: get_loader


.. autoclass:: Transformer(context=None)

    :param context: The context where the transformer should operate.
    :type context: `~psycopg.abc.AdaptContext`
