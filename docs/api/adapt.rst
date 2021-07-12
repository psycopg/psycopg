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


Dumpers and loaders
-------------------

.. autoclass:: Dumper(cls, context=None)

    This is an abstract base class: subclasses *must* at least implement the
    `dump()` method and specify the `format`.

    The class implements the `~psycopg.abc.Dumper` protocol.

    .. automethod:: dump

    .. automethod:: quote

    .. automethod:: get_key

    .. automethod:: upgrade


.. autoclass:: Loader(oid, context=None)

    This is an abstract base class: subclasses *must* at least implement the
    `!load()` method and specify a `format`.

    The class implements the `~psycopg.abc.Loader` protocol.

    .. automethod:: load


Other objects used in adaptations
---------------------------------

.. autoclass:: PyFormat
    :members:


.. data:: psycopg.adapters

   The global, default adapters map establishing how Python and PostgreSQL
   types are converted into each other. This map is used as template when new
   connections are created, using `psycopg.connect()`.

   :type: `~psycopg.adapt.AdaptersMap`


.. autoclass:: AdaptersMap

   .. automethod:: register_dumper
   .. automethod:: register_loader

   .. attribute:: types

       The object where to look up for types information (such as the mapping
       between type names and oids in the specified context).

       :type: `~psycopg.types.TypesRegistry`

   .. automethod:: get_dumper
   .. automethod:: get_loader


.. autoclass:: Transformer(context=None)

    :param context: The context where the transformer should operate.
    :type context: `~psycopg.abc.AdaptContext`
