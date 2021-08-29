.. _psycopg.conninfo:

`conninfo` -- manipulate connection strings
===========================================

This module contains a few utility functions to manipulate database
connection strings.

.. module:: psycopg.conninfo

.. autofunction:: conninfo_to_dict

   .. code:: python

       >>> conninfo_to_dict("postgres://jeff@example.com/db", user="piro")
       {'user': 'piro', 'dbname': 'db', 'host': 'example.com'}


.. autofunction:: make_conninfo

   .. code:: python

        >>> make_conninfo("dbname=db user=jeff", user="piro", port=5432)
        'dbname=db user=piro port=5432'
