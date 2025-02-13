Psycopg style isort
===================

This is an isort_ plugin implementing the style used in the `Psycopg 3`_
project to sort:

- imports in length order
- import lists in natural order

The effect is the same of specifying ``--length-sort`` but only for the module
names. For example::

    from ccc import aaaa, bbb, cc
    from bbbb import ddd, ee
    from aaaaa import fff, gg

Example configuration::

    [tool.isort]
    profile = "black"
    length_sort = true
    multi_line_output = 9
    sort_order = "psycopg"

Note: because this is the first day I use isort at all, there is a chance that
this plug-in is totally useless and the same can be done using isort features.

.. _isort: https://pycqa.github.io/isort/
.. _psycopg 3: https://www.psycopg.org/
