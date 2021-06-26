.. _installation:

Installation
============

Installing the development state
--------------------------------

Psycopg 3 packages have not been released yet, but you can try it out
already by installing it `from the GitHub project`__:

.. code:: bash

    $ pip install git+https://github.com/psycopg/psycopg.git#subdirectory=psycopg
    $ python3
    >>> import psycopg

.. __: https://github.com/psycopg/psycopg


.. _binary-install:

Binary installation
-------------------

.. warning::

    Psycopg 3 is still in a development phase: packages haven't been
    released yet on PyPI.

    Please refer to `the README`__ for the current installation state, and
    please know that things may change.

    .. __: https://github.com/psycopg/psycopg#readme

    The following is how it will be *supposed* to work, once it is released...


The quickest way to start developing with Psycopg 3 is to install the binary
packages by running::

    pip install psycopg[binary]

This will install a self-contained package with all the libraries needed.

The above package should work in most situations. It **will not work** in
some though:

- the ``binary`` package doesn't work on Alpine Linux;
- you have a newly released Python or Mac Os X version for which binary
  packages are not ready yet.

In these case you should proceed to a :ref:`local installation
<local-installation>` or a :ref:`pure Python installation
<pure-python-installation>`.

.. seealso::

    Did Psycopg 3 install ok? Great! You can now move on to the :ref:`basic
    module usage <module-usage>` to learn how it works.

    You can come back here if you the above method didn't work and you need a
    way to install Psycopg 3 past the basic one.


.. _local-installation:

Local installation
------------------

A "Local installation" means obtaining a performing and maintainable library.
The library will include a performing C module and will be bound to the system
libraries (``libpq``, ``libssl``...) so that system upgrade of libraries will
upgrade the libraries used by Psycopg 3 too.

In order to perform a local installation you need some prerequisites:

- a C compiler,
- Python development headers (e.g. the python3-dev package).
- PostgreSQL client development headers (e.g. the libpq-dev package).
- The :program:`pg_config` program available in the :envvar:`PATH`.

You **must be able** to troubleshoot an extension build, for instance you must
be able to read your compiler's error message. If you are not, please don't
try this and follow the `binary installation`_ instead.

If your build prerequisites are in place you can run::

    pip install psycopg[c]


.. _pure-python-installation:

Pure Python installation
------------------------

If you simply install::

    pip install psycopg

without ``[c]`` or ``[binary]`` extras you will obtain a pure Python
implementation. This is particularly handy to debug and hack, but it still
requires the system libpq to operate (which will be used dynamically via
`ctypes`).

In order to use the pure Python installation you will need the ``libpq``
installed in the system: for instance on Debian system you will probably
need::

    sudo apt-get install libpq5

If you are not able to fulfill this requirement please follow the `binary
installation`_.



Psycopg 3 and the ``libpq``
---------------------------

.. admonition:: TODO

    describe the relations between psycopg and the libpq and the binding
    choices
