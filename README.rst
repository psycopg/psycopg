psycopg3 -- PostgreSQL database adapter for Python
==================================================

psycopg3 is a modern implementation of a PostgreSQL adapter for Python.


Installation
------------

The library is still in a development stage and is not available on PyPI in
the form of packages yet. You can install it directly `from the GitHub
project`__::

    $ pip install git+https://github.com/psycopg/psycopg3.git#subdirectory=psycopg3
    $ python3
    >>> import psycopg3

.. __: https://github.com/psycopg/psycopg3

You are required to have the ``libpq``, the PostgreSQL client library, already
installed in the system before using ``psycopg3``. On Debian system you can
obtain it by running::

    sudo apt-get install libpq5

Please check your system's documentation for information about installing the
``libpq`` on your platform.


Hacking
-------

In order to work on the ``psycopg3`` source code you should clone this
repository::

    git clone https://github.com/psycopg/psycopg3.git
    cd psycopg3

Please note that the repository contains the source code of several Python
packages: that's why you don't see a ``setup.py`` here. The packages may have
different requirements:

- The ``psycopg3`` directory contains the pure python implementation of
  ``psycopg3``. The package has only a runtime dependency on the ``libpq``,
  the PostgreSQL client library, which should have been already installed in
  your system.

- The ``psycopg3_c`` directory contains an optimization module written in
  C/Cython. In order to build it you will need a few development tools: please
  look at `Local installation`__ in the docs for the details.

  .. __: https://www.psycopg.org/psycopg3/docs/install.html#local-installation

You can create a local virtualenv and install there the packages `in
development mode`__, together with their development and testing
requirements::

    python -m venv .venv
    source .venv/bin/activate
    pip install -e ./psycopg3[dev,test]     # for the base Python package
    pip install -e ./psycopg3_c             # for the C extension module

.. __: https://pip.pypa.io/en/stable/reference/pip_install/#install-editable

Now hack away! You can use tox to validate the code::

    pip install tox
    tox -p4

and to run the tests::

    psql -c 'create database psycopg3_test'
    export PSYCOPG3_TEST_DSN="dbname=psycopg3_test"
    tox -c psycopg3 -s
    tox -c psycopg3_c -s

Please look at the commands definitions in the ``tox.ini`` files if you want
to run some of them interactively: the dependency should be already in your
virtualenv. Feel free to adapt these recipes if you follow a different
development pattern.
