psycopg3 -- PostgreSQL database adapter for Python
==================================================

psycopg3 is a modern implementation of a PostgreSQL adapter for Python.

The package is split in different parts, with different requirements.

- The pure python package only depends on the **libpq**, the PostgreSQL client
  library. The code is in the ``psycopg3`` directory.

- The optional C optimization: in order to build it requires the **libpq-dev**
  packages, a C compiler and Cython. The code is in the ``psycopg3_c``
  directory.


Installation
------------

The library is still in early development stage. If you want to try it out you
can install it from source using::

    git clone https://github.com/psycopg/psycopg3.git
    cd psycopg3
    python psycopg3/setup.py install    # for the base Python package
    python psycopg3_c/setup.py install  # for the C extension module


Hacking
-------

You can create a local virtualenv and install there the dev and test
requirements. Feel free to adapt the following recipe if you follow a
different development pattern::

    python -m venv .venv
    source .venv/bin/activate
    python psycopg3/setup.py[dev,test] develop    # for the base Python pacakge
    python psycopg3_c/setup.py develop            # for the C extension module

You can use tox to validate the code::

    tox -p4

and to run the tests::

    psql -c 'create database psycopg3_test'
    export PSYCOPG3_TEST_DSN="dbname=psycopg3_test"
    tox -c psycopg3 -s
    tox -c psycopg3_c -s

Please look at the commands definitions in the ``tox.ini`` files if you want
to run some of them interacively: the dependency should be already in your
virtualenv.
