psycopg3 -- PostgreSQL database adapter for Python
==================================================

psycopg3 is a modern implementation of a PostgreSQL adapter for Python.


Installation
------------

The library is still in early development stage. If you want to try it out you
can install it from source using::

    pip install -e git+https://github.com/psycopg/psycopg3.git#egg=psycopg3


Hacking
-------

We assume you have built your virtualenv and ``pip`` just works and ``python``
refers to Python 3. You can set up a dev environment with::

    python setup.py develop

All the available tests and dev support are defined in ``tox.ini``: please
refer to `tox documentation`__ for its usage. You can run all the tests with::

    psql -c 'create database psycopg3_test'
    export PSYCOPG3_TEST_DSN="dbname=psycopg3_test"
    tox -s

You can also install the test dependencies in your virtualenv to run tests
faster: please look at the ``tox.ini`` comments for instructions.

.. __: https://tox.readthedocs.io/
