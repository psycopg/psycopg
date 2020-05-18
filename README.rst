psycopg3 -- PostgreSQL database adapter for Python
==================================================

psycopg3 is a modern implementation of a PostgreSQL adapter for Python.


Installation
------------

The library is still in early development stage. If you want to try it out you
can install it from source using::

    pip install -e git+https://github.com/psycopg/psycopg3.git#egg=psycopg3


Contributing
------------

Install requirements-dev.txt for needed packages and lint tools.

Assuming PostgreSQL is installed, create a test database.

To run tests locally::

     pytest --test-dsn=dbname=YOUR_DB_NAME

