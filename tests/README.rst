psycopg test suite
===================

Quick version
-------------

To run tests on the current code you can install the `test` extra of the
package, specify a connection string  in the `PSYCOPG_TEST_DSN` env var to
connect to a test database, and run ``pytest``::

    $ pip install -e "psycopg[test]"
    $ export PSYCOPG_TEST_DSN="host=localhost dbname=psycopg_test"
    $ pytest


Test options
------------

- The tests output header shows additional psycopg related information,
  on top of the one normally displayed by ``pytest`` and the extensions used::

      $ pytest
      ========================= test session starts =========================
      platform linux -- Python 3.8.5, pytest-6.0.2, py-1.10.0, pluggy-0.13.1
      Using --randomly-seed=2416596601
      libpq available: 130002
      libpq wrapper implementation: c


- By default the tests run using the ``pq`` implementation that psycopg would
  choose (the C module if installed, else the Python one). In order to test a
  different implementation, use the normal `pq module selection mechanism`__
  of the ``PSYCOPG_IMPL`` env var::

      $ PSYCOPG_IMPL=python pytest 
      ========================= test session starts =========================
      [...]
      libpq available: 130002
      libpq wrapper implementation: python

  .. __: https://www.psycopg.org/psycopg/docs/api/pq.html#pq-module-implementations


- Slow tests have a ``slow`` marker which can be selected to reduce test
  runtime to a few seconds only. Please add a ``@pytest.mark.slow`` marker to
  any test needing an arbitrary wait. At the time of writing::

      $ pytest
      ========================= test session starts =========================
      [...]
      ======= 1983 passed, 3 skipped, 110 xfailed in 78.21s (0:01:18) =======

      $ pytest -m "not slow"
      ========================= test session starts =========================
      [...]
      ==== 1877 passed, 2 skipped, 169 deselected, 48 xfailed in 13.47s =====

- ``pytest`` option ``--pq-trace={TRACEFILE,STDERR}`` can be used to capture
  libpq trace. When using ``stderr``, the output will only be shown for
  failing or in-error tests, unless ``-s/--capture=no`` option is used.

- ``pytest`` option ``--pq-debug`` can be used to log access to libpq's
  ``PGconn`` functions.


Testing in docker
-----------------

Useful to test different Python versions without installing them. Can be used
to replicate GitHub actions failures, specifying the ``--randomly-seed`` used
in the test run. The following ``PG*`` env vars are an example to adjust the
test dsn in order to connect to a database running on the docker host: specify
a set of env vars working for your setup::

    $ docker run -ti --rm --volume `pwd`:/src --workdir /src \
      -e PSYCOPG_TEST_DSN -e PGHOST=172.17.0.1 -e PGUSER=`whoami` \
      python:3.7 bash

    # pip install -e "./psycopg[test]" ./psycopg_pool ./psycopg_c
    # pytest


Testing with CockroachDB
========================

You can run CRDB in a docker container using::

    docker run -p 26257:26257 --name crdb --rm \
        cockroachdb/cockroach:v22.1.3 start-single-node --insecure

And use the following connection string to run the tests::

    export PSYCOPG_TEST_DSN="host=localhost port=26257 user=root dbname=defaultdb"
    pytest ...
