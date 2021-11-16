.. currentmodule:: psycopg_pool

.. index::
    single: Release notes
    single: News

``psycopg_pool`` release notes
==============================

Current release
---------------

psycopg_pool 4.0.0
^^^^^^^^^^^^^^^^^^

- Open pool (i.e. start worker tasks) when entering context manager instead of
  at object initialization (:ticket:`#155`).
- Add `ConnectionPool.open()` and `AsyncConnectionPool.open()`
  (:ticket:`#155`).
- Raise an `~psycopg.OperationalError` when trying to re-open a closed pool
  (:ticket:`#155`).

psycopg_pool 3.0.2
^^^^^^^^^^^^^^^^^^

- Remove dependency on the internal `!psycopg._compat` module.


psycopg_pool 3.0.1
^^^^^^^^^^^^^^^^^^

- Don't leave connections idle in transaction after calling
  `~ConnectionPool.check()` (:ticket:`#144`).


psycopg_pool 3.0
----------------

- First release on PyPI.
