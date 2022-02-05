.. currentmodule:: psycopg_pool

.. index::
    single: Release notes
    single: News

``psycopg_pool`` release notes
==============================

Current release
---------------

psycopg_pool 3.1.0
------------------

- Add :ref:`null-pool` (:ticket:`#148`).
- Add `ConnectionPool.open()` and *open* parameter to the pool init
  (:ticket:`#151`).


psycopg_pool 3.0.3
^^^^^^^^^^^^^^^^^^

- Raise `!ValueError` if `ConnectionPool` `!min_size` and `!max_size` are both
  set to 0 (instead of hanging).
- Raise `PoolClosed` calling `~ConnectionPool.wait()` on a closed pool.

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
