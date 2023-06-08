.. currentmodule:: psycopg_pool

.. index::
    single: Release notes
    single: News

``psycopg_pool`` release notes
==============================

Future releases
---------------

psycopg_pool 3.2.0 (unreleased)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Add support for async `!reconnect_failed` callbacks in `AsyncConnectionPool`
  (:ticket:`#520`).


psycopg_pool 3.1.8 (unreleased)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Enforce connections' ``max_lifetime`` on `~ConnectionPool.check()`
  (:ticket:`#482`).


Current release
---------------

psycopg_pool 3.1.7
^^^^^^^^^^^^^^^^^^

- Fix handling of tasks cancelled while waiting in async pool queue
  (:ticket:`#503`).


psycopg_pool 3.1.6
^^^^^^^^^^^^^^^^^^

- Declare all parameters in pools constructors, instead of using `!**kwargs`
  (:ticket:`#493`).


psycopg_pool 3.1.5
^^^^^^^^^^^^^^^^^^

- Make sure that `!ConnectionPool.check()` refills an empty pool
  (:ticket:`#438`).
- Avoid error in Pyright caused by aliasing `!TypeAlias` (:ticket:`#439`).


psycopg_pool 3.1.4
^^^^^^^^^^^^^^^^^^

- Fix async pool exhausting connections, happening if the pool is created
  before the event loop is started (:ticket:`#219`).


psycopg_pool 3.1.3
^^^^^^^^^^^^^^^^^^

- Add support for Python 3.11 (:ticket:`#305`).


psycopg_pool 3.1.2
^^^^^^^^^^^^^^^^^^

- Fix possible failure to reconnect after losing connection from the server
  (:ticket:`#370`).


psycopg_pool 3.1.1
^^^^^^^^^^^^^^^^^^

- Fix race condition on pool creation which might result in the pool not
  filling (:ticket:`#230`).


psycopg_pool 3.1.0
------------------

- Add :ref:`null-pool` (:ticket:`#148`).
- Add `ConnectionPool.open()` and ``open`` parameter to the pool init
  (:ticket:`#151`).
- Drop support for Python 3.6.


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
