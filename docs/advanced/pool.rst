.. currentmodule:: psycopg_pool

.. _connection-pools:

Connection pools
================

A `connection pool`__ is an object managing a set of connections and allowing
their use in functions needing one. Because the time to establish a new
connection can be relatively long, keeping connections open can reduce latency.

.. __: https://en.wikipedia.org/wiki/Connection_pool

This page explains a few basic concepts of Psycopg connection pool's
behaviour. Please refer to the `ConnectionPool` object API for details about
the pool operations.

.. note:: The connection pool objects are distributed in a package separate
   from the main `psycopg` package: use ``pip install "psycopg[pool]"`` or ``pip
   install psycopg_pool`` to make the `psycopg_pool` package available. See
   :ref:`pool-installation`.


Basic connection pool usage
---------------------------

A `ConnectionPool` object can be used to request connections from multiple
concurrent threads. A simple and safe way to use it is as a *context manager*.
Within the `!with` block, you can request the pool a connection using the
`~ConnectionPool.connection()` method, and use it as a context manager too::

    with ConnectionPool(...) as pool:
        with pool.connection() as conn:
            conn.execute("SELECT something FROM somewhere ...")

            with conn.cursor() as cur:
                cur.execute("SELECT something else...")

        # At the end of the `connection()` context, the transaction is committed
        # or rolled back, and the connection returned to the pool

    # At the end of the pool context, all the resources used by the pool are released

The `!connection()` context behaves like the `~psycopg.Connection` object
context: at the end of the block, if there is a transaction open, it will be
committed if the context is exited normally, or rolled back if the context is
exited with an exception. See :ref:`transaction-context` for details.

The pool manages a certain amount of connections (between `!min_size` and
`!max_size`). If the pool has a connection ready in its state, it is served
immediately to the `~connection()` caller, otherwise the caller is put in a
queue and is served a connection as soon as it's available.

If instead of threads your application uses async code you can use the
`AsyncConnectionPool` instead and use the `!async` and `!await` keywords with
the methods requiring them::

    async with AsyncConnectionPool(...) as pool:
        async with pool.connection() as conn:
            await conn.execute("SELECT something FROM somewhere ...")

            with conn.cursor() as cur:
                await cur.execute("SELECT something else...")


Pool startup check
------------------

After a pool is open, it can accept new clients even if it doesn't have
`!min_size` connections ready yet. However, if the application is
misconfigured and cannot connect to the database server, the clients will
block until failing with a `PoolTimeout`.

If you want to make sure early in the application lifetime that the
environment is well configured, you can use the `~ConnectionPool.wait()` method
after opening the pool, which will block until `!min_size` connections have
been acquired, or fail with a `!PoolTimeout` if it doesn't happen in time::

    with ConnectionPool(...) as pool:
        pool.wait()
        use_the(pool)


Connections life cycle
----------------------

When the pool needs a new connection (because it was just opened, or because
an existing connection was closed, or because a spike of activity requires new
connections), it uses a background pool worker to prepare it in the background:

- the worker creates a connection according to the parameters `!conninfo`,
  `!kwargs`, and `!connection_class` passed to `ConnectionPool` constructor,
  calling something similar to :samp:`{connection_class}({conninfo},
  **{kwargs})`;

- if a `!configure` callback was provided, it is called with the new connection
  as parameter. This can be used, for instance, to configure the connection
  adapters.

Once the connection is prepared, it is stored in the pool state, or it is
passed to a client if someone is already in the requests queue.

When a client asks for a connection (typically entering a
`~ConnectionPool.connection()` context):

- if there is a connection available in the pool, it is served to the client
  immediately;

- if no connection is available, the client is put in a queue, and will be
  served a connection once one becomes available (because returned by another
  client or because a new one is created);

- if a `!check` callback was provided, it is called on the connection before
  passing the connection to the client. If the check fails, a new connection
  will be obtained.

When a client has finished to use the connection (typically at the end of the
context stared by `~ConnectionPool.connection()`):

- if there is a transaction open, the transaction is committed (if the block
  is exited normally) or rolled back (if it is exited with an exception);

- if a `!reset` callback was provided, the connection is passed to it, to
  allow application-specific cleanup if needed;

- if, along this process, the connection is found in broken state, or if it
  passed the `!max_lifetime` configured at pool creation, it is discarded and
  a new connection is requested to a worker;

- the connection is finally returned to the pool, or, if there are clients in
  the queue, to the first client waiting.


Debugging pool usage
--------------------

The pool uses the `logging` module to log some key operations to the
``psycopg.pool`` logger. If you are trying to debug the pool behaviour you may
try to log at least the ``INFO`` operations on that logger.

For example, the script:

.. code:: python

    import time
    import logging
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from psycopg_pool import ConnectionPool

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("psycopg.pool").setLevel(logging.INFO)

    pool = ConnectionPool(min_size=2)
    pool.wait()
    logging.info("pool ready")

    def square(n):
        with pool.connection() as conn:
            time.sleep(1)
            rec = conn.execute("SELECT %s * %s", (n, n)).fetchone()
            logging.info(f"The square of {n} is {rec[0]}.")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(square, n) for n in range(4)]
        for future in as_completed(futures):
            future.result()

might print something like:

.. code:: text

    2023-09-20 11:02:39,718 INFO psycopg.pool: waiting for pool 'pool-1' initialization
    2023-09-20 11:02:39,720 INFO psycopg.pool: adding new connection to the pool
    2023-09-20 11:02:39,720 INFO psycopg.pool: adding new connection to the pool
    2023-09-20 11:02:39,720 INFO psycopg.pool: pool 'pool-1' is ready to use
    2023-09-20 11:02:39,720 INFO root: pool ready
    2023-09-20 11:02:39,721 INFO psycopg.pool: connection requested from 'pool-1'
    2023-09-20 11:02:39,721 INFO psycopg.pool: connection given by 'pool-1'
    2023-09-20 11:02:39,721 INFO psycopg.pool: connection requested from 'pool-1'
    2023-09-20 11:02:39,721 INFO psycopg.pool: connection given by 'pool-1'
    2023-09-20 11:02:39,721 INFO psycopg.pool: connection requested from 'pool-1'
    2023-09-20 11:02:39,722 INFO psycopg.pool: connection requested from 'pool-1'
    2023-09-20 11:02:40,724 INFO root: The square of 0 is 0.
    2023-09-20 11:02:40,724 INFO root: The square of 1 is 1.
    2023-09-20 11:02:40,725 INFO psycopg.pool: returning connection to 'pool-1'
    2023-09-20 11:02:40,725 INFO psycopg.pool: connection given by 'pool-1'
    2023-09-20 11:02:40,725 INFO psycopg.pool: returning connection to 'pool-1'
    2023-09-20 11:02:40,726 INFO psycopg.pool: connection given by 'pool-1'
    2023-09-20 11:02:41,728 INFO root: The square of 3 is 9.
    2023-09-20 11:02:41,729 INFO root: The square of 2 is 4.
    2023-09-20 11:02:41,729 INFO psycopg.pool: returning connection to 'pool-1'
    2023-09-20 11:02:41,730 INFO psycopg.pool: returning connection to 'pool-1'

Please do not rely on the messages generated to remain unchanged across
versions: they don't constitute a stable interface.


Pool connection and sizing
--------------------------

A pool can have a fixed size (specifying no `!max_size` or `!max_size` =
`!min_size`) or a dynamic size (when `!max_size` > `!min_size`). In both
cases, as soon as the pool is created, it will try to acquire `!min_size`
connections in the background.

If an attempt to create a connection fails, a new attempt will be made soon
after, using an exponential backoff to increase the time between attempts,
until a maximum of `!reconnect_timeout` is reached. When that happens, the pool
will call the `!reconnect_failed()` function, if provided to the pool, and just
start a new connection attempt. You can use this function either to send
alerts or to interrupt the program and allow the rest of your infrastructure
to restart it.

If more than `!min_size` connections are requested concurrently, new ones are
created, up to `!max_size`. Note that the connections are always created by the
background workers, not by the thread asking for the connection: if a client
requests a new connection, and a previous client terminates its job before the
new connection is ready, the waiting client will be served the existing
connection. This is especially useful in scenarios where the time to establish
a connection dominates the time for which the connection is used (see `this
analysis`__, for instance).

.. __: https://github.com/brettwooldridge/HikariCP/blob/dev/documents/
       Welcome-To-The-Jungle.md

If a pool grows above `!min_size`, but its usage decreases afterwards, a number
of connections are eventually closed: one every time a connection is unused
after the `!max_idle` time specified in the pool constructor.


What's the right size for the pool?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Big question. Who knows. However, probably not as large as you imagine. Please
take a look at `this analysis`__ for some ideas.

.. __: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing

Something useful you can do is probably to use the
`~ConnectionPool.get_stats()` method and monitor the behaviour of your program
to tune the configuration parameters. The size of the pool can also be changed
at runtime using the `~ConnectionPool.resize()` method.


Connection quality
------------------

.. versionadded:: 3.2

The pool doesn't actively check the state of the connections held in its
state. This means that, if communication with the server is lost, or if a
connection is closed for other reasons (such as a server configured with an
`idle_session_timeout`__ killing connections that haven't been used for some
time), the application might be served a connection in broken state.

.. __: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-IDLE-SESSION-TIMEOUT

If you want to configure the pool to check the state of the connection, and
make sure that the application always receives a working connection, you can
configure a `!check` callback. The callback can perform some operation to
verify the quality of the connection and, if it completes without raising
exception, the connection is passed to the client. This, of course, will imply
some network time that the pool client will have to pay.

A simple implementation is available as the static method
`ConnectionPool.check_connection`, which can be used as::

    with ConnectionPool(
        ..., check=ConnectionPool.check_connection, ...
    ) as pool:
        ...


Other ways to create a pool
---------------------------

Using the pool as a context manager is not mandatory: pools can be created and
used without using the context pattern. However, using the context is the
safest way to manage its resources.

When the pool is created, if its `!open` parameter is `!True`, the connection
process starts immediately. In a simple program you might create a pool as a
global object and use it from the rest of your code::

    # module db.py in your program
    from psycopg_pool import ConnectionPool

    pool = ConnectionPool(..., open=True, ...)
    # the pool starts connecting immediately.

    # in another module
    from .db import pool

    def my_function():
        with pool.connection() as conn:
            conn.execute(...)

Using this pattern, the pool will start the connection process already at
import time. If that's too early, and you want to delay opening connections
until the application is ready, you can specify to create a closed pool and
call the `~ConnectionPool.open()` method (and optionally the
`~ClonnectionPool.close()` method) at application startup/shutdown. For
example, in FastAPI, you can use `startup/shutdown events`__::

    pool = ConnectionPool(..., open=False, ...)

    @app.on_event("startup")
    def open_pool():
        pool.open()

    @app.on_event("shutdown")
    def close_pool():
        pool.close()

.. __: https://fastapi.tiangolo.com/advanced/events/#events-startup-shutdown

.. warning::
    The current default for the `!open` parameter is `!True`. However this
    proved to be not the best idea and, in future releases, the default might
    be changed to `!False`. As a consequence, if you rely on the pool to be
    opened on creation, you should specify `!open=True` explicitly.


.. _null-pool:

Null connection pools
---------------------

.. versionadded:: 3.1

Sometimes you may want leave the choice of using or not using a connection
pool as a configuration parameter of your application. For instance, you might
want to use a pool if you are deploying a "large instance" of your application
and can dedicate it a handful of connections; conversely you might not want to
use it if you deploy the application in several instances, behind a load
balancer, and/or using an external connection pool process such as PgBouncer.

Switching between using or not using a pool requires some code change, because
the `ConnectionPool` API is different from the normal `~psycopg.connect()`
function and because the pool can perform additional connection configuration
(in the `!configure` parameter) that, if the pool is removed, should be
performed in some different code path of your application.

The `!psycopg_pool` 3.1 package introduces the `NullConnectionPool` class.
This class has the same interface, and largely the same behaviour, of the
`!ConnectionPool`, but doesn't create any connection beforehand. When a
connection is returned, unless there are other clients already waiting, it
is closed immediately and not kept in the pool state.

A null pool is not only a configuration convenience, but can also be used to
regulate the access to the server by a client program. If `!max_size` is set to
a value greater than 0, the pool will make sure that no more than `!max_size`
connections are created at any given time. If more clients ask for further
connections, they will be queued and served a connection as soon as a previous
client has finished using it, like for the basic pool. Other mechanisms to
throttle client requests (such as `!timeout` or `!max_waiting`) are respected
too.

.. note::

    Queued clients will be handed an already established connection, as soon
    as a previous client has finished using it (and after the pool has
    returned it to idle state and called `!reset()` on it, if necessary).

Because normally (i.e. unless queued) every client will be served a new
connection, the time to obtain the connection is paid by the waiting client;
background workers are not normally involved in obtaining new connections.


.. _pool-stats:

Pool stats
----------

The pool can return information about its usage using the methods
`~ConnectionPool.get_stats()` or `~ConnectionPool.pop_stats()`. Both methods
return the same values, but the latter reset the counters after its use. The
values can be sent to a monitoring system such as Graphite_ or Prometheus_.

.. _Graphite: https://graphiteapp.org/
.. _Prometheus: https://prometheus.io/

The following values should be provided, but please don't consider them as a
rigid interface: it is possible that they might change in the future. Keys
whose value is 0 may not be returned.


======================= =====================================================
Metric                  Meaning
======================= =====================================================
 ``pool_min``           Current value for `~ConnectionPool.min_size`
 ``pool_max``           Current value for `~ConnectionPool.max_size`
 ``pool_size``          Number of connections currently managed by the pool
                        (in the pool, given to clients, being prepared)
 ``pool_available``     Number of connections currently idle in the pool
 ``requests_waiting``   Number of requests currently waiting in a queue to
                        receive a connection
 ``usage_ms``           Total usage time of the connections outside the pool
 ``requests_num``       Number of connections requested to the pool
 ``requests_queued``    Number of requests queued because a connection wasn't
                        immediately available in the pool
 ``requests_wait_ms``   Total time in the queue for the clients waiting
 ``requests_errors``    Number of connection requests resulting in an error
                        (timeouts, queue full...)
 ``returns_bad``        Number of connections returned to the pool in a bad
                        state
 ``connections_num``    Number of connection attempts made by the pool to the
                        server
 ``connections_ms``     Total time spent to establish connections with the
                        server
 ``connections_errors`` Number of failed connection attempts
 ``connections_lost``   Number of connections lost identified by
                        `~ConnectionPool.check()` or by the `!check` callback
======================= =====================================================
