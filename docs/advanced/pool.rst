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


Pool life cycle
---------------

A simple way to use the pool is to create a single instance of it, as a
global object, and to use this object in the rest of the program, allowing
other functions, modules, threads to use it::

    # module db.py in your program
    from psycopg_pool import ConnectionPool

    pool = ConnectionPool(conninfo, **kwargs)
    # the pool starts connecting immediately.

    # in another module
    from .db import pool

    def my_function():
        with pool.connection() as conn:
            conn.execute(...)

Ideally you may want to call `~ConnectionPool.close()` when the use of the
pool is finished. Failing to call `!close()` at the end of the program is not
terribly bad: probably it will just result in some warnings printed on stderr.
However, if you think that it's sloppy, you could use the `atexit` module to
have `!close()` called at the end of the program.

If you want to avoid starting to connect to the database at import time, and
want to wait for the application to be ready, you can create the pool using
`!open=False`, and call the `~ConnectionPool.open()` and
`~ConnectionPool.close()` methods when the conditions are right. Certain
frameworks provide callbacks triggered when the program is started and stopped
(for instance `FastAPI startup/shutdown events`__): they are perfect to
initiate and terminate the pool operations::

    pool = ConnectionPool(conninfo, open=False, **kwargs)

    @app.on_event("startup")
    def open_pool():
        pool.open()

    @app.on_event("shutdown")
    def close_pool():
        pool.close()

.. __: https://fastapi.tiangolo.com/advanced/events/#events-startup-shutdown

Creating a single pool as a global variable is not the mandatory use: your
program can create more than one pool, which might be useful to connect to
more than one database, or to provide different types of connections, for
instance to provide separate read/write and read-only connections. The pool
also acts as a context manager and is open and closed, if necessary, on
entering and exiting the context block::

    from psycopg_pool import ConnectionPool

    with ConnectionPool(conninfo, **kwargs) as pool:
        run_app(pool)

    # the pool is now closed

When the pool is open, the pool's background workers start creating the
requested `!min_size` connections, while the constructor (or the `!open()`
method) returns immediately. This allows the program some leeway to start
before the target database is up and running.  However, if your application is
misconfigured, or the network is down, it means that the program will be able
to start, but the threads requesting a connection will fail with a
`PoolTimeout` only after the timeout on `~ConnectionPool.connection()` is
expired. If this behaviour is not desirable (and you prefer your program to
crash hard and fast, if the surrounding conditions are not right, because
something else will respawn it) you should call the `~ConnectionPool.wait()`
method after creating the pool, or call `!open(wait=True)`: these methods will
block until the pool is full, or will raise a `PoolTimeout` exception if the
pool isn't ready within the allocated time.


Connections life cycle
----------------------

The pool background workers create connections according to the parameters
`!conninfo`, `!kwargs`, and `!connection_class` passed to `ConnectionPool`
constructor, invoking something like :samp:`{connection_class}({conninfo},
**{kwargs})`. Once a connection is created it is also passed to the
`!configure()` callback, if provided, after which it is put in the pool (or
passed to a client requesting it, if someone is already knocking at the door).

If a connection expires (it passes `!max_lifetime`), or is returned to the pool
in broken state, or is found closed by `~ConnectionPool.check()`), then the
pool will dispose of it and will start a new connection attempt in the
background.


Using connections from the pool
-------------------------------

The pool can be used to request connections from multiple threads or
concurrent tasks - it is hardly useful otherwise! If more connections than the
ones available in the pool are requested, the requesting threads are queued
and are served a connection as soon as one is available, either because
another client has finished using it or because the pool is allowed to grow
(when `!max_size` > `!min_size`) and a new connection is ready.

The main way to use the pool is to obtain a connection using the
`~ConnectionPool.connection()` context, which returns a `~psycopg.Connection`
or subclass::

    with my_pool.connection() as conn:
        conn.execute("what you want")

The `!connection()` context behaves like the `~psycopg.Connection` object
context: at the end of the block, if there is a transaction open, it will be
committed, or rolled back if the context is exited with as exception.

At the end of the block the connection is returned to the pool and shouldn't
be used anymore by the code which obtained it. If a `!reset()` function is
specified in the pool constructor, it is called on the connection before
returning it to the pool. Note that the `!reset()` function is called in a
worker thread, so that the thread which used the connection can keep its
execution without being slowed down by it.


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


Connection quality
------------------

The state of the connection is verified when a connection is returned to the
pool: if a connection is broken during its usage it will be discarded on
return and a new connection will be created.

.. warning::

    The health of the connection is not checked when the pool gives it to a
    client.

Why not? Because doing so would require an extra network roundtrip: we want to
save you from its latency. Before getting too angry about it, just think that
the connection can be lost any moment while your program is using it. As your
program should already be able to cope with a loss of a connection during its
process, it should be able to tolerate to be served a broken connection:
unpleasant but not the end of the world.

.. warning::

    The health of the connection is not checked when the connection is in the
    pool.

Does the pool keep a watchful eye on the quality of the connections inside it?
No, it doesn't. Why not? Because you will do it for us! Your program is only
a big ruse to make sure the connections are still alive...

Not (entirely) trolling: if you are using a connection pool, we assume that
you are using and returning connections at a good pace. If the pool had to
check for the quality of a broken connection before your program notices it,
it should be polling each connection even faster than your program uses them.
Your database server wouldn't be amused...

Can you do something better than that? Of course you can, there is always a
better way than polling. You can use the same recipe of :ref:`disconnections`,
reserving a connection and using a thread to monitor for any activity
happening on it. If any activity is detected, you can call the pool
`~ConnectionPool.check()` method, which will run a quick check on each
connection in the pool, removing the ones found in broken state, and using the
background workers to replace them with fresh ones.

If you set up a similar check in your program, in case the database connection
is temporarily lost, we cannot do anything for the threads which had taken
already a connection from the pool, but no other thread should be served a
broken connection, because `!check()` would empty the pool and refill it with
working connections, as soon as they are available.

Faster than you can say poll. Or pool.


.. _idle-session-timeout:

Pool and ``idle_session_timeout`` setting
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using a connection pool is fundamentally incompatible with setting an
`idle_session_timeout`__ on the connection: the pool is designed precisely to
keep connections idle and readily available.

.. __: https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-IDLE-SESSION-TIMEOUT

The current implementation doesn't keep ``idle_session_timeout`` into account,
so, if this setting is used, clients might be served broken connections and
fail with an error such as *terminating connection due to idle-session
timeout*.

In order to avoid the problem, please disable ``idle_session_timeout`` for the
pool connections. Note that, even if your server is configured with a nonzero
``idle_session_timeout`` default, you can still obtain pool connections
without timeout, by using the `!options` keyword argument, for instance::

    p = ConnectionPool(conninfo, kwargs={"options": "-c idle_session_timeout=0"})

.. warning::

    The `!max_idle` parameter is currently only used to shrink the pool if
    there are unused connections; it is not designed to fight against a server
    configured to close connections under its feet.


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
                        `~ConnectionPool.check()`
======================= =====================================================
