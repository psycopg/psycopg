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
   from the main `psycopg` package: use ``pip install psycopg[pool]`` or ``pip
   install psycopg_pool`` to make the `psycopg_pool` package available. See
   :ref:`pool-installation`.


Pool life cycle
---------------

A typical way to use the pool is to create a single instance of it, as a
global object, open it, and to use this object in the rest of the program,
allowing other functions, modules, threads to use it. This is only a common
use however, and not the necessary one; in particular the connection pool acts
as a context manager and can be closed automatically at the end of its
``with`` block::

    from psycopg_pool import ConnectionPool

    with ConnectionPool(conninfo, **kwargs) as my_pool:
        run_app(my_pool)

    # the pool is now closed

If necessary, or convenient, your application may create more than one pool,
for instance to connect to more than one database or to provide separate
read-only and read/write connections.

When a pool is instantiated, the constructor returns immediately. It is then
required to open it to start the background workers that will try to create
the required number of connections to fill the pool. If your application is
misconfigured, or the network is down, it means that the pool will be
available but threads requesting a connection will fail with a `PoolTimeout`
after the `~ConnectionPool.connection()` timeout is expired. If this behaviour
is not desirable you should call the `~ConnectionPool.wait()` method after
opening the pool, which will block until the pool is full or will throw a
`PoolTimeout` if the pool isn't ready within an allocated time.

The pool background workers create connections according to the parameters
*conninfo*, *kwargs*, and *connection_class* passed to `ConnectionPool`
constructor. Once a connection is created it is also passed to the
*configure()* callback, if provided, after which it is put in the pool (or
passed to a client requesting it, if someone is already knocking at the door).
If a connection expires (it passes *max_lifetime*), or is returned to the pool
in broken state, or is found closed by `~ConnectionPool.check()`, then the
pool will dispose of it and will start a new connection attempt in the
background.

When the pool is no more to be used, you should call the
`~ConnectionPool.close()` method (unless the ``with`` syntax was used). If the
pool is a module-level object it may be unclear how to do so. Missing a call
to `!close()` shouldn't be a big problem, it should just result in a few
warnings printed. However, if you think that's sloppy, you can use the
`atexit` module to have the `!close()` method called at the end of the
program.


Using connections from the pool
-------------------------------

The pool can be used to request connections from multiple threads - it is
hardly useful otherwise! If more connections than the ones available in the
pool are requested, the requesting threads are queued and are served a
connection as soon as one is available again: either because another client
has finished using it or because the pool is allowed to grow and a new
connection is ready.

The main way to use the pool is to obtain a connection using the
`~ConnectionPool.connection()` context, which returns a `~psycopg.Connection`
or subclass::

    with my_pool.connection() as conn:
        conn.execute("what you want")

At the end of the block the connection is returned to the pool and shouldn't
be used anymore by the code which obtained it. If a *reset()* function is
specified in the pool constructor, it is called on the connection before
returning it to the pool. Note that the *reset()* function is called in a
worker thread, so that the thread which used the connection can keep its
execution without being slowed down.


Pool connection and sizing
--------------------------

A pool can have a fixed size (specifying no *max_size* or *max_size* =
*min_size*) or a dynamic size (when *max_size* > *min_size*). In both cases, as
soon as the pool is created, it will try to acquire *min_size* connections in
the background.

If an attempt to create a connection fails, a new attempt will be made soon
after, using an exponential backoff to increase the time between attempts,
until a maximum of *reconnect_timeout* is reached. When that happens, the pool
will call the *reconnect_failed()* function, if provided to the pool, and just
start a new connection attempt. You can use this function either to send
alerts or to interrupt the program and allow the rest of your infrastructure
to restart it.

If more than *min_size* connections are requested concurrently, new ones are
created, up to *max_size*. Note that the connections are always created by the
background workers, not by the thread asking for the connection: if a client
requests a new connection, and a previous client terminates its job before the
new connection is ready, the waiting client will be served the existing
connection. This is especially useful in scenarios where the time to connect
is longer than the time the connection is used (see `this analysis`__, for
instance).

.. __: https://github.com/brettwooldridge/HikariCP/blob/dev/documents/
       Welcome-To-The-Jungle.md

If a pool grows above *min_size*, but its usage decreases afterwards, a number
of connections are eventually closed: one each the *max_idle* time specified
in the pool constructor.


What's the right size for the pool
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Big question. Who knows. However, probably not as large as you imagine. Please
take a look at `this analysis`__ for some ideas.

.. __: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing

Something useful you can do is probably to use the
`~ConnectionPool.get_stats()` method and monitor the behaviour of your
program, eventually adjusting the size of the pool using the
`~ConnectionPool.resize()` method.


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
better way than polling. You can use the same recipe of :ref:`disconnections`:
you can dedicate a thread (and a connection) to listen for activity on the
connection. If any activity is detected you can call the pool
`~ConnectionPool.check()` method, which will make every connection in the pool
briefly unavailable and run a quick check on them, returning them to the pool
if they are still working or creating a new connection if they aren't.

If you set up a similar check in your program, in case the database connection
is temporarily lost, we cannot do anything for the thread which already had
taken a connection from the pool, but no other thread should be served a
broken connection, because `!check()` would empty the pool and refill it with
working connections, as soon as they are available.

Faster than you can say poll. Or pool.


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
rigid interface: it is possible that they might change. Keys whose value is 0
may not be returned.


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
