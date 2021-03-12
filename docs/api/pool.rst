`pool` -- Connection pool implementations
=========================================

.. index::
    double: Connection; Pool

.. module:: psycopg3.pool

The package contains two connection pool implementations. A connection pool
creates and maintains a limited amount of PostgreSQL connections and allows a
larger number of users to use them. See :ref:`connection-pools` for more
details and usage pattern.

There package implement two connection pools: `ConnectionPool` is a
synchronous connection pool yielding `~psycopg3.Connection` objects and can be
used by multithread applications. `AsyncConnectionPool` has a similar
interface, but with `asyncio` functions replacing blocking functions, and
yields `~psycopg3.AsyncConnection` instances.

The intended use (but not mandatory) is to create a single connection pool, as
a global object exposed by a module in your application, and use the same
instance from the rest of the code (especially the
`~ConnectionPool.connection()` method.


The `!ConnectionPool` class
---------------------------

.. autoclass:: ConnectionPool(conninfo, *, **arguments)

   This class implements a connection pool serving `~psycopg2.Connection`
   instances (or subclasses).

   :param conninfo: The connection string. See
                    `~psycopg3.Connection.connect()` for details.
   :type conninfo: `!str`

   :param minconn: The minimum number of connection the pool will hold. The
                   pool will actively try to create new connections if some
                   are lost (closed, broken) and will try to never go below
                   *minconn*. Default: 4
   :type minconn: `!int`

   :param maxconn: The maximum number of connections the pool will hold. If
                   `!None`, or equal to *minconn*, the pool will not grow or
                   shrink. If larger than *minconn* the pool can grow if more
                   than *minconn* connections are requested at the same time
                   and will shrink back after the extra connections have been
                   unused for more than *max_idle* seconds. Default: `!None`.
   :type maxconn: `Optional[int]`

   :param kwargs: Extra arguments to pass to `!connect()`. Note that this is
                  *one dict argument* of the pool constructor, which is
                  expanded as `connect()` keyword parameters.

   :type kwargs: `!dict`

   :param configure: A callback to configure a connection after creation.
                     Useful, for instance, to configure its adapters. If the
                     connection is used to run internal queries (to inspect the
                     database) make sure to close an eventual transaction
                     before leaving the function.
   :type configure: `Callable[[Connection], None]`

   :param reset: A callback to reset a function after it has been returned to
                 the pool. The connection is guaranteed to be passed to the
                 *reset()* function in "idle" state (no transaction). When
                 leaving the *reset()* function the connection must be left in
                 *idle* state, otherwise it is discarded.
   :type reset: `Callable[[Connection], None]`

   :param connection_class: The class of the connections to serve. Default:
                            `~psycopg3.Connection`. It should be a
                            `!Connection` subclass.
   :type connection_class: ``Type[Connection]``

   :param name: An optional name to give to the pool, useful, for instance, to
                identify it in the logs if more than one pool is used. If
                `!None` (default) pick a sequential name such as ``pool-1``,
                ``pool-2`` etc.
   :type name: `!str`

   :param timeout: The default maximum time in seconts that a client can wait
                   to receive a connection from the pool (using `connection()`
                   or `getconn()`). Note that these methods allow to override
                   the *timeout* default. Default: 30 seconds.
   :type timeout: `!float`

   :param max_waiting: Maximum number of requests that can be queued to the
                       pool. Adding more requests will fail, raising
                       `TooManyRequests`. Specifying 0 (the default) means to
                       upper bound.
   :type max_waiting: `!int`

   :param max_lifetime: The maximum lifetime of a connection in the pool, in
                        seconds. Connections used for longer get closed and
                        replaced by a new one. The amount is reduced by a
                        random 10% to avoid mass eviction. Default: one hour.
   :type max_lifetime: `!float`

   :param max_idle: Maximum time a connection can be unused in the pool before
                    being closed, and the pool shrunk. This only happens to
                    connections more than *minconn*, if *maxconn* allowed the
                    pool to grow. Default: 10 minutes.
   :type max_idle: `!float`

   :param reconnect_timeout: Maximum time in seconds the pool will try to
                             create a connection. If a connection attempt
                             fails, the pool will try to reconnect a few
                             times, using an exponential backoff and some
                             random factor to avoid mass attempts. If repeated
                             attempt fails, after *reconnect_timeout* second
                             the attempt is aborted and the *reconnect_failed*
                             callback invoked. Default: 5 minutes.
   :type reconnect_timeout: `!float`
                             
   :param reconnect_failed: Callback invoked if an attempt to create a new
                            connection fails for more than *reconnect_timeout*
                            seconds. The user may decide, for instance, to
                            terminate the program (executing `sys.exit()`).
                            By default don't do anything: restart a new
                            connection attempt (if the number of connection
                            fell below *minconn*).
   :type reconnect_failed: ``Callable[[ConnectionPool], None]``

   :param num_workers: Number of background worker threads used to maintain the
                       pool state. Background workers are used for example to
                       create new connections and to clean up connections when
                       they are returned to the pool. Default: 3.
   :type num_workers: `!int`

   .. automethod:: wait
   .. automethod:: connection
   
      .. code:: python

          with my_pool.connection() as conn:
              conn.execute(...)

          # the connection is now back in the pool
   
   .. automethod:: close

      .. note::
          
          The pool can be used as context manager too, in which case it will
          be closed at the end of the block:

          .. code:: python

              with ConnectionPool(...) as pool:
                  # code using the pool

   .. attribute:: name
      :type: str

      The name of the pool set on creation, or automatically generated if not
      set.

   .. autoproperty:: minconn
   .. autoproperty:: maxconn

      The current minimum and maximum size of the pool. Use `resize()` to
      change them at runtime.
   
   .. automethod:: resize
   .. automethod:: check
   .. automethod:: get_stats
   .. automethod:: pop_stats

      See :ref:`pool-stats` for the metrics returned.

   .. rubric:: Functionalities you may not need

   .. automethod:: getconn
   .. automethod:: putconn


.. autoclass:: PoolTimeout()

   Subclass of `~psycopg3.OperationalError`

.. autoclass:: PoolClosed()

   Subclass of `~psycopg3.OperationalError`

.. autoclass:: TooManyRequests()

   Subclass of `~psycopg3.OperationalError`


The `!AsyncConnectionPool` class
--------------------------------

`!AsyncConnectionPool` has a very similar interface to the `ConnectionPool`
class but its blocking method are implemented as `async` coroutines. It
returns `AsyncConnection` instances, or its subclasses if specified so in the
*connection_class* parameter.

Only the function with different signature from `!ConnectionPool` are
listed here.

.. autoclass:: AsyncConnectionPool(conninfo, *, **arguments)

   All the other parameters are the same.

   :param configure: A callback to configure a connection after creation.
   :type configure: `async Callable[[AsyncConnection], None]`

   :param reset: A callback to reset a function after it has been returned to
                 the pool.
   :type reset: `async Callable[[AsyncConnection], None]`

   :param connection_class: The class of the connections to serve. Default:
                            `~psycopg3.AsyncConnection`. It should be an
                            `!AsyncConnection` subclass.
   :type connection_class: ``Type[AsyncConnection]``

   .. automethod:: wait
   .. automethod:: connection
   
      .. code:: python

          async with my_pool.connection() as conn:
              await conn.execute(...)

          # the connection is now back in the pool
   
   .. automethod:: close

      .. note::
          
          The pool can be used as context manager too, in which case it will
          be closed at the end of the block:

          .. code:: python

              async with AsyncConnectionPool(...) as pool:
                  # code using the pool

   .. automethod:: resize
   .. automethod:: check
   .. automethod:: getconn
   .. automethod:: putconn
