`_dns` -- DNS resolution utilities
==================================

.. module:: psycopg._dns

This module contains a few experimental utilities to interact with the DNS
server before performing a connection.

.. warning::
    This module is experimental and its interface could change in the future,
    without warning or respect for the version scheme. It is provided here to
    allow experimentation before making it more stable.

.. warning::
    This module depends on the `dnspython`_ package. The package is currently
    not installed automatically as a Psycopg dependency and must be installed
    manually:

    .. code:: sh

        $ pip install "dnspython >= 2.1"

    .. _dnspython: https://dnspython.readthedocs.io/


.. autofunction:: resolve_srv

   .. warning::
       This is an experimental functionality.

   .. note::
       One possible way to use this function automatically is to subclass
       `~psycopg.Connection`, extending the
       `~psycopg.Connection._get_connection_params()` method::

           import psycopg._dns  # not imported automatically

           class SrvCognizantConnection(psycopg.Connection):
               @classmethod
               def _get_connection_params(cls, conninfo, **kwargs):
                   params = super()._get_connection_params(conninfo, **kwargs)
                   params = psycopg._dns.resolve_srv(params)
                   return params

           # The name will be resolved to db1.example.com
           cnn = SrvCognizantConnection.connect("host=_postgres._tcp.db.psycopg.org")


.. autofunction:: resolve_srv_async


.. automethod:: psycopg.Connection._get_connection_params

    .. warning::
        This is an experimental method.

    This method is a subclass hook allowing to manipulate the connection
    parameters before performing the connection. Make sure to call the
    `!super()` implementation before further manipulation of the arguments::

        @classmethod
        def _get_connection_params(cls, conninfo, **kwargs):
            params = super()._get_connection_params(conninfo, **kwargs)
            # do something with the params
            return params


.. automethod:: psycopg.AsyncConnection._get_connection_params

   .. warning::
       This is an experimental method.


.. autofunction:: resolve_hostaddr_async

   .. note::
       Starting from psycopg 3.1, a similar operation is performed
       automatically by `!AsyncConnection._get_connection_params()`, so this
       function is unneeded.

       In psycopg 3.0, one possible way to use this function automatically is
       to subclass `~psycopg.AsyncConnection`, extending the
       `~psycopg.AsyncConnection._get_connection_params()` method::

           import psycopg._dns  # not imported automatically

           class AsyncDnsConnection(psycopg.AsyncConnection):
               @classmethod
               async def _get_connection_params(cls, conninfo, **kwargs):
                   params = await super()._get_connection_params(conninfo, **kwargs)
                   params = await psycopg._dns.resolve_hostaddr_async(params)
                   return params
