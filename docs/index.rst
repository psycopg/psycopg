===================================================
Psycopg 3 -- PostgreSQL database adapter for Python
===================================================

Psycopg 3 is a newly designed PostgreSQL_ database adapter for the Python_
programming language.

Psycopg 3 presents a familiar interface for everyone who has used
`Psycopg 2`_ or any other `DB-API 2.0`_ database adapter, but allows to use
more modern PostgreSQL and Python features, such as:

- :ref:`asynchronous support <async>`
- :ref:`server-side parameters binding <server-side-binding>`
- :ref:`prepared statements <prepared-statements>`
- :ref:`binary communication <binary-data>`
- :ref:`great COPY support <copy>`
- :ref:`support for static typing <static-typing>`
- :ref:`a redesigned connection pool <connection-pools>`
- :ref:`direct access to the libpq functionalities <psycopg.pq>`

.. _Python: https://www.python.org/
.. _PostgreSQL: https://www.postgresql.org/
.. _Psycopg 2: https://www.psycopg.org/docs/
.. _DB-API 2.0: https://www.python.org/dev/peps/pep-0249/


Documentation
=============

.. toctree::
    :maxdepth: 2

    basic/index
    advanced/index
    api/index

Release notes
-------------

.. toctree::
    :maxdepth: 1

    news
    news_pool


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
