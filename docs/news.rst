.. currentmodule:: psycopg

.. index::
    single: Release notes
    single: News

Release notes
=============

Current release
---------------

psycopg 3.0.2
^^^^^^^^^^^^^

- Fix type hint for `sql.SQL.join()` (:ticket:`#127`).
- Fix type hint for `Connection.notifies()` (:ticket:`#128`).


psycopg 3.0.1
^^^^^^^^^^^^^

- Fix use of the wrong dumper reusing cursors with the same query but different
  parameter types (:ticket:`112`).


psycopg 3.0
-----------

First stable release. Changed from 3.0b1:

- Add :ref:`adapt-shapely` (:ticket:`#80`).
- Add :ref:`adapt-multirange` (:ticket:`#75`).
- Add `pq.__build_version__` constant.
- Don't use the extended protocol with COPY, (:tickets:`#78, #82`).
- Add *context* parameter to `~Connection.connect()` (:ticket:`#83`).
- Fix selection of dumper by oid after `~Copy.set_types()`.
- Drop `!Connection.client_encoding`. Use `ConnectionInfo.encoding` to read
  it, and a :sql:`SET` statement to change it.
- Add binary packages for Python 3.10 (:ticket:`#103`).


psycopg 3.0b1
^^^^^^^^^^^^^

- First public release on PyPI.
