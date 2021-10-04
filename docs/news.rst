.. currentmodule:: psycopg

.. index::
    single: Release notes
    single: News

Release notes
=============

Current release
---------------

psycopg 3.0b2
^^^^^^^^^^^^^

- Add :ref:`adapt-shapely` (:ticket:`#80`).
- Add :ref:`adapt-multirange` (:ticket:`#75`).
- Add `pq.__build_version__` constant.
- Don't use the extended protocol with COPY, (:tickets:`#78, #82`).
- Add *context* parameter to `~Connection.connect()` (:ticket:`#83`).
- Fix selection of dumper by oid after `~Copy.set_types()`.
- Drop `!Connection.client_encoding`. Use `ConnectionInfo.encoding` to read
  it, and a :sql:`SET` statement to change it.


psycopg 3.0b1
^^^^^^^^^^^^^

- First public release on PyPI.
