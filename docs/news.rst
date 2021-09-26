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
- Add `psycopg.pq.__build_version__` constant.
- Don't use the extended protocol with COPY, (:tickets:`#78, #82`).
- Add *context* parameter to `~psycopg.Connection.connect()` (:ticket:`#83`).
- Fix selection of dumper by oid after `~psycopg.Copy.set_types()`.


psycopg 3.0b1
^^^^^^^^^^^^^

- First public release on PyPI.
