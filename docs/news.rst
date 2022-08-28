.. currentmodule:: psycopg

.. index::
    single: Release notes
    single: News

``psycopg`` release notes
=========================

Future releases
---------------

Psycopg 3.0.17 (unreleased)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Fix segfaults on fork on some Linux systems using `ctypes` implementation
  (:ticket:`#300`).


Current release
---------------

Psycopg 3.0.16
^^^^^^^^^^^^^^

- Fix missing `~Cursor.rowcount` after SHOW (:ticket:`#343`).
- Add scripts to build macOS arm64 packages (:ticket:`#162`).


Psycopg 3.0.15
^^^^^^^^^^^^^^

- Fix wrong escaping of unprintable chars in COPY (nonetheless correctly
  interpreted by PostgreSQL).
- Restore the connection to usable state after an error in `~Cursor.stream()`.
- Raise `DataError` instead of `OverflowError` loading binary intervals
  out-of-range.
- Distribute ``manylinux2014`` wheel packages (:ticket:`#124`).


Psycopg 3.0.14
^^^^^^^^^^^^^^

- Raise `DataError` dumping arrays of mixed types (:ticket:`#301`).
- Fix handling of incorrect server results, with blank sqlstate (:ticket:`#303`).
- Fix bad Float4 conversion on ppc64le/musllinux (:ticket:`#304`).


Psycopg 3.0.13
^^^^^^^^^^^^^^

- Fix `Cursor.stream()` slowness (:ticket:`#286`).
- Fix oid for lists of integers, which might cause the server choosing
  bad plans (:ticket:`#293`).
- Make `Connection.cancel()` on a closed connection a no-op instead of an
  error.


Psycopg 3.0.12
^^^^^^^^^^^^^^

- Allow `bytearray`/`memoryview` data too as `Copy.write()` input
  (:ticket:`#254`).
- Fix dumping `~enum.IntEnum` in text mode, Python implementation.


Psycopg 3.0.11
^^^^^^^^^^^^^^

- Fix `DataError` loading arrays with dimensions information (:ticket:`#253`).
- Fix hanging during COPY in case of memory error (:ticket:`#255`).
- Fix error propagation from COPY worker thread (mentioned in :ticket:`#255`).


Psycopg 3.0.10
^^^^^^^^^^^^^^

- Leave the connection in working state after interrupting a query with Ctrl-C
  (:ticket:`#231`).
- Fix `Cursor.description` after a COPY ... TO STDOUT operation
  (:ticket:`#235`).
- Fix building on FreeBSD and likely other BSD flavours (:ticket:`#241`).


Psycopg 3.0.9
^^^^^^^^^^^^^

- Set `Error.sqlstate` when an unknown code is received (:ticket:`#225`).
- Add the `!tzdata` package as a dependency on Windows in order to handle time
  zones (:ticket:`#223`).


Psycopg 3.0.8
^^^^^^^^^^^^^

- Decode connection errors in the ``client_encoding`` specified in the
  connection string, if available (:ticket:`#194`).
- Fix possible warnings in objects deletion on interpreter shutdown
  (:ticket:`#198`).
- Don't leave connections in ACTIVE state in case of error during COPY ... TO
  STDOUT (:ticket:`#203`).


Psycopg 3.0.7
^^^^^^^^^^^^^

- Fix crash in `~Cursor.executemany()` with no input sequence
  (:ticket:`#179`).
- Fix wrong `~Cursor.rowcount` after an `~Cursor.executemany()` returning no
  rows (:ticket:`#178`).


Psycopg 3.0.6
^^^^^^^^^^^^^

- Allow to use `Cursor.description` if the connection is closed
  (:ticket:`#172`).
- Don't raise exceptions on `ServerCursor.close()` if the connection is closed
  (:ticket:`#173`).
- Fail on `Connection.cursor()` if the connection is closed (:ticket:`#174`).
- Raise `ProgrammingError` if out-of-order exit from transaction contexts is
  detected (:tickets:`#176, #177`).
- Add `!CHECK_STANDBY` value to `~pq.ConnStatus` enum.


Psycopg 3.0.5
^^^^^^^^^^^^^

- Fix possible "Too many open files" OS error, reported on macOS but possible
  on other platforms too (:ticket:`#158`).
- Don't clobber exceptions if a transaction block exit with error and rollback
  fails (:ticket:`#165`).


Psycopg 3.0.4
^^^^^^^^^^^^^

- Allow to use the module with strict strings comparison (:ticket:`#147`).
- Fix segfault on Python 3.6 running in ``-W error`` mode, related to
  `!backport.zoneinfo` `ticket #109
  <https://github.com/pganssle/zoneinfo/issues/109>`__.
- Build binary package with libpq versions not affected by `CVE-2021-23222
  <https://www.postgresql.org/support/security/CVE-2021-23222/>`__
  (:ticket:`#149`).


Psycopg 3.0.3
^^^^^^^^^^^^^

- Release musllinux binary packages, compatible with Alpine Linux
  (:ticket:`#141`).
- Reduce size of binary package by stripping debug symbols (:ticket:`#142`).
- Include typing information in the `!psycopg_binary` package.


Psycopg 3.0.2
^^^^^^^^^^^^^

- Fix type hint for `sql.SQL.join()` (:ticket:`#127`).
- Fix type hint for `Connection.notifies()` (:ticket:`#128`).
- Fix call to `MultiRange.__setitem__()` with a non-iterable value and a
  slice, now raising a `TypeError` (:ticket:`#129`).
- Fix disable cursors methods after close() (:ticket:`#125`).


Psycopg 3.0.1
^^^^^^^^^^^^^

- Fix use of the wrong dumper reusing cursors with the same query but different
  parameter types (:ticket:`#112`).


Psycopg 3.0
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


Psycopg 3.0b1
^^^^^^^^^^^^^

- First public release on PyPI.
