.. currentmodule:: psycopg

.. index::
    single: Release notes
    single: News

``psycopg`` release notes
=========================

Future releases
---------------

Psycopg 3.2 (unreleased)
^^^^^^^^^^^^^^^^^^^^^^^^

- Add support for integer, floating point, boolean `NumPy scalar types`__
  (:ticket:`#332`).
- Add :ref:`raw-query-cursors` to execute queries using placeholders in
  PostgreSQL format (`$1`, `$2`...) (:ticket:`#560`).
- Add support for libpq functions to close prepared statements and portals
  introduced in libpq v17 (:ticket:`#603`).
- Disable receiving more than one result on the same cursor in pipeline mode,
  to iterate through `~Cursor.nextset()`. The behaviour was different than
  in non-pipeline mode and not totally reliable (:ticket:`#604`).
  The `Cursor` now only preserves the results set of the last
  `~Cursor.execute()`, consistently with non-pipeline mode.

.. __: https://numpy.org/doc/stable/reference/arrays.scalars.html#built-in-scalar-types


Psycopg 3.1.11 (unreleased)
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Fix integer overflow in C/binary extension with OID > 2^31 (:ticket:`#630`).
- Fix building on Solaris and derivatives (:ticket:`#632`).
- Fix possible lack of critical section guard in async
  `~AsyncCursor.executemany()`.
- Fix missing pipeline fetch in async `~AsyncCursor.scroll()`.


Current release
---------------

Psycopg 3.1.10
^^^^^^^^^^^^^^

- Allow JSON dumpers to dump `bytes` directly instead of `str`,
  for better compatibility with libraries like orjson and msgspec
  (:ticket:`#569`)
- Fix prepared statement cache validation when exiting pipeline mode (or
  `~Cursor.executemany()`) in case an error occurred within the pipeline
  (:ticket:`#585`).
- Fix `connect()` to avoid "leaking" an open `~pq.PGconn` attached to the
  `OperationalError` in case of connection failure. `Error.pgconn` is now a
  shallow copy of the real libpq connection, and the latter is closed before
  the exception propagates (:ticket:`#565`).
- Fix possible (ignored) exception on objects deletion (:ticket:`#591`).
- Don't clobber a Python exception raised during COPY FROM with the resulting
  `!QueryCanceled` raised as a consequence (:ticket:`#593`).
- Fix resetting `Connection.read_only` and `~Connection.deferrable` to their
  default value using `!None` (:ticket:`#612`).
- Add support for Python 3.12.


Psycopg 3.1.9
^^^^^^^^^^^^^

- Fix `TypeInfo.fetch()` using a connection in `!sql_ascii` encoding
  (:ticket:`#503`).
- Fix "filedescriptor out of range" using a large number of files open
  in Python implementation (:ticket:`#532`).
- Allow JSON dumpers to be registered on `!dict` or any other object, as was
  possible in psycopg2 (:ticket:`#541`).
- Fix canceling running queries on process interruption in async connections
  (:ticket:`#543`).
- Fix loading ROW values with different types in the same query using the
  binary protocol (:ticket:`#545`).
- Fix dumping recursive composite types (:ticket:`#547`).


Psycopg 3.1.8
^^^^^^^^^^^^^

- Don't pollute server logs when types looked for by `TypeInfo.fetch()`
  are not found (:ticket:`#473`).
- Set `Cursor.rowcount` to the number of rows of each result set from
  `~Cursor.executemany()` when called with `!returning=True` (:ticket:`#479`).
- Fix `TypeInfo.fetch()` when used with `ClientCursor` (:ticket:`#484`).


Psycopg 3.1.7
^^^^^^^^^^^^^

- Fix server-side cursors using row factories (:ticket:`#464`).


Psycopg 3.1.6
^^^^^^^^^^^^^

- Fix `cursor.copy()` with cursors using row factories (:ticket:`#460`).


Psycopg 3.1.5
^^^^^^^^^^^^^

- Fix array loading slowness compared to psycopg2 (:ticket:`#359`).
- Improve performance around network communication (:ticket:`#414`).
- Return `!bytes` instead of `!memoryview` from `pq.Encoding` methods
  (:ticket:`#422`).
- Fix `Cursor.rownumber` to return `!None` when the result has no row to fetch
  (:ticket:`#437`).
- Avoid error in Pyright caused by aliasing `!TypeAlias` (:ticket:`#439`).
- Fix `Copy.set_types()` used with `varchar` and `name` types (:ticket:`#452`).
- Improve performance using :ref:`row-factories` (:ticket:`#457`).


Psycopg 3.1.4
^^^^^^^^^^^^^

- Include :ref:`error classes <sqlstate-exceptions>` defined in PostgreSQL 15.
- Add support for Python 3.11 (:ticket:`#305`).
- Build binary packages with libpq from PostgreSQL 15.0.


Psycopg 3.1.3
^^^^^^^^^^^^^

- Restore the state of the connection if `Cursor.stream()` is terminated
  prematurely (:ticket:`#382`).
- Fix regression introduced in 3.1 with different named tuples mangling rules
  for non-ascii attribute names (:ticket:`#386`).
- Fix handling of queries with escaped percent signs (``%%``) in `ClientCursor`
  (:ticket:`#399`).
- Fix possible duplicated BEGIN statements emitted in pipeline mode
  (:ticket:`#401`).


Psycopg 3.1.2
^^^^^^^^^^^^^

- Fix handling of certain invalid time zones causing problems on Windows
  (:ticket:`#371`).
- Fix segfault occurring when a loader fails initialization (:ticket:`#372`).
- Fix invalid SAVEPOINT issued when entering `Connection.transaction()` within
  a pipeline using an implicit transaction (:ticket:`#374`).
- Fix queries with repeated named parameters in `ClientCursor` (:ticket:`#378`).
- Distribute macOS arm64 (Apple M1) binary packages (:ticket:`#344`).


Psycopg 3.1.1
^^^^^^^^^^^^^

- Work around broken Homebrew installation of the libpq in a non-standard path
  (:ticket:`#364`)
- Fix possible "unrecognized service" error in async connection when no port
  is specified (:ticket:`#366`).


Psycopg 3.1
-----------

- Add :ref:`Pipeline mode <pipeline-mode>` (:ticket:`#74`).
- Add :ref:`client-side-binding-cursors` (:ticket:`#101`).
- Add `CockroachDB <https://www.cockroachlabs.com/>`__ support in `psycopg.crdb`
  (:ticket:`#313`).
- Add :ref:`Two-Phase Commit <two-phase-commit>` support (:ticket:`#72`).
- Add :ref:`adapt-enum` (:ticket:`#274`).
- Add ``returning`` parameter to `~Cursor.executemany()` to retrieve query
  results (:ticket:`#164`).
- `~Cursor.executemany()` performance improved by using batch mode internally
  (:ticket:`#145`).
- Add parameters to `~Cursor.copy()`.
- Add :ref:`COPY Writer objects <copy-writers>`.
- Resolve domain names asynchronously in `AsyncConnection.connect()`
  (:ticket:`#259`).
- Add `pq.PGconn.trace()` and related trace functions (:ticket:`#167`).
- Add ``prepare_threshold`` parameter to `Connection` init (:ticket:`#200`).
- Add ``cursor_factory`` parameter to `Connection` init.
- Add `Error.pgconn` and `Error.pgresult` attributes (:ticket:`#242`).
- Restrict queries to be `~typing.LiteralString` as per :pep:`675`
  (:ticket:`#323`).
- Add explicit type cast to values converted by `sql.Literal` (:ticket:`#205`).
- Drop support for Python 3.6.


Psycopg 3.0.17
^^^^^^^^^^^^^^

- Fix segfaults on fork on some Linux systems using `ctypes` implementation
  (:ticket:`#300`).
- Load bytea as bytes, not memoryview, using `ctypes` implementation.


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
- Add ``context`` parameter to `~Connection.connect()` (:ticket:`#83`).
- Fix selection of dumper by oid after `~Copy.set_types()`.
- Drop `!Connection.client_encoding`. Use `ConnectionInfo.encoding` to read
  it, and a :sql:`SET` statement to change it.
- Add binary packages for Python 3.10 (:ticket:`#103`).


Psycopg 3.0b1
^^^^^^^^^^^^^

- First public release on PyPI.
