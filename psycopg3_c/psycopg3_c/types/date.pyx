"""
Cython adapters for date/time types.
"""

# Copyright (C) 2021 The Psycopg Team


from cpython.datetime cimport import_datetime, date_new

from psycopg3 import errors as e


# Initialise the datetime C API
import_datetime()

DEF ORDER_YMD = 0
DEF ORDER_DMY = 1
DEF ORDER_MDY = 2


@cython.final
cdef class DateLoader(CLoader):

    format = PQ_TEXT
    cdef int _order

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)

        cdef const char *ds = _get_datestyle(self._pgconn)
        if ds[0] == b'I':  # ISO
            self._order = ORDER_YMD
        elif ds[0] == b'G':  # German
            self._order = ORDER_DMY
        elif ds[0] == b'S' or ds[0] == b'P':  # SQL or Postgres
            self._order = (
                ORDER_DMY if ds.endswith(b"DMY") else ORDER_MDY
            )
        else:
            raise e.InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    cdef object cload(self, const char *data, size_t length):
        if length != 10:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"date not supported: {s!r}")

        cdef int vals[3]
        vals[0] = vals[1] = vals[2] = 0

        cdef size_t i
        cdef int ival = 0
        for i in range(length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
            else:
                ival += 1
                if ival >= 3:
                    s = bytes(data).decode("utf8", "replace")
                    raise e.DataError(f"can't parse date {s!r}")

        try:
            if self._order == ORDER_YMD:
                return date_new(vals[0], vals[1], vals[2])
            elif self._order == ORDER_DMY:
                return date_new(vals[2], vals[1], vals[0])
            else:
                return date_new(vals[2], vals[0], vals[1])
        except ValueError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't manage date {s!r}: {ex}") from None


cdef const char *_get_datestyle(pq.PGconn pgconn):
    cdef const char *ds
    if pgconn is not None:
        ds = libpq.PQparameterStatus(pgconn._pgconn_ptr, b"DateStyle")
        if ds is not NULL and ds[0]:
            return ds

    return b"ISO, DMY"
