"""
psycopg3_c.pq.PGcancel object implementation.
"""

# Copyright (C) 2020-2021 The Psycopg Team


cdef class PGcancel:
    def __cinit__(self):
        self.pgcancel_ptr = NULL

    @staticmethod
    cdef PGcancel _from_ptr(libpq.PGcancel *ptr):
        cdef PGcancel rv = PGcancel.__new__(PGcancel)
        rv.pgcancel_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.free()

    def free(self) -> None:
        if self.pgcancel_ptr is not NULL:
            libpq.PQfreeCancel(self.pgcancel_ptr)
            self.pgcancel_ptr = NULL

    def cancel(self) -> None:
        cdef char buf[256]
        cdef int res = libpq.PQcancel(self.pgcancel_ptr, buf, sizeof(buf))
        if not res:
            raise PQerror(
                f"cancel failed: {buf.decode('utf8', 'ignore')}"
            )
