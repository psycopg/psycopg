"""
psycopg3_c.pq_cython.PGresult object implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.mem cimport PyMem_Malloc, PyMem_Free

from psycopg3.pq.misc import PGresAttDesc


cdef class PGresult:
    def __cinit__(self):
        self.pgresult_ptr = NULL

    @staticmethod
    cdef PGresult _from_ptr(libpq.PGresult *ptr):
        cdef PGresult rv = PGresult.__new__(PGresult)
        rv.pgresult_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.clear()

    def clear(self) -> None:
        if self.pgresult_ptr is not NULL:
            libpq.PQclear(self.pgresult_ptr)
            self.pgresult_ptr = NULL

    @property
    def pgresult_ptr(self) -> Optional[int]:
        if self.pgresult_ptr:
            return <long><void *>self.pgresult_ptr
        else:
            return None

    @property
    def status(self) -> ExecStatus:
        cdef int rv = libpq.PQresultStatus(self.pgresult_ptr)
        return ExecStatus(rv)

    @property
    def error_message(self) -> bytes:
        return libpq.PQresultErrorMessage(self.pgresult_ptr)

    def error_field(self, fieldcode: DiagnosticField) -> Optional[bytes]:
        cdef char * rv = libpq.PQresultErrorField(self.pgresult_ptr, fieldcode)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def ntuples(self) -> int:
        return libpq.PQntuples(self.pgresult_ptr)

    @property
    def nfields(self) -> int:
        return libpq.PQnfields(self.pgresult_ptr)

    def fname(self, column_number: int) -> Optional[bytes]:
        cdef char *rv = libpq.PQfname(self.pgresult_ptr, column_number)
        if rv is not NULL:
            return rv
        else:
            return None

    def ftable(self, column_number: int) -> int:
        return libpq.PQftable(self.pgresult_ptr, column_number)

    def ftablecol(self, column_number: int) -> int:
        return libpq.PQftablecol(self.pgresult_ptr, column_number)

    def fformat(self, column_number: int) -> Format:
        return Format(libpq.PQfformat(self.pgresult_ptr, column_number))

    def ftype(self, column_number: int) -> int:
        return libpq.PQftype(self.pgresult_ptr, column_number)

    def fmod(self, column_number: int) -> int:
        return libpq.PQfmod(self.pgresult_ptr, column_number)

    def fsize(self, column_number: int) -> int:
        return libpq.PQfsize(self.pgresult_ptr, column_number)

    @property
    def binary_tuples(self) -> Format:
        return Format(libpq.PQbinaryTuples(self.pgresult_ptr))

    def get_value(
        self, row_number: int, column_number: int
    ) -> Optional[bytes]:
        cdef int crow = row_number
        cdef int ccol = column_number
        cdef int length = libpq.PQgetlength(self.pgresult_ptr, crow, ccol)
        cdef char *v
        if length:
            v = libpq.PQgetvalue(self.pgresult_ptr, crow, ccol)
            # TODO: avoid copy
            return v[:length]
        else:
            if libpq.PQgetisnull(self.pgresult_ptr, crow, ccol):
                return None
            else:
                return b""

    @property
    def nparams(self) -> int:
        return libpq.PQnparams(self.pgresult_ptr)

    def param_type(self, param_number: int) -> int:
        return libpq.PQparamtype(self.pgresult_ptr, param_number)

    @property
    def command_status(self) -> Optional[bytes]:
        cdef char *rv = libpq.PQcmdStatus(self.pgresult_ptr)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def command_tuples(self) -> Optional[int]:
        cdef char *rv = libpq.PQcmdTuples(self.pgresult_ptr)
        if rv is NULL:
            return None
        cdef bytes brv = rv
        return int(brv) if brv else None

    @property
    def oid_value(self) -> int:
        return libpq.PQoidValue(self.pgresult_ptr)

    def set_attributes(self, descriptions: List[PGresAttDesc]):
        cdef int num = len(descriptions)
        cdef libpq.PGresAttDesc *attrs = <libpq.PGresAttDesc *>PyMem_Malloc(
            num * sizeof(libpq.PGresAttDesc))

        for i in range(num):
            descr = descriptions[i]
            attrs[i].name = descr.name
            attrs[i].tableid = descr.tableid
            attrs[i].columnid = descr.columnid
            attrs[i].format = descr.format
            attrs[i].typid = descr.typid
            attrs[i].typlen = descr.typlen
            attrs[i].atttypmod = descr.atttypmod

        cdef int res = libpq.PQsetResultAttrs(self.pgresult_ptr, num, attrs)
        PyMem_Free(attrs)
        if (res == 0):
            raise PQerror("PQsetResultAttrs failed")
