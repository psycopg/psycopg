"""
psycopg_c.pq.PGresult object implementation.
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython
from cpython.mem cimport PyMem_Malloc, PyMem_Free

from psycopg.pq.misc import PGresAttDesc
from psycopg.pq._enums import ExecStatus


@cython.freelist(8)
cdef class PGresult:
    def __cinit__(self):
        self._pgresult_ptr = NULL

    @staticmethod
    cdef PGresult _from_ptr(libpq.PGresult *ptr):
        cdef PGresult rv = PGresult.__new__(PGresult)
        rv._pgresult_ptr = ptr
        return rv

    def __dealloc__(self) -> None:
        self.clear()

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        status = ExecStatus(self.status)
        return f"<{cls} [{status.name}] at 0x{id(self):x}>"

    def clear(self) -> None:
        if self._pgresult_ptr is not NULL:
            libpq.PQclear(self._pgresult_ptr)
            self._pgresult_ptr = NULL

    @property
    def pgresult_ptr(self) -> Optional[int]:
        if self._pgresult_ptr:
            return <long long><void *>self._pgresult_ptr
        else:
            return None

    @property
    def status(self) -> int:
        return libpq.PQresultStatus(self._pgresult_ptr)

    @property
    def error_message(self) -> bytes:
        return libpq.PQresultErrorMessage(self._pgresult_ptr)

    def error_field(self, int fieldcode) -> Optional[bytes]:
        cdef char * rv = libpq.PQresultErrorField(self._pgresult_ptr, fieldcode)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def ntuples(self) -> int:
        return libpq.PQntuples(self._pgresult_ptr)

    @property
    def nfields(self) -> int:
        return libpq.PQnfields(self._pgresult_ptr)

    def fname(self, int column_number) -> Optional[bytes]:
        cdef char *rv = libpq.PQfname(self._pgresult_ptr, column_number)
        if rv is not NULL:
            return rv
        else:
            return None

    def ftable(self, int column_number) -> int:
        return libpq.PQftable(self._pgresult_ptr, column_number)

    def ftablecol(self, int column_number) -> int:
        return libpq.PQftablecol(self._pgresult_ptr, column_number)

    def fformat(self, int column_number) -> int:
        return libpq.PQfformat(self._pgresult_ptr, column_number)

    def ftype(self, int column_number) -> int:
        return libpq.PQftype(self._pgresult_ptr, column_number)

    def fmod(self, int column_number) -> int:
        return libpq.PQfmod(self._pgresult_ptr, column_number)

    def fsize(self, int column_number) -> int:
        return libpq.PQfsize(self._pgresult_ptr, column_number)

    @property
    def binary_tuples(self) -> int:
        return libpq.PQbinaryTuples(self._pgresult_ptr)

    def get_value(self, int row_number, int column_number) -> Optional[bytes]:
        cdef int crow = row_number
        cdef int ccol = column_number
        cdef int length = libpq.PQgetlength(self._pgresult_ptr, crow, ccol)
        cdef char *v
        if length:
            v = libpq.PQgetvalue(self._pgresult_ptr, crow, ccol)
            # TODO: avoid copy
            return v[:length]
        else:
            if libpq.PQgetisnull(self._pgresult_ptr, crow, ccol):
                return None
            else:
                return b""

    @property
    def nparams(self) -> int:
        return libpq.PQnparams(self._pgresult_ptr)

    def param_type(self, int param_number) -> int:
        return libpq.PQparamtype(self._pgresult_ptr, param_number)

    @property
    def command_status(self) -> Optional[bytes]:
        cdef char *rv = libpq.PQcmdStatus(self._pgresult_ptr)
        if rv is not NULL:
            return rv
        else:
            return None

    @property
    def command_tuples(self) -> Optional[int]:
        cdef char *rv = libpq.PQcmdTuples(self._pgresult_ptr)
        if rv is NULL:
            return None
        cdef bytes brv = rv
        return int(brv) if brv else None

    @property
    def oid_value(self) -> int:
        return libpq.PQoidValue(self._pgresult_ptr)

    def set_attributes(self, descriptions: List[PGresAttDesc]):
        cdef Py_ssize_t num = len(descriptions)
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

        cdef int res = libpq.PQsetResultAttrs(self._pgresult_ptr, <int>num, attrs)
        PyMem_Free(attrs)
        if (res == 0):
            raise e.OperationalError("PQsetResultAttrs failed")
