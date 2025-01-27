cimport cython

import uuid
from cpython.bytes cimport PyBytes_AsString

cdef extern from "Python.h":
    # PyUnicode_AsUTF8 was added to cpython.unicode in 3.1.x but we still
    # support 3.0.x
    const char *PyUnicode_AsUTF8(object unicode) except NULL

from libc.stdio cimport printf


#cdef extern from "Python.h":
#    Py_ssize_t PyLong_AsNativeBytes(PyObject* vv, void* buffer, Py_ssize_t n, int flags)


@cython.final
cdef class UUIDDumper(CDumper):
    format = PQ_TEXT
    oid = oids.UUID_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef const char *src = PyUnicode_AsUTF8(obj.hex)
        cdef char *buf = CDumper.ensure_size(rv, offset, 32)
        memcpy(buf, src, 32)
        return 32


@cython.final
cdef class UUIDBinaryDumper(CDumper):
    format = PQ_BINARY
    oid = oids.UUID_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef const char *src = PyBytes_AsString(obj.bytes)
        cdef char *buf = CDumper.ensure_size(rv, offset, 16)
        memcpy(buf, src, 16)
        return 16
        #cdef PyObject *pyobj = <PyObject*>obj.int
        #cdef char *buf = CDumper.ensure_size(rv, offset, 16)
        #PyLong_AsNativeBytes(pyobj, buf, 16, 4)
        #return 16


@cython.final
cdef class UUIDLoader(CLoader):
    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        return uuid.UUID(hex=data[:length].decode())


@cython.final
cdef class UUIDBinaryLoader(CLoader):
    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return uuid.UUID(bytes=data[:length])
