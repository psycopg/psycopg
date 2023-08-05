"""
Cython adapters for numpy types.
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython


@cython.final
cdef class NPInt16Dumper(_IntDumper):

    oid = oids.INT2_OID


@cython.final
cdef class NPInt32Dumper(_IntDumper):

    oid = oids.INT4_OID


@cython.final
cdef class NPInt64Dumper(_IntDumper):

    oid = oids.INT8_OID


@cython.final
cdef class NPNumericDumper(_IntDumper):

    oid = oids.NUMERIC_OID


@cython.final
cdef class NPInt16BinaryDumper(_IntDumper):

    oid = oids.INT2_OID
    format = PQ_BINARY

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_int2_binary(int(obj), rv, offset)


@cython.final
cdef class NPInt32BinaryDumper(_IntDumper):

    oid = oids.INT4_OID
    format = PQ_BINARY

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_int4_binary(int(obj), rv, offset)


@cython.final
cdef class NPInt64BinaryDumper(_IntDumper):

    oid = oids.INT8_OID
    format = PQ_BINARY

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_int8_binary(int(obj), rv, offset)


@cython.final
cdef class NPNumericBinaryDumper(_IntDumper):

    oid = oids.NUMERIC_OID
    format = PQ_BINARY

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_numeric_binary(int(obj), rv, offset)
