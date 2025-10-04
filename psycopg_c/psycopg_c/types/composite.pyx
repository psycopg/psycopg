cimport cython
from cpython.ref cimport Py_INCREF
from libc.stdint cimport int32_t, uint32_t
from libc.string cimport memcpy
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.list cimport PyList_AsTuple, PyList_New, PyList_SET_ITEM
from cpython.object cimport PyObject

from psycopg_c._psycopg cimport endian
from psycopg_c.pq.libpq cimport Oid


@cython.final
cdef class RecordBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object _ctx
    cdef dict _txs

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        self._ctx = context
        self._txs = {}

    cdef object cload(self, const char *data, size_t length):
        cdef size_t offset = 0
        cdef int i

        cdef uint32_t benfields
        memcpy(&benfields, data, sizeof(benfields))
        cdef int nfields = endian.be32toh(benfields)
        offset += sizeof(benfields)
        cdef list record = PyList_New(nfields)
        cdef list oids = PyList_New(nfields)

        cdef int32_t beoid, befieldlength
        cdef object oid
        cdef Py_ssize_t fieldlength
        cdef object field

        for i in range(nfields):
            memcpy(&beoid, data + offset, sizeof(beoid))
            oid = <Oid>endian.be32toh(beoid)
            offset += sizeof(beoid)
            Py_INCREF(<object>oid)
            PyList_SET_ITEM(oids, i, <object>oid)

            memcpy(&befieldlength, data + offset, sizeof(befieldlength))
            offset += sizeof(befieldlength)

            if befieldlength == _binary_null:
                field = None
            else:
                fieldlength = endian.be32toh(befieldlength)
                if offset + fieldlength > length:
                    raise e.DataError("bad record data: length exceeing data")
                field = data[offset:offset + fieldlength]
                offset += fieldlength

            Py_INCREF(field)
            PyList_SET_ITEM(record, i, field)

        cdef tuple key = PyList_AsTuple(oids)
        cdef object tx
        cdef PyObject* tx_ptr = PyDict_GetItem(self._txs, key)

        if tx_ptr is NULL:
            tx = Transformer(self._ctx)
            tx.set_loader_types(oids, self.format)
            PyDict_SetItem(self._txs, key, tx)
        else:
            tx = <object>tx_ptr

        return tx.load_sequence(record)
