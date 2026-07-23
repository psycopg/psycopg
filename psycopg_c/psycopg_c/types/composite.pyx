cimport cython
from cpython.ref cimport Py_DECREF, Py_INCREF
from libc.stdint cimport int32_t, uint32_t
from libc.string cimport memcpy
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.object cimport PyObject

from psycopg_c._psycopg cimport endian
from psycopg_c.pq.libpq cimport Oid


@cython.final
cdef class RecordLoader(_CRecursiveLoader):

    format = PQ_TEXT

    cdef char *scratch
    cdef size_t sclen

    cdef object cload(self, const char *data, size_t length):
        if length == 2 and data[0] == b'(' and data[1] == b')':
            return ()

        record = tuple(
            _parse_text_record(data+1, length-2, &(self.scratch), &(self.sclen)))
        cast = self._tx.get_loader(oids.TEXT_OID, self.format).load
        for i in range(len(record)):
            if (f := record[i]) is not None:
                value = cast(f)
                Py_DECREF(f)
                Py_INCREF(value)
                PyTuple_SET_ITEM(record, i, value)
        return record

    def __dealloc__(self):
        PyMem_Free(self.scratch)

cdef list _parse_text_record(
    const char *data, size_t length, char **scratch, size_t *sclen
):
    cdef char *buf = data
    cdef list record = []

    while buf < data + length:
        record.append(_parse_record_token(&buf, data + length, scratch, sclen))
        if buf[0] == b',':
            buf += 1
            if buf == data + length:
                record.append(None)

    return record


cdef object _parse_record_token(
    char **bufptr, const char *bufend, char **scratch, size_t *sclen
):
    cdef char *start = bufptr[0]
    cdef int has_quotes = start[0] == b'"'
    cdef bint quoted = has_quotes
    cdef int num_escapes = 0

    if has_quotes:
        start += 1
    cdef char *end = start

    while end < bufend:
        if (end[0] == b',' or end[0] == b')') and not quoted:
            break
        elif (
            end +1 < bufend
            and (
                (end[0] == b'\\' and end[1] == b'\\')
                or (end[0] == b'"' and end[1] == b'"'))):
            num_escapes += 1
            end += 1
        elif end[0] == b'"':
            quoted = False
        end += 1

    bufptr[0] = end
    if has_quotes:
        end -= 1

    if end == start and not has_quotes:
        return None

    if not num_escapes:
        return start[:end-start]

    cdef size_t unesclen = end - start - num_escapes
    if unesclen > sclen[0]:
        scratch[0] = <char *>PyMem_Realloc(scratch[0], unesclen)
        sclen[0] = unesclen

    cdef const char *src = start
    cdef char *dst = scratch[0]
    while src < end:
        if (
            src + 1 < end
            and (
                (src[0] == b'\\' and src[1] == b'\\')
                or (src[0] == b'"' and src[1] == b'"'))):
            dst[0] = src[1]
            src += 2
        else:
            dst[0] = src[0]
            src += 1
        dst += 1

    return scratch[0][:unesclen]


@cython.final
cdef class RecordBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object _ctx
    cdef dict _txs

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        self._ctx = context
        self._txs = {}

    cdef object cload(self, const char *data, size_t length):
        record, oids = _parse_binary_record(data)

        cdef object tx
        cdef PyObject* tx_ptr = PyDict_GetItem(self._txs, oids)

        if tx_ptr is NULL:
            tx = Transformer(self._ctx)
            tx.set_loader_types(list(oids), self.format)
            PyDict_SetItem(self._txs, oids, tx)
        else:
            tx = <object>tx_ptr

        return tx.load_sequence(record)


cdef tuple _parse_binary_record(const char *data):
    cdef size_t offset = 0
    cdef int i

    cdef uint32_t benfields
    memcpy(&benfields, data, sizeof(benfields))
    cdef int nfields = endian.be32toh(benfields)
    offset += sizeof(benfields)
    cdef list record = PyList_New(nfields)
    cdef tuple oids = PyTuple_New(nfields)

    cdef int32_t beoid, befieldlength
    cdef object oid
    cdef Py_ssize_t fieldlength
    cdef object field

    for i in range(nfields):
        memcpy(&beoid, data + offset, sizeof(beoid))
        oid = <Oid>endian.be32toh(beoid)
        offset += sizeof(beoid)
        Py_INCREF(<object>oid)
        PyTuple_SET_ITEM(oids, i, <object>oid)

        memcpy(&befieldlength, data + offset, sizeof(befieldlength))
        offset += sizeof(befieldlength)

        if befieldlength == _binary_null:
            field = None
        else:
            fieldlength = endian.be32toh(befieldlength)
            field = data[offset:offset + fieldlength]
            offset += fieldlength

        Py_INCREF(field)
        PyList_SET_ITEM(record, i, field)

    return record, oids
