cimport cython
from cpython.ref cimport Py_INCREF
from libc.stdint cimport int32_t
from libc.string cimport memcpy
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.object cimport PyObject

from psycopg_c._psycopg cimport endian
from psycopg_c.pq.libpq cimport Oid


@cython.final
cdef class RecordLoader(_CRecursiveLoader):
    format = PQ_TEXT
    _text_oid = oids.TEXT_OID

    cdef PyObject *row_loader
    # A memory area used to unescape elements.
    # Keep it here to avoid a malloc per element and to
    # make sure to free it on error.
    # Cython initializes these to NULL and 0, respectively
    cdef char *scratch
    cdef size_t sclen

    cdef object cload(self, const char *data, size_t length):
        if length == 2:
            return ()

        if self.row_loader is NULL:
            self.row_loader = self._tx._c_get_loader(
                <PyObject *>self._text_oid, <PyObject *>PQ_TEXT)

        cdef RowLoader row_loader = <RowLoader>self.row_loader
        cdef CLoader cloader = None
        cdef object pyload = None
        if row_loader.cloader is not None:
            cloader = row_loader.cloader
        else:
            pyload = row_loader.loadfunc

        return _parse_text_record(
            data + 1, length - 2, &(self.scratch), &(self.sclen), cloader, pyload
        )

    def __dealloc__(self):
        if self.sclen > 0:
            PyMem_Free(self.scratch)


cdef tuple _parse_text_record(
    const char *data,
    size_t length,
    char **scratch_ptr,
    size_t *sclen_ptr,
    CLoader cloader,
    object load,
):
    cdef char *buf = data

    cdef Py_ssize_t nfields
    cdef size_t required_bytes
    nfields, required_bytes = _get_nfields_and_required_bytes(&data, length)

    if required_bytes > sclen_ptr[0]:
        scratch_ptr[0] = <char*>PyMem_Realloc(scratch_ptr[0], required_bytes)

    cdef tuple record = PyTuple_New(nfields)

    cdef char * start
    cdef size_t size
    cdef Py_ssize_t i = 0
    while buf < data + length:
        start, size = _parse_record_token(&buf, data + length, scratch_ptr)
        if start is NULL:
            field = None
        else:
            if cloader is not None:
                field = cloader.cload(start, size)
            elif load is not None:
                field = load(start[:size])
            else:
                field = start[:size]
        Py_INCREF(field)
        PyTuple_SET_ITEM(record, i, field)
        i += 1
        if buf[0] == b',':
            buf += 1
            if buf == data + length:
                Py_INCREF(None)
                PyTuple_SET_ITEM(record, i, None)

    return record


cdef inline (Py_ssize_t, size_t) _get_nfields_and_required_bytes(
    char **dataptr, size_t length
) noexcept nogil:
    cdef char *curr = dataptr[0]
    cdef Py_ssize_t nfields = 0
    cdef size_t required_bytes = 0

    while curr < dataptr[0] + length:
        required_bytes = max(
            required_bytes, _length_of_record_token(&curr, dataptr[0] + length)
        )
        nfields += 1
        if curr[0] == b',':
            curr += 1
            if curr == dataptr[0] + length:
                nfields += 1

    return nfields, required_bytes


cdef inline Py_ssize_t _length_of_record_token(
    char **bufptr, const char *bufend
) noexcept nogil:
    cdef char *start = bufptr[0]
    cdef char *end = start
    cdef bint quoted = end[0] == b'"'
    cdef size_t num_escapes = 0

    if quoted:
        end += 1

    while end < bufend:
        if end[0] == b',' and not quoted:
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

    if num_escapes > 0:
        return <Py_ssize_t> (end - start - num_escapes)
    else:
        return 0


cdef inline (char *, size_t) _parse_record_token(
    char **bufptr, const char *bufend, char **scratch_ptr
) noexcept nogil:
    cdef char *start = bufptr[0]
    cdef bint has_quotes = start[0] == b'"'
    cdef bint quoted = has_quotes
    cdef int num_escapes = 0
    cdef char *dst = scratch_ptr[0]

    if has_quotes:
        start += 1
    cdef char *end = start

    while end < bufend:
        if end[0] == b',' and not quoted:
            break
        elif has_quotes and (
            end + 1 < bufend
            and (
                (end[0] == b'\\' and end[1] == b'\\')
                or (end[0] == b'"' and end[1] == b'"'))):
            if num_escapes == 0:
                memcpy(dst, start, end + 1 - start)
                dst += end + 1 - start
            else:
                dst[0] = end[0]
                dst += 1
            num_escapes += 1
            end += 1
        elif end[0] == b'"':
            quoted = False
        elif num_escapes > 0:
            dst[0] = end[0]
            dst += 1
        end += 1

    bufptr[0] = end
    if has_quotes:
        end -= 1

    if end == start and not has_quotes:
        return NULL, 0

    if not num_escapes:
        return start, end - start

    return scratch_ptr[0], end - start - num_escapes


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
            tx = <Transformer>Transformer(self._ctx)
            tx.set_loader_types(oids, self.format)
            PyDict_SetItem(self._txs, oids, tx)
        else:
            tx = <Transformer>tx_ptr

        return tx.load_sequence(record)


cdef inline tuple _parse_binary_record(const char *data):
    cdef Py_ssize_t nfields
    cdef size_t offset
    nfields, offset = _get_nfields_and_offset(data)
    cdef tuple record = PyTuple_New(nfields)
    cdef tuple oids = PyTuple_New(nfields)

    cdef int32_t beoid, befieldlength
    cdef object oid
    cdef Py_ssize_t fieldlength

    cdef Py_ssize_t i
    for i in range(nfields):
        memcpy(&beoid, data + offset, sizeof(beoid))
        oid = <Oid>endian.be32toh(beoid)
        offset += sizeof(beoid)
        Py_INCREF(oid)
        PyTuple_SET_ITEM(oids, i, oid)

        memcpy(&befieldlength, data + offset, sizeof(befieldlength))
        offset += sizeof(befieldlength)

        if befieldlength == _binary_null:
            field = None
        else:
            fieldlength = endian.be32toh(befieldlength)
            field = data[offset:offset + fieldlength]
            offset += fieldlength

        Py_INCREF(field)
        PyTuple_SET_ITEM(record, i, field)

    return record, oids


cdef class _CompositeLoader(CLoader):
    """
    Base class to create text loaders of specific composite types.

    The class is complete but lack information about the fields types and
    object factory. These will be added by register_composite(), which will
    create a subclass of this class.
    """
    format = PQ_TEXT

    cdef Transformer _tx
    # A memory area used to unescape elements.
    # Keep it here to avoid a malloc per element and to
    # make sure to free it on error.
    # Cython initializes these to NULL and 0, respectively
    cdef char *scratch
    cdef size_t sclen

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        # Note: we cannot use the RecursiveLoader base class here because we
        # always want a different Transformer instance, otherwise the types
        # loaded will conflict with the types loaded by the record.
        self._tx = Transformer(context)

    def __init__(self, oid: int, context: abc.AdaptContext | None = None):
        self._tx.set_loader_types(self.info.field_types, self.format)

    cdef object cload(self, const char *data, size_t length):
        if length == 2:
            args = ()
        else:
            targs = _parse_text_record(
                data + 1, length - 2, &(self.scratch), &(self.sclen), None, None
            )
            args = self._tx.load_sequence(targs)
        return type(self).make_object(args, self.info)

    def __dealloc__(self):
        if self.sclen > 0:
            PyMem_Free(self.scratch)


cdef class _CompositeBinaryLoader(CLoader):
    """
    Base class to create text loaders of specific composite types.

    The class is complete but lack information about the fields types, names,
    and object factory. These will be added by register_composite(), which will
    create a subclass of this class.
    """
    format = PQ_BINARY

    cdef Transformer _tx

    def __cinit__(self, oid: int, context: abc.AdaptContext | None = None):
        self._tx = Transformer(context)
        self._tx.set_loader_types(self.info.field_types, self.format)

    def __init__(self, oid: int, context: abc.AdaptContext | None = None):
        self._tx.set_loader_types(self.info.field_types, self.format)

    cdef object cload(self, const char *data, size_t length):
        bargs = _parse_binary_composite(data)
        args = self._tx.load_sequence(bargs)
        return type(self).make_object(args, self.info)


cdef inline tuple _parse_binary_composite(const char *data):
    cdef size_t offset
    cdef Py_ssize_t nfields
    nfields, offset = _get_nfields_and_offset(data)
    cdef tuple record = PyTuple_New(nfields)

    cdef int32_t befieldlength, fieldlength

    cdef Py_ssize_t i
    for i in range(nfields):
        offset += sizeof(int32_t)  # skip oid

        memcpy(&befieldlength, data + offset, sizeof(befieldlength))
        offset += sizeof(befieldlength)

        if befieldlength == _binary_null:
            field = None
        else:
            fieldlength = endian.be32toh(befieldlength)
            field = data[offset:offset + fieldlength]
            offset += fieldlength

        Py_INCREF(field)
        PyTuple_SET_ITEM(record, i, field)

    return record


cdef inline (Py_ssize_t, size_t) _get_nfields_and_offset(char *data) noexcept nogil:
    cdef int32_t benfields
    memcpy(&benfields, data, sizeof(benfields))
    cdef Py_ssize_t nfields = endian.be32toh(benfields)
    return nfields, <size_t>sizeof(benfields)
