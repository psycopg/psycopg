cimport cython
from psycopg_c.pq cimport _buffer_as_string_and_size
from cpython.bytearray cimport PyByteArray_GET_SIZE, PyByteArray_AS_STRING, PyByteArray_FromStringAndSize
from libc.stdint cimport int32_t, uint32_t
from libc.string cimport memcpy
from psycopg_c._psycopg cimport endian, RowDumper, CDumper
from psycopg import errors as e


RANGE_EMPTY = 0x01  # range is empty
RANGE_LB_INC = 0x02  # lower bound is inclusive
RANGE_UB_INC = 0x04  # upper bound is inclusive
RANGE_LB_INF = 0x08  # lower bound is -infinity
RANGE_UB_INF = 0x10  # upper bound is +infinity


_EMPTY_HEAD = bytearray([RANGE_EMPTY])


# FIXME: exception handling in cdef variant
def _fail_dump(obj: Any) -> Buffer:
    raise e.InternalError("trying to dump a range element without information")


cdef RowDumper _fail_dumper = RowDumper()
_fail_dumper.dumpfunc = _fail_dump


cdef Py_ssize_t _dump_range_binary(obj, bytearray rv, Py_ssize_t offset, Transformer tx, object oid) except -1:
    CDumper.ensure_size(rv, offset, 1)
    if not obj:
        rv[offset] = RANGE_EMPTY
        return 1

    cdef Py_ssize_t pos = offset + 1

    cdef lower = obj.lower
    cdef upper = obj.upper
    cdef bint lower_inf = lower is None
    cdef bint upper_inf = upper is None

    cdef bint lower_inc = obj.lower_inc
    cdef bint upper_inc = obj.upper_inc
    cdef RowDumper row_dumper

    # FIXME: clarify why the fail_dump constellation is solved indirectly
    # FIXME: does it still apply to _inner_oid, where we always know the dumper?
    if not lower_inf or not upper_inf:
        if oid:
            row_dumper = <RowDumper>tx.get_dumper_by_oid(<PyObject *>oid, <PyObject *>PQ_BINARY)
        else:
            row_dumper = <RowDumper>tx.get_row_dumper(
                <PyObject *>(lower if not lower_inf else upper), <PyObject *>PG_BINARY)
    else:
        row_dumper = _fail_dumper

    # write header w'o branching
    cdef int head = (<int>lower_inc) << 1 | (<int>upper_inc) << 2 | (<int>lower_inf) << 3 | (<int>upper_inf) << 4

    cdef Py_ssize_t size
    cdef uint32_t besize
    cdef char *buf
    cdef char *target

    if row_dumper.cdumper is not None:
        if not lower_inf:
            size = row_dumper.cdumper.cdump(lower, rv, pos + sizeof(besize))
            besize = endian.htobe32(<int32_t>size)
            target = PyByteArray_AS_STRING(rv)
            memcpy(target + pos, <void *>&besize, sizeof(besize))
            pos += size + sizeof(besize)
        if not upper_inf:
            size = row_dumper.cdumper.cdump(upper, rv, pos + sizeof(besize))
            besize = endian.htobe32(<int32_t>size)
            target = PyByteArray_AS_STRING(rv)
            memcpy(target + pos, <void *>&besize, sizeof(besize))
            pos += size + sizeof(besize)
    else:
        if not lower_inf:
            b = PyObject_CallFunctionObjArgs(
                row_dumper.dumpfunc, <PyObject *>lower, NULL)
            if b is None:
                head |= RANGE_LB_INF
            else:
                _buffer_as_string_and_size(b, &buf, &size)
                target = CDumper.ensure_size(rv, pos, size + sizeof(besize))
                besize = endian.htobe32(<int32_t>size)
                memcpy(target, <void *>&besize, sizeof(besize))
                memcpy(target + sizeof(besize), buf, size)
                pos += size + sizeof(besize)
        if not upper_inf:
            b = PyObject_CallFunctionObjArgs(
                row_dumper.dumpfunc, <PyObject *>upper, NULL)
            if b is None:
                head |= RANGE_UB_INF
            else:
                _buffer_as_string_and_size(b, &buf, &size)
                target = CDumper.ensure_size(rv, pos, size + sizeof(besize))
                besize = endian.htobe32(<int32_t>size)
                memcpy(target, <void *>&besize, sizeof(besize))
                memcpy(target + sizeof(besize), buf, size)
                pos += size + sizeof(besize)

    rv[offset] = head
    return pos - offset


# FIXME: not needed anymore, once tx.get_dumper_by_oid gets exposed to python
def dump_range_binary(tx: Transformer, obj: Any, oid: int | None) -> bytearray:
    cdef bytearray rv = PyByteArray_FromStringAndSize("", 0)
    _dump_range_binary(obj, rv, 0, tx, oid)
    return rv


cdef class _RangeBinaryDumper(CDumper):
    format = PQ_BINARY
    _inner_oid = None
    cdef Transformer _tx

    def __cinit__(self, cls, context: AdaptContext | None = None):
        self._tx = Transformer.from_context(context)

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return _dump_range_binary(obj, rv, offset, self._tx, self._inner_oid)


@cython.final
cdef class Int4RangeBinaryDumper(_RangeBinaryDumper):
    oid = oids.INT4RANGE_OID


@cython.final
cdef class Int8RangeBinaryDumper(_RangeBinaryDumper):
    oid = oids.INT8RANGE_OID
    _inner_oid = oids.INT8_OID


@cython.final
cdef class NumericRangeBinaryDumper(_RangeBinaryDumper):
    oid = oids.NUMRANGE_OID


@cython.final
cdef class DateRangeBinaryDumper(_RangeBinaryDumper):
    oid = oids.DATERANGE_OID


@cython.final
cdef class TimestampRangeBinaryDumper(_RangeBinaryDumper):
    oid = oids.TSRANGE_OID


@cython.final
cdef class TimestamptzRangeBinaryDumper(_RangeBinaryDumper):
    oid = oids.TSTZRANGE_OID



cdef Py_ssize_t _escape_text(bytearray rv, Py_ssize_t pos, Py_ssize_t size):
    cdef char *target = PyByteArray_AS_STRING(rv) + pos
    cdef int additional = 0
    cdef int needs_escape = 0
    cdef int tmpsize = size
    cdef char c
    for j in range(size):
        if (c := range_escape_lut[target[j]]):
            needs_escape = 1
            if c == b'"' or c == b"\\":
                additional += 1
    if needs_escape:
        additional += 2
    if additional > 0:
        tmpsize = size + additional
        target = CDumper.ensure_size(rv, pos, tmpsize)
        target[tmpsize - 1] = b'"'
        additional -= 1
        for j in range(<int>size - 1, -1, -1):
            c = target[j]
            if c == b'"' or c == b"\\":
                target[j + additional] = c
                additional -= 1
                target[j + additional] = c
            else:
                target[j + additional] = target[j]
        target[0] = b'"'
    return tmpsize


cdef Py_ssize_t _dump_range_text(obj, bytearray rv, Py_ssize_t offset, Transformer tx) except -1:
    cdef char* empty_string = b"empty"
    cdef char *target

    # FIXME: obj.isempty in TEXT vs. (not obj) in BINARY?
    if obj.isempty:
        target = CDumper.ensure_size(rv, offset, 5)
        memcpy(target, empty_string, 5)
        return 5

    cdef Py_ssize_t pos = offset

    cdef lower = obj.lower
    cdef upper = obj.upper
    cdef bint lower_inf = lower is None
    cdef bint upper_inf = upper is None
    cdef bint lower_inc = obj.lower_inc
    cdef bint upper_inc = obj.upper_inc
    cdef RowDumper row_dumper

    if not lower_inf or not upper_inf:
        row_dumper = <RowDumper>tx.get_row_dumper(
            <PyObject *>(lower if not lower_inf else upper), <PyObject *>PG_TEXT)
    else:
        row_dumper = _fail_dumper

    target = CDumper.ensure_size(rv, pos, 3)
    target[0] = b"[" if lower_inc else b"("
    pos += 1

    cdef Py_ssize_t size
    cdef char *buf

    if not lower_inf:
        if row_dumper.cdumper is not None:
            size = row_dumper.cdumper.cdump(lower, rv, pos)
        else:
            b = PyObject_CallFunctionObjArgs(
                row_dumper.dumpfunc, <PyObject *>lower, NULL)
            if not b:
                # FIXME: None is treated differently than (not b)?
                size = -1 if b is None else 0
            else:
                _buffer_as_string_and_size(b, &buf, &size)
                if size:
                    target = CDumper.ensure_size(rv, pos, size)
                    memcpy(target, buf, size)
        if not size:
            target = CDumper.ensure_size(rv, pos, 2)
            target[0] = b"\""
            target[1] = b"\""
            pos += 2
        elif size > 0:
            pos += _escape_text(rv, pos, size)

    target = CDumper.ensure_size(rv, pos, 2)
    target[0] = b","
    pos += 1

    if not upper_inf:
        if row_dumper.cdumper is not None:
            size = row_dumper.cdumper.cdump(upper, rv, pos)
        else:
            b = PyObject_CallFunctionObjArgs(
                row_dumper.dumpfunc, <PyObject *>lower, NULL)
            if not b:
                # FIXME: None is treated differently than (not b)?
                size = -1 if b is None else 0
            else:
                _buffer_as_string_and_size(b, &buf, &size)
                if size:
                    target = CDumper.ensure_size(rv, pos, size)
                    memcpy(target, buf, size)
        if not size:
            target = CDumper.ensure_size(rv, pos, 2)
            target[0] = b"\""
            target[1] = b"\""
            pos += 2
        elif size > 0:
            pos += _escape_text(rv, pos, size)

    target = CDumper.ensure_size(rv, pos, 1)
    target[0] = b"]" if upper_inc else b")"
    pos += 1

    return pos - offset


cdef class _RangeDumper(CDumper):
    format = PQ_TEXT
    cdef Transformer _tx

    def __cinit__(self, cls, context: AdaptContext | None = None):
        self._tx = Transformer.from_context(context)

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return _dump_range_text(obj, rv, offset, self._tx)


@cython.final
cdef class Int4RangeDumper(_RangeDumper):
    oid = oids.INT4RANGE_OID


@cython.final
cdef class Int8RangeDumper(_RangeDumper):
    oid = oids.INT8RANGE_OID


@cython.final
cdef class NumericRangeDumper(_RangeDumper):
    oid = oids.NUMRANGE_OID


@cython.final
cdef class DateRangeDumper(_RangeDumper):
    oid = oids.DATERANGE_OID


@cython.final
cdef class TimestampRangeDumper(_RangeDumper):
    oid = oids.TSRANGE_OID


@cython.final
cdef class TimestamptzRangeDumper(_RangeDumper):
    oid = oids.TSTZRANGE_OID



cdef extern from *:
    """
/* ",\\()[] \t\n\r\f\v --> [9, 10, 11, 12, 13, 32, 34, 40, 41, 44, 91, 92, 93] */

static const char range_escape_lut[] = {
  0,   0,   0,   0,   0,   0,   0,   0,   0,   9,  10,  11,  12,  13,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
 32,   0,  34,   0,   0,   0,   0,   0,  40,  41,   0,   0,  44,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  91,  92,  93,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
};
    """
    const char[256] range_escape_lut
