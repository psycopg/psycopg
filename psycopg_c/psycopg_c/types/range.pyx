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
