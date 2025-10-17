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


cdef inline _cdump_inline(bytearray out, CDumper dumper, object obj):
    cdef uint32_t besize
    cdef Py_ssize_t pos = PyByteArray_GET_SIZE(out)
    cdef Py_ssize_t size = dumper.cdump(obj, out, pos + sizeof(besize))
    besize = endian.htobe32(<int32_t>size)
    cdef char *target = PyByteArray_AS_STRING(out)
    memcpy(target + pos, <void *>&besize, sizeof(besize))


cdef inline _write_dumped_obj(bytearray out, object obj):
    cdef Py_ssize_t size
    cdef uint32_t besize
    cdef char *buf
    _buffer_as_string_and_size(obj, &buf, &size)
    cdef Py_ssize_t pos = PyByteArray_GET_SIZE(out)
    cdef char *target = CDumper.ensure_size(out, pos, size + sizeof(besize))
    besize = endian.htobe32(<int32_t>size)
    memcpy(target, <void *>&besize, sizeof(besize))
    memcpy(target + sizeof(besize), buf, size)


def _fail_dump(obj: Any) -> Buffer:
    raise e.InternalError("trying to dump a range element without information")


cdef RowDumper _fail_dumper = RowDumper()
_fail_dumper.dumpfunc = _fail_dump


def dump_range_binary(tx: Transformer, obj: Any, oid: int | None) -> bytearray:
    if not obj:
        return _EMPTY_HEAD

    cdef lower = obj.lower
    cdef upper = obj.upper
    cdef bint lower_inf = lower is None
    cdef bint upper_inf = upper is None

    cdef bytearray out = PyByteArray_FromStringAndSize("", 1)
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

    if row_dumper.cdumper is not None:
        if not lower_inf:
            _cdump_inline(out, row_dumper.cdumper, lower)
        if not upper_inf:
            _cdump_inline(out, row_dumper.cdumper, upper)
    else:
        if not lower_inf:
            b = PyObject_CallFunctionObjArgs(
                row_dumper.dumpfunc, <PyObject *>lower, NULL)
            if b is None:
                head |= RANGE_LB_INF
            else:
                _write_dumped_obj(out, b)
        if not upper_inf:
            b = PyObject_CallFunctionObjArgs(
                row_dumper.dumpfunc, <PyObject *>upper, NULL)
            if b is None:
                head |= RANGE_UB_INF
            else:
                _write_dumped_obj(out, b)

    out[0] = head
    return out


cdef class _RangeBinaryDumper(CDumper):
    format = PQ_BINARY
    _inner_oid = None
    cdef Transformer _tx

    def __cinit__(self, cls, context: AdaptContext | None = None):
        self._tx = Transformer.from_context(context)

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        # FIXME: rewrite dump_range_binary for inplace writing as cdef
        cdef bytearray out = dump_range_binary(self._tx, obj, self._inner_oid)
        cdef char *src = PyByteArray_AS_STRING(out)
        cdef Py_ssize_t size = PyByteArray_GET_SIZE(out)
        cdef char *target = CDumper.ensure_size(rv, offset, size)
        memcpy(target, src, size)
        return size


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
