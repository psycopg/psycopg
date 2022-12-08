"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython

from libc.stdint cimport *
from libc.string cimport memcpy, strlen
from cpython.mem cimport PyMem_Free
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.long cimport (
    PyLong_FromString, PyLong_FromLong, PyLong_FromLongLong,
    PyLong_FromUnsignedLong, PyLong_AsLongLong)
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.float cimport PyFloat_FromDouble, PyFloat_AsDouble
from cpython.unicode cimport PyUnicode_DecodeUTF8

from decimal import Decimal, Context, DefaultContext

from psycopg_c._psycopg cimport endian
from psycopg import errors as e
from psycopg._wrappers import Int2, Int4, Int8, IntNumeric

cdef extern from "Python.h":
    # work around https://github.com/cython/cython/issues/3909
    double PyOS_string_to_double(
        const char *s, char **endptr, PyObject *overflow_exception) except? -1.0
    char *PyOS_double_to_string(
        double val, char format_code, int precision, int flags, int *ptype
    ) except NULL
    int Py_DTSF_ADD_DOT_0
    long long PyLong_AsLongLongAndOverflow(object pylong, int *overflow) except? -1

    # Missing in cpython/unicode.pxd
    const char *PyUnicode_AsUTF8(object unicode) except NULL


# defined in numutils.c
cdef extern from *:
    """
int pg_lltoa(int64_t value, char *a);
#define MAXINT8LEN 20
    """
    int pg_lltoa(int64_t value, char *a)
    const int MAXINT8LEN


cdef class _NumberDumper(CDumper):

    format = PQ_TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_text(obj, rv, offset)

    def quote(self, obj) -> bytearray:
        cdef Py_ssize_t length

        rv = PyByteArray_FromStringAndSize("", 0)
        if obj >= 0:
            length = self.cdump(obj, rv, 0)
        else:
            PyByteArray_Resize(rv, 23)
            rv[0] = b' '
            length = 1 + self.cdump(obj, rv, 1)

        PyByteArray_Resize(rv, length)
        return rv


@cython.final
cdef class Int2Dumper(_NumberDumper):

    oid = oids.INT2_OID


@cython.final
cdef class Int4Dumper(_NumberDumper):

    oid = oids.INT4_OID


@cython.final
cdef class Int8Dumper(_NumberDumper):

    oid = oids.INT8_OID


@cython.final
cdef class IntNumericDumper(_NumberDumper):

    oid = oids.NUMERIC_OID


@cython.final
cdef class Int2BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.INT2_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int16_t *buf = <int16_t *>CDumper.ensure_size(
            rv, offset, sizeof(int16_t))
        cdef int16_t val = <int16_t>PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint16_t *ptvar = <uint16_t *>(&val)
        buf[0] = endian.htobe16(ptvar[0])
        return sizeof(int16_t)


@cython.final
cdef class Int4BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.INT4_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int32_t *buf = <int32_t *>CDumper.ensure_size(
            rv, offset, sizeof(int32_t))
        cdef int32_t val = <int32_t>PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint32_t *ptvar = <uint32_t *>(&val)
        buf[0] = endian.htobe32(ptvar[0])
        return sizeof(int32_t)


@cython.final
cdef class Int8BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.INT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int64_t *buf = <int64_t *>CDumper.ensure_size(
            rv, offset, sizeof(int64_t))
        cdef int64_t val = PyLong_AsLongLong(obj)
        # swap bytes if needed
        cdef uint64_t *ptvar = <uint64_t *>(&val)
        buf[0] = endian.htobe64(ptvar[0])
        return sizeof(int64_t)


cdef extern from *:
    """
/* Ratio between number of bits required to store a number and number of pg
 * decimal digits required (log(2) / log(10_000)).
 */
#define BIT_PER_PGDIGIT 0.07525749891599529

/* decimal digits per Postgres "digit" */
#define DEC_DIGITS 4

#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_NAN 0xC000
#define NUMERIC_PINF 0xD000
#define NUMERIC_NINF 0xF000
"""
    const double BIT_PER_PGDIGIT
    const int DEC_DIGITS
    const int NUMERIC_POS
    const int NUMERIC_NEG
    const int NUMERIC_NAN
    const int NUMERIC_PINF
    const int NUMERIC_NINF


@cython.final
cdef class IntNumericBinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_numeric_binary(obj, rv, offset)


cdef class IntDumper(_NumberDumper):

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        raise TypeError(
            f"{type(self).__name__} is a dispatcher to other dumpers:"
            " dump() is not supposed to be called"
        )

    cpdef get_key(self, obj, format):
        cdef long long val
        cdef int overflow

        val = PyLong_AsLongLongAndOverflow(obj, &overflow)
        if overflow:
            return IntNumeric

        if INT32_MIN <= obj <= INT32_MAX:
            if INT16_MIN <= obj <= INT16_MAX:
                return Int2
            else:
                return Int4
        else:
            if INT64_MIN <= obj <= INT64_MAX:
                return Int8
            else:
                return IntNumeric

    _int2_dumper = Int2Dumper
    _int4_dumper = Int4Dumper
    _int8_dumper = Int8Dumper
    _int_numeric_dumper = IntNumericDumper

    cpdef upgrade(self, obj, format):
        cdef long long val
        cdef int overflow

        val = PyLong_AsLongLongAndOverflow(obj, &overflow)
        if overflow:
            return self._int_numeric_dumper(IntNumeric)

        if INT32_MIN <= obj <= INT32_MAX:
            if INT16_MIN <= obj <= INT16_MAX:
                return self._int2_dumper(Int2)
            else:
                return self._int4_dumper(Int4)
        else:
            if INT64_MIN <= obj <= INT64_MAX:
                return self._int8_dumper(Int8)
            else:
                return self._int_numeric_dumper(IntNumeric)


@cython.final
cdef class IntBinaryDumper(IntDumper):

    format = PQ_BINARY

    _int2_dumper = Int2BinaryDumper
    _int4_dumper = Int4BinaryDumper
    _int8_dumper = Int8BinaryDumper
    _int_numeric_dumper = IntNumericBinaryDumper


@cython.final
cdef class IntLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        # if the number ends with a 0 we don't need a copy
        if data[length] == b'\0':
            return PyLong_FromString(data, NULL, 10)

        # Otherwise we have to copy it aside
        if length > MAXINT8LEN:
            raise ValueError("string too big for an int")

        cdef char[MAXINT8LEN + 1] buf
        memcpy(buf, data, length)
        buf[length] = 0
        return PyLong_FromString(buf, NULL, 10)



@cython.final
cdef class Int2BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int16_t>endian.be16toh((<uint16_t *>data)[0]))


@cython.final
cdef class Int4BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLong(<int32_t>endian.be32toh((<uint32_t *>data)[0]))


@cython.final
cdef class Int8BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromLongLong(<int64_t>endian.be64toh((<uint64_t *>data)[0]))


@cython.final
cdef class OidBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        return PyLong_FromUnsignedLong(endian.be32toh((<uint32_t *>data)[0]))


cdef class _FloatDumper(CDumper):

    format = PQ_TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef double d = PyFloat_AsDouble(obj)
        cdef char *out = PyOS_double_to_string(
            d, b'r', 0, Py_DTSF_ADD_DOT_0, NULL)
        cdef Py_ssize_t length = strlen(out)
        cdef char *tgt = CDumper.ensure_size(rv, offset, length)
        memcpy(tgt, out, length)
        PyMem_Free(out)
        return length

    def quote(self, obj) -> bytes:
        value = bytes(self.dump(obj))
        cdef PyObject *ptr = PyDict_GetItem(_special_float, value)
        if ptr != NULL:
            return <object>ptr

        return value if obj >= 0 else b" " + value

cdef dict _special_float = {
    b"inf": b"'Infinity'::float8",
    b"-inf": b"'-Infinity'::float8",
    b"nan": b"'NaN'::float8",
}


@cython.final
cdef class FloatDumper(_FloatDumper):

    oid = oids.FLOAT8_OID


@cython.final
cdef class Float4Dumper(_FloatDumper):

    oid = oids.FLOAT4_OID


@cython.final
cdef class FloatBinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.FLOAT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef double d = PyFloat_AsDouble(obj)
        cdef uint64_t *intptr = <uint64_t *>&d
        cdef uint64_t *buf = <uint64_t *>CDumper.ensure_size(
            rv, offset, sizeof(uint64_t))
        buf[0] = endian.htobe64(intptr[0])
        return sizeof(uint64_t)


@cython.final
cdef class Float4BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.FLOAT4_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef float f = <float>PyFloat_AsDouble(obj)
        cdef uint32_t *intptr = <uint32_t *>&f
        cdef uint32_t *buf = <uint32_t *>CDumper.ensure_size(
            rv, offset, sizeof(uint32_t))
        buf[0] = endian.htobe32(intptr[0])
        return sizeof(uint32_t)


@cython.final
cdef class FloatLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        cdef char *endptr
        cdef double d = PyOS_string_to_double(
            data, &endptr, <PyObject *>OverflowError)
        return PyFloat_FromDouble(d)


@cython.final
cdef class Float4BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint32_t asint = endian.be32toh((<uint32_t *>data)[0])
        # avoid warning:
        # dereferencing type-punned pointer will break strict-aliasing rules
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<float *>swp)[0])


@cython.final
cdef class Float8BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t asint = endian.be64toh((<uint64_t *>data)[0])
        cdef char *swp = <char *>&asint
        return PyFloat_FromDouble((<double *>swp)[0])


@cython.final
cdef class DecimalDumper(CDumper):

    format = PQ_TEXT
    oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_decimal_to_text(obj, rv, offset)

    def quote(self, obj) -> bytes:
        value = bytes(self.dump(obj))
        cdef PyObject *ptr = PyDict_GetItem(_special_decimal, value)
        if ptr != NULL:
            return <object>ptr

        return value if obj >= 0 else b" " + value

cdef dict _special_decimal = {
    b"Infinity": b"'Infinity'::numeric",
    b"-Infinity": b"'-Infinity'::numeric",
    b"NaN": b"'NaN'::numeric",
}


@cython.final
cdef class NumericLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        s = PyUnicode_DecodeUTF8(<char *>data, length, NULL)
        return Decimal(s)


cdef dict _decimal_special = {
    NUMERIC_NAN: Decimal("NaN"),
    NUMERIC_PINF: Decimal("Infinity"),
    NUMERIC_NINF: Decimal("-Infinity"),
}

cdef dict _contexts = {}
for _i in range(DefaultContext.prec):
    _contexts[_i] = DefaultContext


@cython.final
cdef class NumericBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):

        cdef uint16_t *data16 = <uint16_t *>data
        cdef uint16_t ndigits = endian.be16toh(data16[0])
        cdef int16_t weight = <int16_t>endian.be16toh(data16[1])
        cdef uint16_t sign = endian.be16toh(data16[2])
        cdef uint16_t dscale = endian.be16toh(data16[3])
        cdef int shift
        cdef int i
        cdef PyObject *pctx
        cdef object key

        if sign == NUMERIC_POS or sign == NUMERIC_NEG:
            if length != (4 + ndigits) * sizeof(uint16_t):
                raise e.DataError("bad ndigits in numeric binary representation")

            val = 0
            for i in range(ndigits):
                val *= 10_000
                val += endian.be16toh(data16[i + 4])

            shift = dscale - (ndigits - weight - 1) * DEC_DIGITS

            key = (weight + 2) * DEC_DIGITS + dscale
            pctx = PyDict_GetItem(_contexts, key)
            if pctx == NULL:
                ctx = Context(prec=key)
                PyDict_SetItem(_contexts, key, ctx)
                pctx = <PyObject *>ctx

            return (
                Decimal(val if sign == NUMERIC_POS else -val)
                .scaleb(-dscale, <object>pctx)
                .shift(shift, <object>pctx)
            )
        else:
            try:
                return _decimal_special[sign]
            except KeyError:
                raise e.DataError(f"bad value for numeric sign: 0x{sign:X}")


@cython.final
cdef class DecimalBinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_decimal_to_numeric_binary(obj, rv, offset)


@cython.final
cdef class NumericDumper(CDumper):

    format = PQ_TEXT
    oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        if isinstance(obj, int):
            return dump_int_to_text(obj, rv, offset)
        else:
            return dump_decimal_to_text(obj, rv, offset)


@cython.final
cdef class NumericBinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        if isinstance(obj, int):
            return dump_int_to_numeric_binary(obj, rv, offset)
        else:
            return dump_decimal_to_numeric_binary(obj, rv, offset)


cdef Py_ssize_t dump_decimal_to_text(obj, bytearray rv, Py_ssize_t offset) except -1:
    cdef char *src
    cdef Py_ssize_t length
    cdef char *buf

    b = bytes(str(obj), "utf-8")
    PyBytes_AsStringAndSize(b, &src, &length)

    if src[0] != b's':
        buf = CDumper.ensure_size(rv, offset, length)
        memcpy(buf, src, length)

    else:  # convert sNaN to NaN
        length = 3  # NaN
        buf = CDumper.ensure_size(rv, offset, length)
        memcpy(buf, b"NaN", length)

    return length


cdef extern from *:
    """
/* Weights of py digits into a pg digit according to their positions. */
static const int pydigit_weights[] = {1000, 100, 10, 1};
"""
    const int[4] pydigit_weights


@cython.cdivision(True)
cdef Py_ssize_t dump_decimal_to_numeric_binary(
    obj, bytearray rv, Py_ssize_t offset
) except -1:

    # TODO: this implementation is about 30% slower than the text dump.
    # This might be probably optimised by accessing the C structure of
    # the Decimal object, if available, which would save the creation of
    # several intermediate Python objects (the DecimalTuple, the digits
    # tuple, and then accessing them).

    cdef object t = obj.as_tuple()
    cdef int sign = t[0]
    cdef tuple digits = t[1]
    cdef uint16_t *buf
    cdef Py_ssize_t length

    cdef object pyexp = t[2]
    cdef const char *bexp
    if not isinstance(pyexp, int):
        # Handle inf, nan
        length = 4 * sizeof(uint16_t)
        buf = <uint16_t *>CDumper.ensure_size(rv, offset, length)
        buf[0] = 0
        buf[1] = 0
        buf[3] = 0
        bexp = PyUnicode_AsUTF8(pyexp)
        if bexp[0] == b'n' or bexp[0] == b'N':
            buf[2] = endian.htobe16(NUMERIC_NAN)
        elif bexp[0] == b'F':
            if sign:
                buf[2] = endian.htobe16(NUMERIC_NINF)
            else:
                buf[2] = endian.htobe16(NUMERIC_PINF)
        else:
            raise e.DataError(f"unexpected decimal exponent: {pyexp}")
        return length

    cdef int exp = pyexp
    cdef uint16_t ndigits = <uint16_t>len(digits)

    # Find the last nonzero digit
    cdef int nzdigits = ndigits
    while nzdigits > 0 and digits[nzdigits - 1] == 0:
        nzdigits -= 1

    cdef uint16_t dscale
    if exp <= 0:
        dscale = -exp
    else:
        dscale = 0
        # align the py digits to the pg digits if there's some py exponent
        ndigits += exp % DEC_DIGITS

    if nzdigits == 0:
        length = 4 * sizeof(uint16_t)
        buf = <uint16_t *>CDumper.ensure_size(rv, offset, length)
        buf[0] = 0  # ndigits
        buf[1] = 0  # weight
        buf[2] = endian.htobe16(NUMERIC_POS)  # sign
        buf[3] = endian.htobe16(dscale)
        return length

    # Equivalent of 0-padding left to align the py digits to the pg digits
    # but without changing the digits tuple.
    cdef int wi = 0
    cdef int mod = (ndigits - dscale) % DEC_DIGITS
    if mod < 0:
        # the difference between C and Py % operator
        mod += 4
    if mod:
        wi = DEC_DIGITS - mod
        ndigits += wi

    cdef int tmp = nzdigits + wi
    cdef int pgdigits = tmp // DEC_DIGITS + (tmp % DEC_DIGITS and 1)
    length = (pgdigits + 4) * sizeof(uint16_t)
    buf = <uint16_t*>CDumper.ensure_size(rv, offset, length)
    buf[0] = endian.htobe16(pgdigits)
    buf[1] = endian.htobe16(<int16_t>((ndigits + exp) // DEC_DIGITS - 1))
    buf[2] = endian.htobe16(NUMERIC_NEG) if sign else endian.htobe16(NUMERIC_POS)
    buf[3] = endian.htobe16(dscale)

    cdef uint16_t pgdigit = 0
    cdef int bi = 4
    for i in range(nzdigits):
        pgdigit += pydigit_weights[wi] * <int>(digits[i])
        wi += 1
        if wi >= DEC_DIGITS:
            buf[bi] = endian.htobe16(pgdigit)
            pgdigit = wi = 0
            bi += 1

    if pgdigit:
        buf[bi] = endian.htobe16(pgdigit)

    return length


cdef Py_ssize_t dump_int_to_text(obj, bytearray rv, Py_ssize_t offset) except -1:
    cdef long long val
    cdef int overflow
    cdef char *buf
    cdef char *src
    cdef Py_ssize_t length

    # Ensure an int or a subclass. The 'is' type check is fast.
    # Passing a float must give an error, but passing an Enum should work.
    if type(obj) is not int and not isinstance(obj, int):
        raise e.DataError(f"integer expected, got {type(obj).__name__!r}")

    val = PyLong_AsLongLongAndOverflow(obj, &overflow)
    if not overflow:
        buf = CDumper.ensure_size(rv, offset, MAXINT8LEN + 1)
        length = pg_lltoa(val, buf)
    else:
        b = bytes(str(obj), "utf-8")
        PyBytes_AsStringAndSize(b, &src, &length)
        buf = CDumper.ensure_size(rv, offset, length)
        memcpy(buf, src, length)

    return length


cdef Py_ssize_t dump_int_to_numeric_binary(obj, bytearray rv, Py_ssize_t offset) except -1:
    # Calculate the number of PG digits required to store the number
    cdef uint16_t ndigits
    ndigits = <uint16_t>((<int>obj.bit_length()) * BIT_PER_PGDIGIT) + 1

    cdef uint16_t sign = NUMERIC_POS
    if obj < 0:
        sign = NUMERIC_NEG
        obj = -obj

    cdef Py_ssize_t length = sizeof(uint16_t) * (ndigits + 4)
    cdef uint16_t *buf
    buf = <uint16_t *><void *>CDumper.ensure_size(rv, offset, length)
    buf[0] = endian.htobe16(ndigits)
    buf[1] = endian.htobe16(ndigits - 1)  # weight
    buf[2] = endian.htobe16(sign)
    buf[3] = 0  # dscale

    cdef int i = 4 + ndigits - 1
    cdef uint16_t rem
    while obj:
        rem = obj % 10000
        obj //= 10000
        buf[i] = endian.htobe16(rem)
        i -= 1
    while i > 3:
        buf[i] = 0
        i -= 1

    return length
