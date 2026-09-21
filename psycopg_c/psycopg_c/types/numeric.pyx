"""
Cython adapters for numeric types.
"""

# Copyright (C) 2020 The Psycopg Team

cimport cython
from cpython.mem cimport PyMem_Free
from libc.stdint cimport *
from libc.string cimport memcpy, memset, strlen
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem
from cpython.long cimport PyLong_AsLongLong, PyLong_FromLong, PyLong_FromLongLong
from cpython.long cimport PyLong_FromString, PyLong_FromUnsignedLong
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.float cimport PyFloat_AsDouble, PyFloat_FromDouble
from cpython.unicode cimport PyUnicode_DecodeUTF8

import sys
from decimal import Context, Decimal, DefaultContext

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


# global to hold types considered as integer python types
# _IntOrSubclassDumper and _MixedNumericDumper ctors
# change it to int or (int, numpy.integer) once
_int_classes = None


cdef class _IntDumper(CDumper):

    format = PQ_TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_text(obj, rv, offset)

    def quote(self, obj) -> abc.Buffer | None:
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


cdef class _IntOrSubclassDumper(_IntDumper):

    format = PQ_TEXT

    def __cinit__(self, cls, context: AdaptContext | None = None):
        global _int_classes

        if _int_classes is None:
            if "numpy" in sys.modules:
                import numpy
                _int_classes = (int, numpy.integer)
            else:
                _int_classes = int

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_or_sub_to_text(obj, rv, offset)


@cython.final
cdef class Int2Dumper(_IntOrSubclassDumper):

    oid = oids.INT2_OID


@cython.final
cdef class Int4Dumper(_IntOrSubclassDumper):

    oid = oids.INT4_OID


@cython.final
cdef class Int8Dumper(_IntOrSubclassDumper):

    oid = oids.INT8_OID


@cython.final
cdef class IntNumericDumper(_IntOrSubclassDumper):

    oid = oids.NUMERIC_OID


@cython.final
cdef class Int2BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.INT2_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_int2_binary(obj, rv, offset)


@cython.final
cdef class Int4BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.INT4_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_int4_binary(obj, rv, offset)


@cython.final
cdef class Int8BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.INT8_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_int8_binary(obj, rv, offset)


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

/* Limits of the numeric binary format: the weight is an int16 and the dscale
 * is stored in the lower 14 bits of the sign/dscale word.
 */
#define MAX_WEIGHT 0x7FFF
#define MAX_DSCALE 0x3FFF

/* Max number of pg digits in the integral part of a numeric: the weight of an
 * integer is ndigits - 1, so this is the largest valid ndigits for an integer.
 */
#define MAX_PGDIGITS (MAX_WEIGHT + 1)
"""
    const double BIT_PER_PGDIGIT
    const int DEC_DIGITS
    const int NUMERIC_POS
    const int NUMERIC_NEG
    const int NUMERIC_NAN
    const int NUMERIC_PINF
    const int NUMERIC_NINF
    const int MAX_WEIGHT
    const int MAX_DSCALE
    const int MAX_PGDIGITS


# The smallest integer that doesn't fit in a numeric (10_000 ** MAX_PGDIGITS).
# It is a 131073 digits number, taking about 57KB, and it is only needed to
# check values of that size: calculate it on first use and cache it.
cdef object MAX_NUMERIC_INT = None


@cython.final
cdef class IntNumericBinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_int_to_numeric_binary(obj, rv, offset)


cdef class IntDumper(CDumper):

    format = PQ_TEXT

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

        if INT32_MIN <= val <= INT32_MAX:
            if INT16_MIN <= val <= INT16_MAX:
                return Int2
            else:
                return Int4
        else:
            if INT64_MIN <= val <= INT64_MAX:
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

        if INT32_MIN <= val <= INT32_MAX:
            if INT16_MIN <= val <= INT16_MAX:
                return self._int2_dumper(Int2)
            else:
                return self._int4_dumper(Int4)
        else:
            if INT64_MIN <= val <= INT64_MAX:
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
        cdef int16_t bedata
        memcpy(&bedata, data, sizeof(bedata))
        return PyLong_FromLong(<int16_t>endian.be16toh(bedata))


@cython.final
cdef class Int4BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int32_t bedata
        memcpy(&bedata, data, sizeof(bedata))
        return PyLong_FromLong(<int32_t>endian.be32toh(bedata))


@cython.final
cdef class Int8BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t bedata
        memcpy(&bedata, data, sizeof(bedata))
        return PyLong_FromLongLong(<int64_t>endian.be64toh(bedata))


@cython.final
cdef class OidBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint32_t bedata
        memcpy(&bedata, data, sizeof(bedata))
        return PyLong_FromUnsignedLong(endian.be32toh(bedata))


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

    def quote(self, obj) -> abc.Buffer | None:
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
        cdef uint64_t ival
        memcpy(&ival, &d, sizeof(ival))
        cdef uint64_t beval = endian.htobe64(ival)
        cdef uint64_t *buf = <uint64_t *>CDumper.ensure_size(
            rv, offset, sizeof(beval))
        memcpy(buf, &beval, sizeof(beval))
        return sizeof(beval)


@cython.final
cdef class Float4BinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.FLOAT4_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef float f = <float>PyFloat_AsDouble(obj)
        cdef uint32_t ival
        memcpy(&ival, &f, sizeof(ival))
        cdef uint32_t beval = endian.htobe32(ival)
        cdef uint32_t *buf = <uint32_t *>CDumper.ensure_size(
            rv, offset, sizeof(beval))
        memcpy(buf, &beval, sizeof(beval))
        return sizeof(beval)


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
        cdef uint32_t bedata
        memcpy(&bedata, data, sizeof(bedata))
        cdef uint32_t asint = endian.be32toh(bedata)
        cdef float f
        memcpy(&f, &asint, sizeof(asint))
        return PyFloat_FromDouble(f)


@cython.final
cdef class Float8BinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t bedata
        memcpy(&bedata, data, sizeof(bedata))
        cdef uint64_t asint = endian.be64toh(bedata)
        cdef double d
        memcpy(&d, &asint, sizeof(asint))
        return PyFloat_FromDouble(d)


@cython.final
cdef class DecimalDumper(CDumper):

    format = PQ_TEXT
    oid = oids.NUMERIC_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        return dump_decimal_to_text(obj, rv, offset)

    def quote(self, obj) -> abc.Buffer | None:
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

        cdef uint16_t behead[4]
        memcpy(&behead, data, sizeof(behead))
        cdef uint16_t ndigits = endian.be16toh(behead[0])
        cdef int16_t weight = <int16_t>endian.be16toh(behead[1])
        cdef uint16_t sign = endian.be16toh(behead[2])
        cdef uint16_t dscale = endian.be16toh(behead[3])

        cdef int shift
        cdef int _i
        cdef PyObject *pctx
        cdef object key
        cdef const char *digitptr
        cdef uint16_t bedigit

        if sign == NUMERIC_POS or sign == NUMERIC_NEG:
            if length != (4 + ndigits) * sizeof(uint16_t):
                raise e.DataError("bad ndigits in numeric binary representation")

            val = 0
            digitptr = data + sizeof(behead)
            for _i in range(ndigits):
                memcpy(&bedigit, digitptr, sizeof(bedigit))
                digitptr += sizeof(bedigit)
                val *= 10_000
                val += endian.be16toh(bedigit)

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


cdef class _MixedNumericDumper(CDumper):

    oid = oids.NUMERIC_OID

    def __cinit__(self, cls, context: AdaptContext | None = None):
        global _int_classes

        if _int_classes is None:
            if "numpy" in sys.modules:
                import numpy
                _int_classes = (int, numpy.integer)
            else:
                _int_classes = int


@cython.final
cdef class NumericDumper(_MixedNumericDumper):

    format = PQ_TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        if type(obj) is int:  # fast path
            return dump_int_to_text(obj, rv, offset)
        elif isinstance(obj, Decimal):
            return dump_decimal_to_text(obj, rv, offset)
        elif isinstance(obj, _int_classes):
            return dump_int_to_text(obj, rv, offset)
        else:
            raise TypeError(
                f"class {type(self).__name__} cannot dump {type(obj).__name__}"
            )


@cython.final
cdef class NumericBinaryDumper(_MixedNumericDumper):

    format = PQ_BINARY

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        if type(obj) is int:
            return dump_int_to_numeric_binary(obj, rv, offset)
        elif isinstance(obj, Decimal):
            return dump_decimal_to_numeric_binary(obj, rv, offset)
        elif isinstance(obj, _int_classes):
            return dump_int_to_numeric_binary(int(obj), rv, offset)
        else:
            raise TypeError(
                f"class {type(self).__name__} cannot dump {type(obj).__name__}"
            )


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
    cdef uint16_t behead[4]
    cdef Py_ssize_t length

    cdef object pyexp = t[2]
    cdef const char *bexp

    if not isinstance(pyexp, int):
        # Handle inf, nan
        buf = <uint16_t *>CDumper.ensure_size(rv, offset, sizeof(behead))
        behead[0] = 0
        behead[1] = 0
        behead[3] = 0
        bexp = PyUnicode_AsUTF8(pyexp)
        if bexp[0] == b'n' or bexp[0] == b'N':
            behead[2] = endian.htobe16(NUMERIC_NAN)
        elif bexp[0] == b'F':
            if sign:
                behead[2] = endian.htobe16(NUMERIC_NINF)
            else:
                behead[2] = endian.htobe16(NUMERIC_PINF)
        else:
            raise e.DataError(f"unexpected decimal exponent: {pyexp}")
        memcpy(buf, behead, sizeof(behead))
        return sizeof(behead)

    # Check the exponent before converting it to a C integer, in order to fail
    # with a DataError, not an OverflowError, on absurd values. The dscale
    # check is exact; the weight is checked exactly further down.
    if pyexp <= 0:
        if -pyexp > MAX_DSCALE:
            raise e.DataError(
                "decimal too precise for PostgreSQL numeric binary format"
                f" (maximum {MAX_DSCALE} digits after the decimal point)"
            )
    elif pyexp > MAX_PGDIGITS * DEC_DIGITS:
        raise e.DataError(
            "decimal too large for PostgreSQL numeric binary format"
            f" (maximum {MAX_PGDIGITS} base-10000 digits)"
        )

    cdef Py_ssize_t exp = pyexp
    cdef Py_ssize_t ndigits = len(digits)

    # Find the last nonzero digit
    cdef Py_ssize_t nzdigits = ndigits
    while nzdigits > 0 and digits[nzdigits - 1] == 0:
        nzdigits -= 1

    cdef Py_ssize_t dscale
    if exp <= 0:
        dscale = -exp
    else:
        dscale = 0
        # align the py digits to the pg digits if there's some py exponent
        ndigits += exp % DEC_DIGITS

    if nzdigits == 0:
        buf = <uint16_t *>CDumper.ensure_size(rv, offset, sizeof(behead))
        behead[0] = 0  # ndigits
        behead[1] = 0  # weight
        behead[2] = endian.htobe16(NUMERIC_POS)  # sign
        behead[3] = endian.htobe16(<uint16_t>dscale)
        memcpy(buf, behead, sizeof(behead))
        return sizeof(behead)

    # Equivalent of 0-padding left to align the py digits to the pg digits
    # but without changing the digits tuple.
    cdef Py_ssize_t wi = 0
    cdef Py_ssize_t mod = (ndigits - dscale) % DEC_DIGITS
    if mod < 0:
        # the difference between C and Py % operator
        mod += 4
    if mod:
        wi = DEC_DIGITS - mod
        ndigits += wi

    # The weight is an int16: the digits before the decimal point are limited
    # even if the number of pg digits would fit in its uint16 field.
    # Note: ndigits + exp is a multiple of DEC_DIGITS, so C and Py division
    # agree on the result even if the value is negative.
    cdef Py_ssize_t weight = (ndigits + exp) // DEC_DIGITS - 1
    if weight > MAX_WEIGHT:
        raise e.DataError(
            "decimal too large for PostgreSQL numeric binary format"
            f" (maximum {MAX_PGDIGITS} base-10000 digits)"
        )

    cdef Py_ssize_t tmp = nzdigits + wi
    cdef Py_ssize_t pgdigits = tmp // DEC_DIGITS + (tmp % DEC_DIGITS and 1)
    length = sizeof(behead) + pgdigits * sizeof(uint16_t)
    buf = <uint16_t*>CDumper.ensure_size(rv, offset, length)
    behead[0] = endian.htobe16(<uint16_t>pgdigits)
    behead[1] = endian.htobe16(<uint16_t><int16_t>weight)
    behead[2] = endian.htobe16(NUMERIC_NEG) if sign else endian.htobe16(NUMERIC_POS)
    behead[3] = endian.htobe16(<uint16_t>dscale)
    memcpy(buf, behead, sizeof(behead))
    buf += 4

    cdef uint16_t pgdigit = 0, bedigit
    for i in range(nzdigits):
        pgdigit += pydigit_weights[wi] * <int>(digits[i])
        wi += 1
        if wi >= DEC_DIGITS:
            bedigit = endian.htobe16(pgdigit)
            memcpy(buf, &bedigit, sizeof(bedigit))
            buf += 1
            pgdigit = wi = 0

    if pgdigit:
        bedigit = endian.htobe16(pgdigit)
        memcpy(buf, &bedigit, sizeof(bedigit))

    return length


cdef Py_ssize_t dump_int_to_text(obj, bytearray rv, Py_ssize_t offset) except -1:
    cdef long long val
    cdef int overflow
    cdef char *buf
    cdef char *src
    cdef Py_ssize_t length

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


cdef Py_ssize_t dump_int_or_sub_to_text(
    obj, bytearray rv, Py_ssize_t offset
) except -1:
    cdef long long val
    cdef int overflow
    cdef char *buf
    cdef char *src
    cdef Py_ssize_t length

    # Ensure an int or a subclass. The 'is' type check is fast.
    # Passing a float must give an error, but passing an Enum should work.
    if type(obj) is not int and not isinstance(obj, _int_classes):
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


cdef Py_ssize_t dump_int_to_int2_binary(
    obj, bytearray rv, Py_ssize_t offset
) except -1:
    cdef int16_t val = <int16_t>PyLong_AsLongLong(obj)
    cdef int16_t *buf = <int16_t *>CDumper.ensure_size(rv, offset, sizeof(val))
    cdef uint16_t beval = endian.htobe16(val)  # swap bytes if needed
    memcpy(buf, &beval, sizeof(beval))
    return sizeof(val)


cdef Py_ssize_t dump_int_to_int4_binary(
    obj, bytearray rv, Py_ssize_t offset
) except -1:
    cdef int32_t val = <int32_t>PyLong_AsLongLong(obj)
    cdef int32_t *buf = <int32_t *>CDumper.ensure_size(rv, offset, sizeof(val))
    cdef uint32_t beval = endian.htobe32(val)  # swap bytes if needed
    memcpy(buf, &beval, sizeof(beval))
    return sizeof(val)


cdef Py_ssize_t dump_int_to_int8_binary(
    obj, bytearray rv, Py_ssize_t offset
) except -1:
    cdef int64_t val = PyLong_AsLongLong(obj)
    cdef int64_t *buf = <int64_t *>CDumper.ensure_size(rv, offset, sizeof(val))
    cdef uint64_t beval = endian.htobe64(val)  # swap bytes if needed
    memcpy(buf, &beval, sizeof(beval))
    return sizeof(val)


cdef Py_ssize_t dump_int_to_numeric_binary(
    obj, bytearray rv, Py_ssize_t offset
) except -1:
    # Calculate the number of PG digits required to store the number
    cdef Py_ssize_t nbits = obj.bit_length()
    cdef Py_ssize_t ndigits = <Py_ssize_t>(nbits * BIT_PER_PGDIGIT) + 1
    # ndigits is uint16 in the wire format, but the weight below is int16.
    # Since an integer's weight is ndigits - 1, MAX_PGDIGITS is the last
    # valid count.
    if ndigits > MAX_PGDIGITS:
        global MAX_NUMERIC_INT
        if MAX_NUMERIC_INT is None:
            # Note: use pow(), not '**', which Cython would compile into a
            # C double pow(), returning inf instead of an exact integer.
            MAX_NUMERIC_INT = pow(10_000, MAX_PGDIGITS)

        # The bit-length estimate may include one leading zero PG digit.
        # Resolve the boundary exactly before rejecting the value.
        if abs(obj) >= MAX_NUMERIC_INT:
            raise e.DataError(
                "integer too large for PostgreSQL numeric binary format"
                f" (maximum {MAX_PGDIGITS} base-10000 digits)"
            )
        ndigits = MAX_PGDIGITS

    cdef uint16_t sign = NUMERIC_POS
    if obj < 0:
        sign = NUMERIC_NEG
        obj = -obj

    cdef Py_ssize_t length = sizeof(uint16_t) * (ndigits + 4)
    cdef uint16_t *buf
    buf = <uint16_t *><void *>CDumper.ensure_size(rv, offset, length)

    cdef uint16_t behead[4]
    behead[0] = endian.htobe16(<uint16_t>ndigits)
    behead[1] = endian.htobe16(<uint16_t>(ndigits - 1))  # weight
    behead[2] = endian.htobe16(sign)
    behead[3] = 0  # dscale
    memcpy(buf, behead, sizeof(behead))

    cdef int i = 4 + ndigits - 1
    cdef uint16_t rem, berem
    while obj:
        rem = obj % 10000
        obj //= 10000
        berem = endian.htobe16(rem)
        memcpy(buf + i, &berem, sizeof(berem))
        i -= 1
    while i > 3:
        memset(buf + i, 0, sizeof(buf[0]))
        i -= 1

    return length
