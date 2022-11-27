"""
C optimized functions to manipulate arrays
"""

# Copyright (C) 2022 The Psycopg Team

import cython

from libc.stdint cimport int32_t, uint32_t
from libc.string cimport strchr
from cpython.mem cimport PyMem_Realloc, PyMem_Free
from cpython.ref cimport Py_INCREF
from cpython.list cimport PyList_New,PyList_Append, PyList_GetSlice
from cpython.list cimport PyList_GET_ITEM, PyList_SET_ITEM, PyList_GET_SIZE
from cpython.object cimport PyObject

from psycopg_c.pq cimport _buffer_as_string_and_size
from psycopg_c.pq.libpq cimport Oid
from psycopg_c._psycopg cimport endian

from psycopg import errors as e

cdef extern from *:
    """
/* Defined in PostgreSQL in src/include/utils/array.h */
#define MAXDIM 6
    """
    const int MAXDIM


def array_load_text(
    data: Buffer, loader: Loader, delimiter: bytes = b","
) -> List[Any]:
    cdef char cdelim = delimiter[0]

    cdef char *buf = NULL
    cdef Py_ssize_t length = 0
    _buffer_as_string_and_size(data, &buf, &length)

    cdef CLoader cloader = None
    cdef object pyload = None

    if isinstance(loader, CLoader):
        cloader = <CLoader>loader
    else:
        pyload = loader.load

    if length == 0:
        raise e.DataError("malformed array: empty data")

    cdef char *end = buf + length

    # Keep and grow a buffer instead of malloc'ing at each element
    cdef char *scratch = NULL
    cdef size_t sclen = 0

    # Remove the dimensions information prefix (``[...]=``)
    if buf[0] == b"[":
        buf = strchr(buf + 1, b'=')
        if buf == NULL:
            raise e.DataError("malformed array: no '=' after dimension information")
        buf += 1

    cdef list stack = []
    cdef list a = []
    rv = a
    cdef PyObject *tmp

    try:
        while buf < end:
            if buf[0] == b'{':
                if stack:
                    tmp = PyList_GET_ITEM(stack, PyList_GET_SIZE(stack) - 1)
                    PyList_Append(<object>tmp, a)
                PyList_Append(stack, a)
                a = []
                buf += 1

            elif buf[0] == b'}':
                if not stack:
                    raise e.DataError("malformed array: unexpected '}'")
                rv = stack.pop()
                buf += 1

            elif buf[0] == cdelim:
                buf += 1

            else:
                v = _parse_token(
                    &buf, end, cdelim, &scratch, &sclen, cloader, pyload)
                if not stack:
                    raise e.DataError("malformed array: missing initial '{'")
                tmp = PyList_GET_ITEM(stack, PyList_GET_SIZE(stack) - 1)
                PyList_Append(<object>tmp, v)

    finally:
        PyMem_Free(scratch)

    return rv


cdef object _parse_token(
    char **bufptr, char *bufend, char cdelim,
    char **scratch, size_t *sclen, CLoader cloader, object load
):
    cdef char *start = bufptr[0]
    cdef int has_quotes = start[0] == b'"'
    cdef int quoted = has_quotes
    cdef int num_escapes = 0
    cdef int escaped = 0

    if has_quotes:
        start += 1
    cdef char *end = start

    while end < bufend:
        if (end[0] == cdelim or end[0] == b'}') and not quoted:
            break
        elif end[0] == b'\\' and not escaped:
            num_escapes += 1
            escaped = 1
            end += 1
            continue
        elif end[0] == b'"' and not escaped:
            quoted = 0
        escaped = 0
        end += 1
    else:
        raise e.DataError("malformed array: hit the end of the buffer")

    # Return the new position for the buffer
    bufptr[0] = end
    if has_quotes:
        end -= 1

    cdef int length = (end - start)
    if length == 4 and not has_quotes \
            and start[0] == b'N' and start[1] == b'U' \
            and start[2] == b'L' and start[3] == b'L':
        return None

    cdef char *src
    cdef char *tgt
    cdef size_t unesclen

    if not num_escapes:
        if cloader is not None:
            return cloader.cload(start, length)
        else:
            b = start[:length]
            return load(b)

    else:
        unesclen = length - num_escapes + 1
        if unesclen > sclen[0]:
            scratch[0] = <char *>PyMem_Realloc(scratch[0], unesclen)
            sclen[0] = unesclen

        src = start
        tgt = scratch[0]
        while src < end:
            if src[0] == b'\\':
                src += 1
            tgt[0] = src[0]
            src += 1
            tgt += 1

        tgt[0] = b'\x00'

        if cloader is not None:
            return cloader.cload(scratch[0], length - num_escapes)
        else:
            b = scratch[0][:length - num_escapes]
            return load(b)


@cython.cdivision(True)
def array_load_binary(data: Buffer, Transformer tx) -> List[Any]:
    cdef char *buf = NULL
    cdef Py_ssize_t length = 0
    _buffer_as_string_and_size(data, &buf, &length)

    # head is ndims, hasnull, elem oid
    cdef uint32_t *buf32 = <uint32_t *>buf
    cdef int ndims = endian.be32toh(buf32[0])

    if ndims <= 0:
        return []
    elif ndims > MAXDIM:
        raise e.DataError(
            r"unexpected number of dimensions %s exceeding the maximum allowed %s"
            % (ndims, MAXDIM)
        )

    cdef object oid = <Oid>endian.be32toh(buf32[2])
    cdef PyObject *row_loader = tx._c_get_loader(<PyObject *>oid, <PyObject *>PQ_BINARY)

    cdef Py_ssize_t[MAXDIM] dims
    cdef int i
    for i in range(ndims):
        # Every dimension is dim, lower bound
        dims[i] = endian.be32toh(buf32[3 + 2 * i])

    buf += (3 + 2 * ndims) * sizeof(uint32_t)
    out = _array_load_binary_rec(ndims, dims, &buf, row_loader)
    return out


cdef object _array_load_binary_rec(
    Py_ssize_t ndims, Py_ssize_t *dims, char **bufptr, PyObject *row_loader
):
    cdef char *buf
    cdef int i
    cdef int32_t size
    cdef object val

    cdef Py_ssize_t nelems = dims[0]
    cdef list out = PyList_New(nelems)

    if ndims == 1:
        buf = bufptr[0]
        for i in range(nelems):
            size = <int32_t>endian.be32toh((<uint32_t *>buf)[0])
            buf += sizeof(uint32_t)
            if size == -1:
                val = None
            else:
                if (<RowLoader>row_loader).cloader is not None:
                    val = (<RowLoader>row_loader).cloader.cload(buf, size)
                else:
                    val = (<RowLoader>row_loader).loadfunc(buf[:size])
                buf += size

            Py_INCREF(val)
            PyList_SET_ITEM(out, i, val)

        bufptr[0] = buf

    else:
        for i in range(nelems):
            val = _array_load_binary_rec(ndims - 1, dims + 1, bufptr, row_loader)
            Py_INCREF(val)
            PyList_SET_ITEM(out, i, val)

    return out
