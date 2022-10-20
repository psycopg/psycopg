"""
C optimized functions to manipulate arrays
"""

# Copyright (C) 2022 The Psycopg Team

import cython

from libc.stdint cimport int32_t, uint32_t
from libc.string cimport strchr
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.ref cimport Py_INCREF
from cpython.list cimport PyList_New, PyList_SET_ITEM, PyList_GetSlice

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

    cdef char *buf
    cdef Py_ssize_t length
    _buffer_as_string_and_size(data, &buf, &length)
    load = loader.load

    if length == 0:
        raise e.DataError("malformed array: empty data")

    cdef char *end = buf + length

    # Remove the dimensions information prefix (``[...]=``)
    if buf[0] == b"[":
        buf = strchr(buf + 1, b'=')
        if buf == NULL:
            raise e.DataError("malformed array: no '=' after dimension information")
        buf += 1

    rv = None
    stack: List[Any] = []

    while buf < end:
        if buf[0] == b'{':
            a = []
            if rv is None:
                rv = a
            if stack:
                stack[-1].append(a)
            stack.append(a)
            buf += 1

        elif buf[0] == b'}':
            if not stack:
                raise e.DataError("malformed array: unexpected '}'")
            rv = stack.pop()
            buf += 1

        elif buf[0] == cdelim:
            buf += 1

        else:
            v = parse_token(&buf, end, cdelim, load)
            if not stack:
                raise e.DataError("malformed array: missing initial '{'")
            stack[-1].append(v)

    assert rv is not None
    return rv


cdef object parse_token(char **bufptr, char *bufend, char cdelim, object load):
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

    cdef char *unesc
    cdef char *src
    cdef char *tgt

    if not num_escapes:
        # TODO: fast path for C dumpers and no copy
        b = start[:length]
        return load(b)

    else:
        unesc = <char *>PyMem_Malloc(length - num_escapes)
        src = start
        tgt = unesc
        while src < end:
            if src[0] == b'\\':
                src += 1
            tgt[0] = src[0]
            src += 1
            tgt += 1

        try:
            b = unesc[:length - num_escapes]
            return load(b)
        finally:
            PyMem_Free(unesc)


@cython.cdivision(True)
def array_load_binary(data: Buffer, tx: Transformer) -> List[Any]:
    cdef char *buf
    cdef Py_ssize_t length
    _buffer_as_string_and_size(data, &buf, &length)

    # head is ndims, hasnull, elem oid
    cdef uint32_t *buf32 = <uint32_t *>buf
    cdef int ndims = endian.be32toh(buf32[0])

    if ndims == 0:
        return []
    elif ndims > MAXDIM:
        raise e.DataError(
            r"unexpected number of dimensions %s exceeding the maximum allowed %s"
            % (ndims, MAXDIM)
        )

    cdef Oid oid = <Oid>endian.be32toh(buf32[2])
    load = tx.get_loader(oid, PQ_BINARY).load

    cdef Py_ssize_t dim
    cdef Py_ssize_t[MAXDIM] dims
    cdef Py_ssize_t nelems = 1
    cdef Py_ssize_t i
    for i in range(ndims):
        # Every dimension is dim, lower bound
        dims[i] = dim = endian.be32toh(buf32[3 + 2 * i])
        nelems *= dim

    buf += (3 + 2 * ndims) * sizeof(uint32_t)
    cdef list out = PyList_New(nelems)

    cdef Py_ssize_t size
    for i in range(nelems):
        size = <int32_t>endian.be32toh((<uint32_t *>buf)[0])
        buf += sizeof(uint32_t)
        if size == -1:
            Py_INCREF(None)
            PyList_SET_ITEM(out, i, None)
        else:
            # TODO: do without copy for C loaders
            val = load(buf[:size])
            Py_INCREF(val)
            PyList_SET_ITEM(out, i, val)
            buf += size

    # fon ndims > 1 we have to aggregate the array into sub-arrays
    cdef list tmp, tmp2
    cdef long j
    if ndims > 1:
        for i in range(ndims - 1, 0, -1):
            dim = dims[i]
            nelems //= dim
            tmp = PyList_New(nelems)
            for j in range(nelems):
                tmp2 = PyList_GetSlice(out, j * dim, (j + 1) * dim)
                Py_INCREF(tmp2)
                PyList_SET_ITEM(tmp, j, tmp2)

            out = tmp

    return out
