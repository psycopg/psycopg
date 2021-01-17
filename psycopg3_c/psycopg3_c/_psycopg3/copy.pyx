"""
C optimised functions for the copy system.

"""

# Copyright (C) 2020-2021 The Psycopg Team

from libc.string cimport memcpy
from libc.stdint cimport uint16_t, uint32_t, int32_t
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from cpython.bytearray cimport PyByteArray_AS_STRING, PyByteArray_GET_SIZE
from cpython.memoryview cimport PyMemoryView_FromObject

from psycopg3_c._psycopg3 cimport endian
from psycopg3_c.pq cimport ViewBuffer

from psycopg3 import errors as e

cdef int32_t _binary_null = -1


def format_row_binary(
    row: Sequence[Any], tx: Transformer, out: bytearray = None
) -> bytearray:
    """Convert a row of adapted data to the data to send for binary copy"""
    cdef Py_ssize_t rowlen = len(row)
    cdef uint16_t berowlen = endian.htobe16(rowlen)

    cdef Py_ssize_t pos  # offset in 'out' where to write
    if out is None:
        out = PyByteArray_FromStringAndSize("", 0)
        pos = 0
    else:
        pos = PyByteArray_GET_SIZE(out)

    # let's start from a nice chunk
    # (larger than most fixed size; for variable ones, oh well, we'll resize it)
    cdef char *target = CDumper.ensure_size(
        out, pos, sizeof(berowlen) + 20 * rowlen)

    # Write the number of fields as network-order 16 bits
    memcpy(target, <void *>&berowlen, sizeof(berowlen))
    pos += sizeof(berowlen)

    cdef Py_ssize_t size
    cdef uint32_t besize
    cdef char *buf
    cdef int i
    cdef PyObject *fmt = <PyObject *>PG_BINARY
    cdef PyObject *row_dumper

    for i in range(rowlen):
        item = row[i]
        if item is not None:
            row_dumper = tx.get_row_dumper(<PyObject *>item, fmt)
            if (<RowDumper>row_dumper).cdumper is not None:
                # A cdumper can resize if necessary and copy in place
                size = (<RowDumper>row_dumper).cdumper.cdump(
                    item, out, pos + sizeof(besize))
                # Also add the size of the item, before the item
                besize = endian.htobe32(size)
                target = PyByteArray_AS_STRING(out)  # might have been moved by cdump
                memcpy(target + pos, <void *>&besize, sizeof(besize))
            else:
                # A Python dumper, gotta call it and extract its juices
                b = PyObject_CallFunctionObjArgs(
                    (<RowDumper>row_dumper).dumpfunc, <PyObject *>item, NULL)
                _buffer_as_string_and_size(b, &buf, &size)
                target = CDumper.ensure_size(out, pos, size + sizeof(besize))
                besize = endian.htobe32(size)
                memcpy(target, <void *>&besize, sizeof(besize))
                memcpy(target + sizeof(besize), buf, size)

            pos += size + sizeof(besize)

        else:
            target = CDumper.ensure_size(out, pos, sizeof(_binary_null))
            memcpy(target, <void *>&_binary_null, sizeof(_binary_null))
            pos += sizeof(_binary_null)

    # Resize to the final size
    PyByteArray_Resize(out, pos)
    return out


def format_row_text(
    row: Sequence[Any], tx: Transformer, out: bytearray = None
) -> bytearray:
    cdef Py_ssize_t pos  # offset in 'out' where to write
    if out is None:
        out = PyByteArray_FromStringAndSize("", 0)
        pos = 0
    else:
        pos = PyByteArray_GET_SIZE(out)

    cdef Py_ssize_t rowlen = len(row)

    if rowlen == 0:
        PyByteArray_Resize(out, pos + 1)
        out[pos] = b"\n"
        return out

    cdef Py_ssize_t size, tmpsize
    cdef char *buf
    cdef int i, j
    cdef unsigned char *target
    cdef int nesc = 0
    cdef int with_tab
    cdef PyObject *fmt = <PyObject *>PG_TEXT
    cdef PyObject *row_dumper

    for i in range(rowlen):
        # Include the tab before the data, so it gets included in the resizes
        with_tab = i > 0

        item = row[i]
        if item is None:
            if with_tab:
                target = <unsigned char *>CDumper.ensure_size(out, pos, 3)
                memcpy(target, b"\t\\N", 3)
                pos += 3
            else:
                target = <unsigned char *>CDumper.ensure_size(out, pos, 2)
                memcpy(target, b"\\N", 2)
                pos += 2
            continue

        row_dumper = tx.get_row_dumper(<PyObject *>item, fmt)
        if (<RowDumper>row_dumper).cdumper is not None:
            # A cdumper can resize if necessary and copy in place
            size = (<RowDumper>row_dumper).cdumper.cdump(
                item, out, pos + with_tab)
            target = <unsigned char *>PyByteArray_AS_STRING(out) + pos
        else:
            # A Python dumper, gotta call it and extract its juices
            b = PyObject_CallFunctionObjArgs(
                (<RowDumper>row_dumper).dumpfunc, <PyObject *>item, NULL)
            _buffer_as_string_and_size(b, &buf, &size)
            target = <unsigned char *>CDumper.ensure_size(out, pos, size + with_tab)
            memcpy(target + with_tab, buf, size)

        # Prepend a tab to the data just written
        if with_tab:
            target[0] = b"\t"
            target += 1
            pos += 1

        # Now from pos to pos + size there is a textual representation: it may
        # contain chars to escape. Scan to find how many such chars there are.
        for j in range(size):
            nesc += copy_escape_lut[target[j]]

        # If there is any char to escape, walk backwards pushing the chars
        # forward and interspersing backslashes.
        if nesc > 0:
            tmpsize = size + nesc
            target = <unsigned char *>CDumper.ensure_size(out, pos, tmpsize)
            for j in range(size - 1, -1, -1):
                target[j + nesc] = target[j]
                if copy_escape_lut[target[j]] != 0:
                    nesc -= 1
                    target[j + nesc] = b"\\"
                    if nesc <= 0:
                        break
            pos += tmpsize
        else:
            pos += size

    # Resize to the final size, add the newline
    PyByteArray_Resize(out, pos + 1)
    out[pos] = b"\n"
    return out


def parse_row_binary(data, tx: Transformer) -> Tuple[Any, ...]:
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint16_t benfields = (<uint16_t *>ptr)[0]
    cdef int nfields = endian.be16toh(benfields)
    ptr += sizeof(benfields)
    cdef list row = PyList_New(nfields)

    cdef int col
    cdef int32_t belength
    cdef Py_ssize_t length

    for col in range(nfields):
        memcpy(&belength, ptr, sizeof(belength))
        ptr += sizeof(belength)
        if belength == _binary_null:
            field = None
        else:
            length = endian.be32toh(belength)
            if ptr + length > bufend:
                raise e.DataError("bad copy data: length exceeding data")
            field = PyMemoryView_FromObject(
                ViewBuffer._from_buffer(data, ptr, length))
            ptr += length

        Py_INCREF(field)
        PyList_SET_ITEM(row, col, field)

    return tx.load_sequence(row)


def parse_row_text(data, tx: Transformer) -> Tuple[Any, ...]:
    cdef unsigned char *fstart
    cdef Py_ssize_t size
    _buffer_as_string_and_size(data, <char **>&fstart, &size)

    # politely assume that the number of fields will be what in the result
    cdef int nfields = tx._nfields
    cdef list row = PyList_New(nfields)

    cdef unsigned char *fend
    cdef unsigned char *rowend = fstart + size
    cdef unsigned char *src
    cdef unsigned char *tgt
    cdef int col
    cdef int num_bs

    for col in range(nfields):
        fend = fstart
        num_bs = 0
        # Scan to the end of the field, remember if you see any backslash
        while fend[0] != b'\t' and fend[0] != b'\n' and fend < rowend:
            if fend[0] == b'\\':
                num_bs += 1
                # skip the next char to avoid counting escaped backslashes twice
                fend += 1
            fend += 1

        # Check if we stopped for the right reason
        if fend >= rowend:
            raise e.DataError("bad copy data: field delimiter not found")
        elif fend[0] == b'\t' and col == nfields - 1:
            raise e.DataError("bad copy data: got a tab at the end of the row")
        elif fend[0] == b'\n' and col != nfields - 1:
            raise e.DataError(
                "bad copy format: got a newline before the end of the row")

        # Is this a NULL?
        if fend - fstart == 2 and fstart[0] == b'\\' and fstart[1] == b'N':
            field = None

        # Is this a field with no backslash?
        elif num_bs == 0:
            # Nothing to unescape: we don't need a copy
            field = PyMemoryView_FromObject(
                ViewBuffer._from_buffer(data, fstart, fend - fstart))

        # This is a field containing backslashes
        else:
            # We need a copy of the buffer to unescape
            field = PyByteArray_FromStringAndSize("", 0)
            PyByteArray_Resize(field, fend - fstart - num_bs)
            tgt = <unsigned char *>PyByteArray_AS_STRING(field)
            src = fstart
            while (src < fend):
                if src[0] != b'\\':
                    tgt[0] = src[0]
                else:
                    src += 1
                    tgt[0] = copy_unescape_lut[src[0]]
                src += 1
                tgt += 1

        Py_INCREF(field)
        PyList_SET_ITEM(row, col, field)

        # Start of the field
        fstart = fend + 1

    # Convert the array of buffers into Python objects
    return tx.load_sequence(row)


cdef extern from *:
    """
/* handle chars to (un)escape in text copy representation */
/* '\b', '\t', '\n', '\v', '\f', '\r', '\\' */

/* Which char to prepend a backslash when escaping */
static const char copy_escape_lut[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};

/* Conversion of escaped to unescaped chars */
static const char copy_unescape_lut[] = {
  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,
 16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
 32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,
 48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,
 64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,
 80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,
 96,  97,   8,  99, 100, 101,  12, 103, 104, 105, 106, 107, 108, 109,  10, 111,
112, 113,  13, 115,   9, 117,  11, 119, 120, 121, 122, 123, 124, 125, 126, 127,
128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191,
192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207,
208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255,
};
    """
    const char[256] copy_escape_lut
    const char[256] copy_unescape_lut
