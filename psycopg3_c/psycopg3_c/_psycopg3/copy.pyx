"""
C optimised functions for the copy system.

"""

# Copyright (C) 2020 The Psycopg Team

from libc.string cimport memcpy
from libc.stdint cimport uint16_t, uint32_t, int32_t
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from cpython.bytearray cimport PyByteArray_AS_STRING, PyByteArray_GET_SIZE

from psycopg3_c._psycopg3.endian cimport htobe16, htobe32

cdef int32_t _binary_null = -1


def format_row_binary(
    row: Sequence[Any], tx: Transformer, out: bytearray = None
) -> bytearray:
    """Convert a row of adapted data to the data to send for binary copy"""
    cdef Py_ssize_t rowlen = len(row)
    cdef uint16_t berowlen = htobe16(rowlen)

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

    for i in range(rowlen):
        item = row[i]
        if item is not None:
            dumper = tx.get_dumper(item, FORMAT_BINARY)
            if isinstance(dumper, CDumper):
                # A cdumper can resize if necessary and copy in place
                size = (<CDumper>dumper).cdump(item, out, pos + sizeof(besize))
                # Also add the size of the item, before the item
                besize = htobe32(size)
                target = PyByteArray_AS_STRING(out)  # might have been moved by cdump
                memcpy(target + pos, <void *>&besize, sizeof(besize))
            else:
                # A Python dumper, gotta call it and extract its juices
                b = dumper.dump(item)
                _buffer_as_string_and_size(b, &buf, &size)
                target = CDumper.ensure_size(out, pos, size + sizeof(besize))
                besize = htobe32(size)
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

        dumper = tx.get_dumper(item, FORMAT_TEXT)
        if isinstance(dumper, CDumper):
            # A cdumper can resize if necessary and copy in place
            size = (<CDumper>dumper).cdump(item, out, pos + with_tab)
            target = <unsigned char *>PyByteArray_AS_STRING(out) + pos
        else:
            # A Python dumper, gotta call it and extract its juices
            b = dumper.dump(item)
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
            nesc += copy_escape_char[target[j]]

        # If there is any char to escape, walk backwards pushing the chars
        # forward and interspersing backslashes.
        if nesc > 0:
            tmpsize = size + nesc
            target = <unsigned char *>CDumper.ensure_size(out, pos, tmpsize)
            for j in range(size - 1, -1, -1):
                target[j + nesc] = target[j]
                if copy_escape_char[target[j]] != 0:
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


cdef extern from *:
    """
/* The characters to escape in textual copy */
/* '\b', '\t', '\n', '\v', '\f', '\r', '\\' */
static const char copy_escape_char[] = {
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
    """
    const char[256] copy_escape_char
