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
