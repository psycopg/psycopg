"""
C optimised functions for the copy system.

"""

# Copyright (C) 2020 The Psycopg Team

from libc.string cimport memcpy
from libc.stdint cimport uint16_t, uint32_t, int32_t
from cpython.bytearray cimport PyByteArray_FromStringAndSize, PyByteArray_Resize
from psycopg3_c._psycopg3.endian cimport htobe16, htobe32

cdef int32_t _binary_null = -1


def format_row_binary(row: Sequence[Any], tx: Transformer) -> bytearray:
    """Convert a row of adapted data to the data to send for binary copy"""
    cdef bytearray out = PyByteArray_FromStringAndSize("", 0)
    cdef Py_ssize_t pos  # position in out where we write
    cdef Py_ssize_t length
    cdef uint16_t rowlen
    cdef uint32_t hlength
    cdef char *buf
    cdef char *target
    cdef int i
    cdef CDumper cdumper

    rowlen = len(row)

    # let's start from a nice chunk
    # (larger than most fixed size, for variable ones, oh well, we will resize)
    PyByteArray_Resize(out, sizeof(rowlen) + 20 * rowlen)

    # Write the number of fields as network-order 16 bits
    buf = PyByteArray_AS_STRING(out)
    (<uint16_t*>buf)[0] = htobe16(rowlen)  # this is aligned
    pos = sizeof(rowlen)

    for i in range(rowlen):
        item = row[i]
        if item is not None:
            dumper = tx.get_dumper(item, FORMAT_BINARY)
            if isinstance(dumper, CDumper):
                # A cdumper can resize if necessary and copy in place
                cdumper = dumper
                length = cdumper.cdump(item, out, pos + sizeof(hlength))
                # Also add the length of the item, before the item
                hlength = htobe32(length)
                target = PyByteArray_AS_STRING(out)  # might have been moved by cdump
                memcpy(target + pos, <void *>&hlength, sizeof(hlength))
            else:
                # A Python dumper, gotta call it and extract its juices
                b = dumper.dump(item)
                _buffer_as_string_and_size(b, &buf, &length)
                target = CDumper.ensure_size(out, pos, length + sizeof(hlength))
                hlength = htobe32(length)
                memcpy(target, <void *>&hlength, sizeof(hlength))
                memcpy(target + sizeof(hlength), buf, length)

            pos += length + sizeof(hlength)

        else:
            target = CDumper.ensure_size(out, pos, sizeof(_binary_null))
            memcpy(target, <void *>&_binary_null, sizeof(_binary_null))
            pos += sizeof(_binary_null)

    # Resize to the final size
    PyByteArray_Resize(out, pos)
    return out
