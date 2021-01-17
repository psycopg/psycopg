"""
PQbuffer object implementation.
"""

# Copyright (C) 2020-2021 The Psycopg Team

cimport cython
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.buffer cimport PyObject_CheckBuffer, PyBUF_SIMPLE
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release


@cython.freelist(32)
cdef class PQBuffer:
    """
    Wrap a chunk of memory allocated by the libpq and expose it as memoryview.
    """
    @staticmethod
    cdef PQBuffer _from_buffer(unsigned char *buf, Py_ssize_t length):
        cdef PQBuffer rv = PQBuffer.__new__(PQBuffer)
        rv.buf = buf
        rv.len = length
        return rv

    def __cinit__(self):
        self.buf = NULL
        self.len = 0

    def __dealloc__(self):
        if self.buf:
            libpq.PQfreemem(self.buf)

    def __repr__(self):
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
            f"({bytes(self)})"
        )

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        buffer.buf = self.buf
        buffer.obj = self
        buffer.len = self.len
        buffer.itemsize = sizeof(unsigned char)
        buffer.readonly = 1
        buffer.ndim = 1
        buffer.format = NULL  # unsigned char
        buffer.shape = &self.len
        buffer.strides = NULL
        buffer.suboffsets = NULL
        buffer.internal = NULL

    def __releasebuffer__(self, Py_buffer *buffer):
        pass


@cython.freelist(32)
cdef class ViewBuffer:
    """
    Wrap a chunk of memory owned by a different object.
    """
    @staticmethod
    cdef ViewBuffer _from_buffer(
        object obj, unsigned char *buf, Py_ssize_t length
    ):
        cdef ViewBuffer rv = ViewBuffer.__new__(ViewBuffer)
        rv.obj = obj
        rv.buf = buf
        rv.len = length
        return rv

    def __cinit__(self):
        self.buf = NULL
        self.len = 0

    def __repr__(self):
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
            f"({bytes(self)})"
        )

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        buffer.buf = self.buf
        buffer.obj = self
        buffer.len = self.len
        buffer.itemsize = sizeof(unsigned char)
        buffer.readonly = 1
        buffer.ndim = 1
        buffer.format = NULL  # unsigned char
        buffer.shape = &self.len
        buffer.strides = NULL
        buffer.suboffsets = NULL
        buffer.internal = NULL

    def __releasebuffer__(self, Py_buffer *buffer):
        pass


cdef int _buffer_as_string_and_size(
    data: "Buffer", char **ptr, Py_ssize_t *length
) except -1:
    cdef Py_buffer buf

    if isinstance(data, bytes):
        PyBytes_AsStringAndSize(data, ptr, length)
    elif PyObject_CheckBuffer(data):
        PyObject_GetBuffer(data, &buf, PyBUF_SIMPLE)
        ptr[0] = <char *>buf.buf
        length[0] = buf.len
        PyBuffer_Release(&buf)
    else:
        raise TypeError(f"bytes or buffer expected, got {type(data)}")
