"""
PQbuffer object implementation.
"""

# Copyright (C) 2020 The Psycopg Team


cdef class PQBuffer:
    """
    Wrap a chunk of memory allocated by the libpq and expose it as memoryview.
    """
    @staticmethod
    cdef PQBuffer _from_buffer(unsigned char *buf, Py_ssize_t len):
        cdef PQBuffer rv = PQBuffer.__new__(PQBuffer)
        rv.buf = buf
        rv.len = len
        return rv

    def __cinit__(self):
        self.buf = NULL
        self.len = 0

    def __dealloc__(self):
        if self.buf:
            impl.PQfreemem(self.buf)

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

