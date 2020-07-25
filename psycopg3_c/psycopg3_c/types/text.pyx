"""
Cython adapters for textual types.
"""

# Copyright (C) 2020 The Psycopg Team

from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Malloc
from cpython.object cimport PyObject
from cpython.unicode cimport PyUnicode_DecodeUTF8
from psycopg3_c cimport libpq


cdef struct TextContext:
    PyObject *pydecoder
    int is_utf8


cdef object load_text(const char *data, size_t length, void *context):
    cdef TextContext *tcontext = <TextContext *>context
    if tcontext.is_utf8:
        return PyUnicode_DecodeUTF8(<char *>data, length, NULL)

    b = PyBytes_FromStringAndSize(data, length)
    decoder = <object>(tcontext.pydecoder)
    if decoder is not None:
        # TODO: check if the refcount is right
        return decoder(b)[0]
    else:
        return b


cdef void *get_context_text(object loader):
    cdef TextContext *rv = <TextContext *>PyMem_Malloc(sizeof(TextContext))
    rv.pydecoder = <PyObject *>loader.decode

    if loader.connection is None or loader.connection.encoding == "UTF8":
        rv.is_utf8 = 1
    else:
        rv.is_utf8 = 0

    return rv


cdef object load_bytea_text(const char *data, size_t length, void *context):
    cdef size_t len_out
    cdef unsigned char *out = libpq.PQunescapeBytea(
        <const unsigned char *>data, &len_out)
    if out is NULL:
        raise MemoryError(
            f"couldn't allocate for unescape_bytea of {len(data)} bytes"
        )

    rv = out[:len_out]
    libpq.PQfreemem(out)
    return rv


cdef object load_bytea_binary(const char *data, size_t length, void *context):
    return data[:length]


cdef void register_text_c_loaders():
    logger.debug("registering optimised text c loaders")
    from psycopg3.types import text
    register_c_loader(text.StringLoader.load, load_text, get_context_text)
    register_c_loader(text.UnknownLoader.load, load_text, get_context_text)
    register_c_loader(text.load_bytea_text, load_bytea_text)
    register_c_loader(text.load_bytea_binary, load_bytea_binary)
