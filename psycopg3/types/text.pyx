from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_DecodeUTF8
from psycopg3.pq cimport libpq


cdef object load_text(const char *data, size_t length, void *context):
    # TODO: optimize
    b = PyBytes_FromStringAndSize(data, length)
    if context is not NULL:
        codec = <object>context
        # TODO: check if the refcount is right (but a DECREF here segfaults)
        return codec(b)[0]
    else:
        return b


cdef void *get_context_text(object loader):
    if loader.decode is not None:
        return <void *>loader.decode
    else:
        return NULL


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
