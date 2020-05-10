from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_DecodeUTF8


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


cdef object load_unknown_text(const char *data, size_t length, void *context):
    # TODO: codec
    return PyUnicode_DecodeUTF8(data, length, NULL)


cdef object load_unknown_binary(const char *data, size_t length, void *context):
    return PyBytes_FromStringAndSize(data, length)
