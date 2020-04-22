cimport cpython

cdef object load_text_binary(const char *data, size_t length, void *context):
    # TODO: codec
    return cpython.PyUnicode_DecodeUTF8(data, length, NULL)


cdef object load_unknown_binary(const char *data, size_t length, void *context):
    # TODO: codec
    return cpython.PyUnicode_DecodeUTF8(data, length, NULL)
