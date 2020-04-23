from cpython.unicode cimport PyUnicode_DecodeUTF8

cdef object load_text_binary(const char *data, size_t length, void *context):
    # TODO: codec
    return PyUnicode_DecodeUTF8(data, length, NULL)


cdef object load_unknown_binary(const char *data, size_t length, void *context):
    # TODO: codec
    return PyUnicode_DecodeUTF8(data, length, NULL)
