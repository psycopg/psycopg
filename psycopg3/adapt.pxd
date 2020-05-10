from cpython.object cimport PyObject

ctypedef object (*cloader_func)(const char *data, size_t length, void *context)
ctypedef void * (*get_context_func)(object conn)


cdef struct RowLoader:
    PyObject *pyloader  # borrowed
    cloader_func cloader
    void *context
