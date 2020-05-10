cdef object load_text(const char *data, size_t length, void *context)
cdef void *get_context_text(object loader)
cdef object load_unknown_text(const char *data, size_t length, void *context)
cdef object load_unknown_binary(const char *data, size_t length, void *context)
