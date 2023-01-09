from psycopg_c cimport pq

cdef class Transformer:
    cdef readonly object connection
    cdef readonly object adapters
    cdef readonly object types
    cdef readonly object formats
    cdef str _encoding
    cdef int _none_oid
    # mapping class -> Dumper instance (auto, text, binary)
    cdef dict _auto_dumpers
    cdef dict _text_dumpers
    cdef dict _binary_dumpers
    # mapping oid -> Loader instance (text, binary)
    cdef dict _text_loaders
    cdef dict _binary_loaders
    # mapping oid -> Dumper instance (text, binary)
    cdef dict _oid_text_dumpers
    cdef dict _oid_binary_dumpers
    cdef pq.PGresult _pgresult
    cdef int _nfields, _ntuples
    cdef list _row_dumpers
    cdef list _row_loaders
    cdef dict _oid_types

