# Include pid_t but Windows doesn't have it
# Don't use "IF" so that the generated C is portable and can be included
# in the sdist.
cdef extern from * nogil:
    """
#if defined(_WIN32) || defined(WIN32) || defined(MS_WINDOWS)
    typedef signed pid_t;
#else
    #include <fcntl.h>
#endif
    """
    ctypedef signed pid_t

from psycopg_c.pq cimport libpq

ctypedef char *(*conn_bytes_f) (const libpq.PGconn *)
ctypedef int(*conn_int_f) (const libpq.PGconn *)


cdef class PGconn:
    cdef libpq.PGconn* _pgconn_ptr
    cdef object __weakref__
    cdef public object notice_handler
    cdef public object notify_handler
    cdef pid_t _procpid

    @staticmethod
    cdef PGconn _from_ptr(libpq.PGconn *ptr)

    cpdef object notifies(self)


cdef class PGresult:
    cdef libpq.PGresult* _pgresult_ptr

    @staticmethod
    cdef PGresult _from_ptr(libpq.PGresult *ptr)


cdef class PGcancel:
    cdef libpq.PGcancel* pgcancel_ptr

    @staticmethod
    cdef PGcancel _from_ptr(libpq.PGcancel *ptr)


cdef class Escaping:
    cdef PGconn conn

    cpdef escape_literal(self, data)
    cpdef escape_identifier(self, data)
    cpdef escape_string(self, data)
    cpdef escape_bytea(self, data)
    cpdef unescape_bytea(self, const unsigned char *data)


cdef class PQBuffer:
    cdef unsigned char *buf
    cdef Py_ssize_t len

    @staticmethod
    cdef PQBuffer _from_buffer(unsigned char *buf, Py_ssize_t length)


cdef class ViewBuffer:
    cdef unsigned char *buf
    cdef Py_ssize_t len
    cdef object obj

    @staticmethod
    cdef ViewBuffer _from_buffer(
        object obj, unsigned char *buf, Py_ssize_t length)


cdef int _buffer_as_string_and_size(
    data: "Buffer", char **ptr, Py_ssize_t *length
) except -1
