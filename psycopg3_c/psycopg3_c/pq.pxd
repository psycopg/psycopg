from posix.fcntl cimport pid_t
from psycopg3_c.pq cimport libpq

ctypedef char *(*conn_bytes_f) (const libpq.PGconn *)
ctypedef int(*conn_int_f) (const libpq.PGconn *)


cdef class PGconn:
    cdef libpq.PGconn* pgconn_ptr
    cdef object __weakref__
    cdef public object notice_handler
    cdef public object notify_handler
    cdef pid_t _procpid

    @staticmethod
    cdef PGconn _from_ptr(libpq.PGconn *ptr)

    cpdef object notifies(self)


cdef class PGresult:
    cdef libpq.PGresult* pgresult_ptr

    @staticmethod
    cdef PGresult _from_ptr(libpq.PGresult *ptr)


cdef class PGcancel:
    cdef libpq.PGcancel* pgcancel_ptr

    @staticmethod
    cdef PGcancel _from_ptr(libpq.PGcancel *ptr)


cdef class Escaping:
    cdef PGconn conn


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
