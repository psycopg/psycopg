from posix.fcntl cimport pid_t
from psycopg3_c cimport libpq as impl

ctypedef char *(*conn_bytes_f) (const impl.PGconn *)
ctypedef int(*conn_int_f) (const impl.PGconn *)


cdef class PGconn:
    cdef impl.PGconn* pgconn_ptr
    cdef object __weakref__
    cdef public object notice_handler
    cdef public object notify_handler
    cdef pid_t _procpid

    @staticmethod
    cdef PGconn _from_ptr(impl.PGconn *ptr)

    cdef int _ensure_pgconn(self) except 0
    cdef char *_call_bytes(self, conn_bytes_f func) except NULL
    cdef int _call_int(self, conn_int_f func) except -1


cdef class PGresult:
    cdef impl.PGresult* pgresult_ptr

    @staticmethod
    cdef PGresult _from_ptr(impl.PGresult *ptr)


cdef class PGcancel:
    cdef impl.PGcancel* pgcancel_ptr

    @staticmethod
    cdef PGcancel _from_ptr(impl.PGcancel *ptr)


cdef class Escaping:
    cdef PGconn conn
