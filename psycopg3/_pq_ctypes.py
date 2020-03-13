"""
libpq access using ctypes
"""

# Copyright (C) 2020 The Psycopg Team

import ctypes
import ctypes.util
from ctypes import Structure, POINTER
from ctypes import c_char_p, c_int

pq = ctypes.pydll.LoadLibrary(ctypes.util.find_library("pq"))


class PGconn(Structure):
    _fields_ = []


PGconn_ptr = POINTER(PGconn)

PQconnectdb = pq.PQconnectdb
PQconnectdb.argtypes = [c_char_p]
PQconnectdb.restype = PGconn_ptr

PQstatus = pq.PQstatus
PQstatus.argtypes = [PGconn_ptr]
PQstatus.restype = c_int

PQerrorMessage = pq.PQerrorMessage
PQerrorMessage.argtypes = [PGconn_ptr]
PQerrorMessage.restype = c_char_p
