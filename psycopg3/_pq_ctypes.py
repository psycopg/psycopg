"""
libpq access using ctypes
"""

# Copyright (C) 2020 The Psycopg Team

import ctypes
import ctypes.util
from ctypes import Structure, POINTER
from ctypes import c_char_p, c_int, c_void_p

pq = ctypes.pydll.LoadLibrary(ctypes.util.find_library("pq"))


# libpq data types


class PGconn_struct(Structure):
    _fields_ = []


class PQconninfoOption_struct(Structure):
    _fields_ = [
        ("keyword", c_char_p),
        ("envvar", c_char_p),
        ("compiled", c_char_p),
        ("val", c_char_p),
        ("label", c_char_p),
        ("dispatcher", c_char_p),
        ("dispsize", c_int),
    ]


PGconn_ptr = POINTER(PGconn_struct)
PQconninfoOption_ptr = POINTER(PQconninfoOption_struct)


# Function definitions as explained in PostgreSQL 12 documentation

# 33.1. Database Connection Control Functions

# PQconnectdbParams: doesn't seem useful, won't wrap for now

PQconnectdb = pq.PQconnectdb
PQconnectdb.argtypes = [c_char_p]
PQconnectdb.restype = PGconn_ptr

# PQsetdbLogin: not useful
# PQsetdb: not useful

# PQconnectStartParams: not useful

PQconnectStart = pq.PQconnectStart
PQconnectStart.argtypes = [c_char_p]
PQconnectStart.restype = PGconn_ptr

PQconnectPoll = pq.PQconnectPoll
PQconnectPoll.argtypes = [PGconn_ptr]
PQconnectPoll.restype = c_int

PQconndefaults = pq.PQconndefaults
PQconndefaults.argtypes = []
PQconndefaults.restype = PQconninfoOption_ptr

PQconninfoFree = pq.PQconninfoFree
PQconninfoFree.argtypes = [PQconninfoOption_ptr]
PQconninfoFree.restype = None

PQconninfo = pq.PQconninfo
PQconninfo.argtypes = [PGconn_ptr]
PQconninfo.restype = PQconninfoOption_ptr

PQconninfoParse = pq.PQconninfoParse
PQconninfoParse.argtypes = [c_char_p, POINTER(c_char_p)]
PQconninfoParse.restype = PQconninfoOption_ptr

PQfinish = pq.PQfinish
PQfinish.argtypes = [PGconn_ptr]
PQfinish.restype = None

PQreset = pq.PQreset
PQreset.argtypes = [PGconn_ptr]
PQreset.restype = None

PQresetStart = pq.PQresetStart
PQresetStart.argtypes = [PGconn_ptr]
PQresetStart.restype = c_int

PQresetPoll = pq.PQresetPoll
PQresetPoll.argtypes = [PGconn_ptr]
PQresetPoll.restype = c_int

PQping = pq.PQping
PQping.argtypes = [c_char_p]
PQping.restype = c_int


# 33.2. Connection Status Functions

PQstatus = pq.PQstatus
PQstatus.argtypes = [PGconn_ptr]
PQstatus.restype = c_int

PQerrorMessage = pq.PQerrorMessage
PQerrorMessage.argtypes = [PGconn_ptr]
PQerrorMessage.restype = c_char_p

PQsocket = pq.PQsocket
PQsocket.argtypes = [PGconn_ptr]
PQsocket.restype = c_int


# 33.11. Miscellaneous Functions

PQfreemem = pq.PQfreemem
PQfreemem.argtypes = [c_void_p]
PQfreemem.restype = None
