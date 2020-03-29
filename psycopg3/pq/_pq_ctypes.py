"""
libpq access using ctypes
"""

# Copyright (C) 2020 The Psycopg Team

import ctypes
import ctypes.util
from ctypes import Structure, POINTER
from ctypes import c_char, c_char_p, c_int, c_uint, c_void_p
from typing import List, Tuple

from psycopg3.exceptions import NotSupportedError

libname = ctypes.util.find_library("pq")
if libname is None:
    raise ImportError("libpq library not found")

pq = ctypes.pydll.LoadLibrary(libname)

# Get the libpq version to define what functions are available.

PQlibVersion = pq.PQlibVersion
PQlibVersion.argtypes = []
PQlibVersion.restype = c_int

libpq_version = PQlibVersion()


# libpq data types


Oid = c_uint


class PGconn_struct(Structure):
    _fields_: List[Tuple[str, type]] = []


class PGresult_struct(Structure):
    _fields_: List[Tuple[str, type]] = []


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
PGresult_ptr = POINTER(PGresult_struct)
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

PQdb = pq.PQdb
PQdb.argtypes = [PGconn_ptr]
PQdb.restype = c_char_p

PQuser = pq.PQuser
PQuser.argtypes = [PGconn_ptr]
PQuser.restype = c_char_p

PQpass = pq.PQpass
PQpass.argtypes = [PGconn_ptr]
PQpass.restype = c_char_p

PQhost = pq.PQhost
PQhost.argtypes = [PGconn_ptr]
PQhost.restype = c_char_p

_PQhostaddr = None

if libpq_version >= 120000:
    _PQhostaddr = pq.PQhostaddr
    _PQhostaddr.argtypes = [PGconn_ptr]
    _PQhostaddr.restype = c_char_p


def PQhostaddr(pgconn):
    if _PQhostaddr is not None:
        return _PQhostaddr(pgconn)
    else:
        raise NotSupportedError(
            f"PQhostaddr requires libpq from PostgreSQL 12,"
            f" {libpq_version} available instead"
        )


PQport = pq.PQport
PQport.argtypes = [PGconn_ptr]
PQport.restype = c_char_p

PQtty = pq.PQtty
PQtty.argtypes = [PGconn_ptr]
PQtty.restype = c_char_p

PQoptions = pq.PQoptions
PQoptions.argtypes = [PGconn_ptr]
PQoptions.restype = c_char_p

PQstatus = pq.PQstatus
PQstatus.argtypes = [PGconn_ptr]
PQstatus.restype = c_int

PQtransactionStatus = pq.PQtransactionStatus
PQtransactionStatus.argtypes = [PGconn_ptr]
PQtransactionStatus.restype = c_int

PQparameterStatus = pq.PQparameterStatus
PQparameterStatus.argtypes = [PGconn_ptr, c_char_p]
PQparameterStatus.restype = c_char_p

PQprotocolVersion = pq.PQprotocolVersion
PQprotocolVersion.argtypes = [PGconn_ptr]
PQprotocolVersion.restype = c_int

PQserverVersion = pq.PQserverVersion
PQserverVersion.argtypes = [PGconn_ptr]
PQserverVersion.restype = c_int

PQerrorMessage = pq.PQerrorMessage
PQerrorMessage.argtypes = [PGconn_ptr]
PQerrorMessage.restype = c_char_p

PQsocket = pq.PQsocket
PQsocket.argtypes = [PGconn_ptr]
PQsocket.restype = c_int

PQbackendPID = pq.PQbackendPID
PQbackendPID.argtypes = [PGconn_ptr]
PQbackendPID.restype = c_int

PQconnectionNeedsPassword = pq.PQconnectionNeedsPassword
PQconnectionNeedsPassword.argtypes = [PGconn_ptr]
PQconnectionNeedsPassword.restype = c_int

PQconnectionUsedPassword = pq.PQconnectionUsedPassword
PQconnectionUsedPassword.argtypes = [PGconn_ptr]
PQconnectionUsedPassword.restype = c_int

PQsslInUse = pq.PQsslInUse
PQsslInUse.argtypes = [PGconn_ptr]
PQsslInUse.restype = c_int

# TODO: PQsslAttribute, PQsslAttributeNames, PQsslStruct, PQgetssl


# 33.3. Command Execution Functions

PQexec = pq.PQexec
PQexec.argtypes = [PGconn_ptr, c_char_p]
PQexec.restype = PGresult_ptr

PQexecParams = pq.PQexecParams
PQexecParams.argtypes = [
    PGconn_ptr,
    c_char_p,
    c_int,
    POINTER(Oid),
    POINTER(c_char_p),
    POINTER(c_int),
    POINTER(c_int),
    c_int,
]
PQexecParams.restype = PGresult_ptr

PQprepare = pq.PQprepare
PQprepare.argtypes = [PGconn_ptr, c_char_p, c_char_p, c_int, POINTER(Oid)]
PQprepare.restype = PGresult_ptr

PQexecPrepared = pq.PQexecPrepared
PQexecPrepared.argtypes = [
    PGconn_ptr,
    c_char_p,
    c_int,
    POINTER(c_char_p),
    POINTER(c_int),
    POINTER(c_int),
    c_int,
]
PQexecPrepared.restype = PGresult_ptr

PQdescribePrepared = pq.PQdescribePrepared
PQdescribePrepared.argtypes = [PGconn_ptr, c_char_p]
PQdescribePrepared.restype = PGresult_ptr

PQdescribePortal = pq.PQdescribePortal
PQdescribePortal.argtypes = [PGconn_ptr, c_char_p]
PQdescribePortal.restype = PGresult_ptr

PQresultStatus = pq.PQresultStatus
PQresultStatus.argtypes = [PGresult_ptr]
PQresultStatus.restype = c_int

# PQresStatus: not needed, we have pretty enums

PQresultErrorMessage = pq.PQresultErrorMessage
PQresultErrorMessage.argtypes = [PGresult_ptr]
PQresultErrorMessage.restype = c_char_p

# TODO: PQresultVerboseErrorMessage

PQresultErrorField = pq.PQresultErrorField
PQresultErrorField.argtypes = [PGresult_ptr, c_int]
PQresultErrorField.restype = c_char_p

PQclear = pq.PQclear
PQclear.argtypes = [PGresult_ptr]
PQclear.restype = None


# 33.3.2. Retrieving Query Result Information

PQntuples = pq.PQntuples
PQntuples.argtypes = [PGresult_ptr]
PQntuples.restype = c_int

PQnfields = pq.PQnfields
PQnfields.argtypes = [PGresult_ptr]
PQnfields.restype = c_int

PQfname = pq.PQfname
PQfname.argtypes = [PGresult_ptr, c_int]
PQfname.restype = c_char_p

# PQfnumber: useless and hard to use

PQftable = pq.PQftable
PQftable.argtypes = [PGresult_ptr, c_int]
PQftable.restype = Oid

PQftablecol = pq.PQftablecol
PQftablecol.argtypes = [PGresult_ptr, c_int]
PQftablecol.restype = c_int

PQfformat = pq.PQfformat
PQfformat.argtypes = [PGresult_ptr, c_int]
PQfformat.restype = c_int

PQftype = pq.PQftype
PQftype.argtypes = [PGresult_ptr, c_int]
PQftype.restype = Oid

PQfmod = pq.PQfmod
PQfmod.argtypes = [PGresult_ptr, c_int]
PQfmod.restype = c_int

PQfsize = pq.PQfsize
PQfsize.argtypes = [PGresult_ptr, c_int]
PQfsize.restype = c_int

PQbinaryTuples = pq.PQbinaryTuples
PQbinaryTuples.argtypes = [PGresult_ptr]
PQbinaryTuples.restype = c_int

PQgetvalue = pq.PQgetvalue
PQgetvalue.argtypes = [PGresult_ptr, c_int, c_int]
PQgetvalue.restype = POINTER(c_char)  # not a null-terminated string

PQgetisnull = pq.PQgetisnull
PQgetisnull.argtypes = [PGresult_ptr, c_int, c_int]
PQgetisnull.restype = c_int

PQgetlength = pq.PQgetlength
PQgetlength.argtypes = [PGresult_ptr, c_int, c_int]
PQgetlength.restype = c_int

PQnparams = pq.PQnparams
PQnparams.argtypes = [PGresult_ptr]
PQnparams.restype = c_int

PQparamtype = pq.PQparamtype
PQparamtype.argtypes = [PGresult_ptr, c_int]
PQparamtype.restype = Oid

# PQprint: pretty useless

# 33.3.3. Retrieving Other Result Information

PQcmdStatus = pq.PQcmdStatus
PQcmdStatus.argtypes = [PGresult_ptr]
PQcmdStatus.restype = c_char_p

PQcmdTuples = pq.PQcmdTuples
PQcmdTuples.argtypes = [PGresult_ptr]
PQcmdTuples.restype = c_char_p

PQoidValue = pq.PQoidValue
PQoidValue.argtypes = [PGresult_ptr]
PQoidValue.restype = Oid


# 33.4. Asynchronous Command Processing

PQsendQuery = pq.PQsendQuery
PQsendQuery.argtypes = [PGconn_ptr, c_char_p]
PQsendQuery.restype = c_int

PQsendQueryParams = pq.PQsendQueryParams
PQsendQueryParams.argtypes = [
    PGconn_ptr,
    c_char_p,
    c_int,
    POINTER(Oid),
    POINTER(c_char_p),
    POINTER(c_int),
    POINTER(c_int),
    c_int,
]
PQsendQueryParams.restype = c_int

# TODO: PQsendPrepare PQsendQueryPrepared
#       PQsendDescribePrepared PQsendDescribePortal

PQgetResult = pq.PQgetResult
PQgetResult.argtypes = [PGconn_ptr]
PQgetResult.restype = PGresult_ptr

PQconsumeInput = pq.PQconsumeInput
PQconsumeInput.argtypes = [PGconn_ptr]
PQconsumeInput.restype = c_int

PQisBusy = pq.PQisBusy
PQisBusy.argtypes = [PGconn_ptr]
PQisBusy.restype = c_int

PQsetnonblocking = pq.PQsetnonblocking
PQsetnonblocking.argtypes = [PGconn_ptr, c_int]
PQsetnonblocking.restype = c_int

PQisnonblocking = pq.PQisnonblocking
PQisnonblocking.argtypes = [PGconn_ptr]
PQisnonblocking.restype = c_int

PQflush = pq.PQflush
PQflush.argtypes = [PGconn_ptr]
PQflush.restype == c_int


# 33.11. Miscellaneous Functions

PQfreemem = pq.PQfreemem
PQfreemem.argtypes = [c_void_p]
PQfreemem.restype = None

PQmakeEmptyPGresult = pq.PQmakeEmptyPGresult
PQmakeEmptyPGresult.argtypes = [PGconn_ptr, c_int]
PQmakeEmptyPGresult.restype = PGresult_ptr
