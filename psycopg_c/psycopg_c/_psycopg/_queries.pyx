"""
Utility module to manipulate queries
"""

# Copyright (C) 2020 The Psycopg Team

import re
from typing import Any, Dict, List, Mapping, Match, NamedTuple, Optional
from typing import Sequence, Tuple, Union, TYPE_CHECKING
from functools import lru_cache

from . import pq
from . import errors as e
from .sql import Composable
from .abc import Buffer, Query, Params
from ._enums import PyFormat
from ._encodings import conn_encoding
from libc.stdlib cimport malloc, free

cdef extern from "Python.h":
    Py_ssize_t PyUnicode_GET_LENGTH(PyObject *o)
    void *PyUnicode_DATA(PyObject *o)
cdef extern from "string.h":
    void *memset(void *s, int c, size_t n);
    char *strerror(int errnum);
cdef extern from "stdio.h":
    int fprintf(FILE *stream, const char *format, ...);
    int snprintf(char *str, size_t size, const char *format, ...);
    FILE* stdout

cdef enum item_type_enum:
    ITEM_INT = 0,
    ITEM_STR = 1
    
cdef union query_item:
        int data_int
        void* data_bytes

cdef struct query_part:
        void* pre
        unsigned pre_len
        query_item item
        item_type_enum item_type
        unsigned data_len
        char format

cdef struct c_list
cdef struct c_list:
        void* data
        unsigned data_len
        c_list* next

cdef struct c_list_iter:
        c_list* root
        c_list* ptr
        unsigned idx
                   
cdef c_list* iterate_list(c_list_iter* self):
    if not self:
        raise MemoryError("Null-pointer dereference on c_list_iter")
    if not self.ptr:
        raise MemoryError("Null-pointer dereference on c_list_iter.ptr")
    if not self.idx:
        self.idx += 1
        return self.ptr
    if not self.ptr.next:
        return NULL
    self.ptr = self.ptr.next
    self.idx += 1
    return self.ptr

cdef void reset_iterator(c_list_iter* self):
    if not self:
        raise MemoryError("Null-pointer dereference on c_list_iter")
    self.idx = 0
    self.ptr = self.root

cdef c_list_iter* new_iterator(c_list* root):
    cdef c_list_iter* p = <c_list_iter*>malloc(sizeof(c_list_iter))
    if not p:
        raise MemoryError("Dynamic allocation failure")
    p.root = root
    p.ptr = root
    p.idx = 0
    return p

cdef c_list* new_list():
    cdef c_list* p = <c_list*>malloc(sizeof(c_list))
    if not p:
        raise MemoryError("Dynamic allocation failure")
    return p

cdef void list_append(c_list* self, void* data, unsigned data_len, int copy):
    if not self:
        raise MemoryError("Null-pointer dereference on c_list.ptr")
    if not self.data:
        self.data = <void*>malloc(data_len)
        if not self.data:
            raise MemoryError("Dynamic allocation failure")
        memcpy(self.data, data, data_len)
        self.data_len = data_len
        return

    cdef c_list* i = self

    cdef c_list* newitem
    newitem = <c_list*>malloc(sizeof(c_list))
    if not newitem:
        raise MemoryError("Dynamic allocation failure")
    if copy:
        newitem.data = <void*>malloc(data_len)
        if not newitem.data:
            raise MemoryError("Dynamic allocation failure")
        newitem.data_len = data_len
        memcpy(newitem.data, data, data_len)
    else:
        newitem.data = data
        newitem.data_len = data_len
    while 1:
        if not i.next:
            break
        i = i.next
    i.next = newitem

cdef void list_append_PyStr(c_list* root, PyObject* pystr):
    cdef unsigned data_len = <unsigned>PyUnicode_GET_LENGTH(pystr)
    cdef void* data = <void*>PyUnicode_DATA(pystr)
    list_append(root, data, data_len)

class QueryPart(NamedTuple):
    pre: bytes
    item: Union[int, str]
    format: PyFormat

cdef class PostgresQuery():
    """
    Helper to convert a Python query and parameters into Postgres format.
    """

    cdef bytes query
    cdef object params
    cdef Transformer _tx
    cdef tuple types
    cdef list _want_formats
    cdef list formats
    cdef object _encoding
    cdef list _order

    #Old
    cdef list _parts
    #New
    cdef c_list* parts
    
    def __cinit__(self, transformer: "Transformer"):
        self._tx = transformer
        self.parts = new_list()
        
        self.params: Optional[Sequence[Optional[Buffer]]] = None
        # these are tuples so they can be used as keys e.g. in prepared stmts
        self.types: Tuple[int, ...] = ()

        # The format requested by the user and the ones to really pass Postgres
        self._want_formats: Optional[List[PyFormat]] = None
        self.formats: Optional[Sequence[pq.Format]] = None

        self._encoding = conn_encoding(transformer.connection)
        self.query = b""
        self._order: Optional[List[str]] = None

    cpdef convert(self, query: Query, vars: Optional[Params]):
        """
        Set up the query and parameters to convert.

        The results of this function can be obtained accessing the object
        attributes (`query`, `params`, `types`, `formats`).
        """
        if isinstance(query, str):
            bquery = query.encode(self._encoding)
        elif isinstance(query, Composable):
            bquery = query.as_bytes(self._tx)
        else:
            bquery = query

        if vars is not None:
            (
                self.query,
                self._want_formats,
                self._order,
            ) = _query2pg(self.parts, bquery, self._encoding)
        else:
            self.query = bquery
            self._want_formats = self._order = None

        self.dump(vars)

    @classmethod
    def dump(self, vars: Optional[Params]):
        """
        Process a new set of variables on the query processed by `convert()`.

        This method updates `params` and `types`.
        """
        if vars is not None:
            params = _validate_and_reorder_params(self._parts, vars, self._order)
            assert self._want_formats is not None
            self.params = self._tx.dump_sequence(params, self._want_formats)
            self.types = self._tx.types or ()
            self.formats = self._tx.formats
        else:
            self.params = None
            self.types = ()
            self.formats = None

cdef class PostgresClientQuery(PostgresQuery):
    """
    PostgresQuery subclass merging query and arguments client-side.
    """

    cdef bytes template;

    cpdef convert(self, query: Query, vars: Optional[Params]):
        """
        Set up the query and parameters to convert.

        The results of this function can be obtained accessing the object
        attributes (`query`, `params`, `types`, `formats`).
        """
        if isinstance(query, str):
            bquery = query.encode(self._encoding)
        elif isinstance(query, Composable):
            bquery = query.as_bytes(self._tx)
        else:
            bquery = query

        if vars is not None:
            (self.template, self._order, self._parts) = _query2pg_client(
                bquery, self._encoding
            )
        else:
            self.query = bquery
            self._order = None

        self.dump(vars)

    @classmethod
    def dump(self, vars: Optional[Params]):
        """
        Process a new set of variables on the query processed by `convert()`.

        This method updates `params` and `types`.
        """
        if vars is not None:
            params = _validate_and_reorder_params(self._parts, vars, self._order)
            self.params = tuple(
                self._tx.as_literal(p) if p is not None else b"NULL" for p in params)
            self.query = self.template % self.params
        else:
            self.params = None


#@lru_cache()
#Returns Tuple[bytes, List[PyFormat], Optional[List[str]], List[QueryPart]]:
cdef tuple _query2pg(
    c_list* parts, query: bytes, encoding: str
):
    """
    Convert Python query and params into something Postgres understands.

    - Convert Python placeholders (``%s``, ``%(name)s``) into Postgres
      format (``$1``, ``$2``)
    - placeholders can be %s, %t, or %b (auto, text or binary)
    - return ``query`` (bytes), ``formats`` (list of formats) ``order``
      (sequence of names used in the query, in the position they appear)
      ``parts`` (splits of queries and placeholders).
    """

    cdef c_list* order = new_list()
    cdef c_list* chunks = new_list()
    cdef c_list* formats = new_list()

    _split_query(parts, query, encoding)
        
    cdef c_list_iter* i = new_iterator(parts)
    cdef c_list* p = iterate_list(i)

    cdef char cbuf[128] = 0 # Conversion buffer

    cdef query_part* qp
    
    qp = p.data
    if not qp:
        return
    if qp.item_type == ITEM_INT:
        while qp.next:
            list_append(chunks, qp.pre, qp.pre_len, 0)
            cdef int len = snprintf(cbuf, 128, "$%d", (qp.item.data_int + 1))
            if len < 0:
                fprintf(stderr, "%s", strerror(errno))
                return TypeError("snprintf failed")
            list_append(formats, &qp.format, 1, 0)
            p = iterate_list(i)
            if not p.data:
                break
            qp = p.data            
    elif qp.item_type == ITEM_STR
        seen: Dict[str, Tuple[bytes, PyFormat]] = {}
        order = []
        while qp.next:
        #for part in parts[:-1]:
            chunks.append(qp.pre)
            if part.item not in seen:
                ph = b"$%d" % (len(seen) + 1)
                seen[part.item] = (ph, part.format)
                order.append(qp.item.data_bytes)
                chunks.append(ph)
                formats.append(part.format)
            else:
                if seen[part.item][1] != part.format:
                    raise e.ProgrammingError(
                        f"placeholder '{part.item}' cannot have different formats"
                    )
                chunks.append(seen[part.item][0])

    # last part
    chunks.append(parts[-1].pre)

    return b"".join(chunks), formats, order, parts


#Returns Tuple[bytes, Optional[List[str]], List[QueryPart]]
#@lru_cache()
cdef _query2pg_client(
    query: bytes, encoding: str
):
    """
    Convert Python query and params into a template to perform client-side binding
    """
    parts = _split_query(query, encoding, collapse_double_percent=False)
    order: Optional[List[str]] = None
    chunks: List[bytes] = []

    if isinstance(parts[0].item, int):
        for part in parts[:-1]:
            assert isinstance(part.item, int)
            chunks.append(part.pre)
            chunks.append(b"%s")

    elif isinstance(parts[0].item, str):
        seen: Dict[str, Tuple[bytes, PyFormat]] = {}
        order = []
        for part in parts[:-1]:
            assert isinstance(part.item, str)
            chunks.append(part.pre)
            if part.item not in seen:
                ph = b"%s"
                seen[part.item] = (ph, part.format)
                order.append(part.item)
                chunks.append(ph)
            else:
                chunks.append(seen[part.item][0])
                order.append(part.item)

    # last part
    chunks.append(parts[-1].pre)

    return b"".join(chunks), order, parts

#Returns Sequence[Any]
cdef _validate_and_reorder_params(
    parts: List[QueryPart], vars: Params, order: Optional[List[str]]
):
    """
    Verify the compatibility between a query and a set of params.
    """
    # Try concrete types, then abstract types
    t = type(vars)
    if t is list or t is tuple:
        sequence = True
    elif t is dict:
        sequence = False
    elif isinstance(vars, Sequence) and not isinstance(vars, (bytes, str)):
        sequence = True
    elif isinstance(vars, Mapping):
        sequence = False
    else:
        raise TypeError(
            "query parameters should be a sequence or a mapping,"
            f" got {type(vars).__name__}"
        )

    if sequence:
        if len(vars) != len(parts) - 1:
            raise e.ProgrammingError(
                f"the query has {len(parts) - 1} placeholders but"
                f" {len(vars)} parameters were passed"
            )
        if vars and not isinstance(parts[0].item, int):
            raise TypeError("named placeholders require a mapping of parameters")
        return vars  # type: ignore[return-value]

    else:
        if vars and len(parts) > 1 and not isinstance(parts[0][1], str):
            raise TypeError(
                "positional placeholders (%s) require a sequence of parameters"
            )
        try:
            return [vars[item] for item in order or ()]  # type: ignore[call-overload]
        except KeyError:
            raise e.ProgrammingError(
                "query parameter missing:"
                f" {', '.join(sorted(i for i in order or () if i not in vars))}"
            )

_re_placeholder = re.compile(
    rb"""(?x)
        %                       # a literal %
        (?:
            (?:
                \( ([^)]+) \)   # or a name in (braces)
                .               # followed by a format
            )
            |
            (?:.)               # or any char, really
        )
        """
)

#Returns List[QueryPart]
cdef list _split_query(
    c_list* out, 
    query: bytes,
    encoding: str = "ascii",
    collapse_double_percent: bool = True
):
    parts: List[Tuple[bytes, Optional[Match[bytes]]]] = []
    cdef unsigned cur = 0

    # pairs [(fragment, match], with the last match None
    m = None
    for m in _re_placeholder.finditer(query):
        pre = query[cur : m.span(0)[0]]
        parts.append((pre, m))
        cur = m.span(0)[1]
    if m:
        parts.append((query[cur:], None))
    else:
        parts.append((query, None))

    cdef query_part* qp;
    
    # drop the "%%", validate
    cdef unsigned i = 0
    phtype = None
    while i < len(parts):
        pre, m = parts[i]
        if m is None:
            # last part
            qp = <query_part*>malloc(sizeof(query_part))
            if not qp:
                raise MemoryError("Dynamic allocation failure")
            qp.pre = PyUnicode_DATA(pre)
            qp.pre_len = PyUnicode_GET_LENGTH(pre)
            qp.item.data_int = 0
            #data_len only used when item.data_bytes is populated
            qp.data_len = 0
            qp.format = 's'
            list_append(out, qp, sizeof(query_part))
            break

        ph = m.group(0)
        if ph == b"%%":
            # unescape '%%' to '%' if necessary, then merge the parts
            if collapse_double_percent:
                ph = b"%"
            pre1, m1 = parts[i + 1]
            parts[i + 1] = (pre + ph + pre1, m1)
            del parts[i]
            continue

        if ph == b"%(":
            raise e.ProgrammingError(
                "incomplete placeholder:"
                f" '{query[m.span(0)[0]:].split()[0].decode(encoding)}'"
            )
        elif ph == b"% ":
            # explicit messasge for a typical error
            raise e.ProgrammingError(
                "incomplete placeholder: '%'; if you want to use '%' as an"
                " operator you can double it up, i.e. use '%%'"
            )
        elif ph[-1:] not in b"sbt":
            raise e.ProgrammingError(
                "only '%s', '%b', '%t' are allowed as placeholders, got"
                f" '{m.group(0).decode(encoding)}'"
            )

        # Index or name
        item: Union[int, str]
        item = m.group(1).decode(encoding) if m.group(1) else i

        if not phtype:
            phtype = type(item)
        elif phtype is not type(item):
            raise e.ProgrammingError(
                "positional and named placeholders cannot be mixed"
            )

        format = _ph_to_fmt[ph[-1:]]
        rv.append(QueryPart(pre, item, format))
        i += 1

    return rv


_ph_to_fmt = {
    b"s": PyFormat.AUTO,
    b"t": PyFormat.TEXT,
    b"b": PyFormat.BINARY,
}
