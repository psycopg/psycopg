"""
psycopg3_c.pq_cython.Conninfo object implementation.
"""

# Copyright (C) 2020 The Psycopg Team


class Conninfo:
    @classmethod
    def get_defaults(cls) -> List[ConninfoOption]:
        cdef impl.PQconninfoOption *opts = impl.PQconndefaults()
        if opts is NULL :
            raise MemoryError("couldn't allocate connection defaults")
        rv = _options_from_array(opts)
        impl.PQconninfoFree(opts)
        return rv

    @classmethod
    def parse(cls, conninfo: bytes) -> List[ConninfoOption]:
        cdef char *errmsg = NULL
        cdef impl.PQconninfoOption *opts = impl.PQconninfoParse(conninfo, &errmsg)
        if opts is NULL:
            if errmsg is NULL:
                raise MemoryError("couldn't allocate on conninfo parse")
            else:
                exc = PQerror(errmsg.decode("utf8", "replace"))
                impl.PQfreemem(errmsg)
                raise exc

        rv = _options_from_array(opts)
        impl.PQconninfoFree(opts)
        return rv

    def __repr__(self):
        return f"<{type(self).__name__} ({self.keyword.decode('ascii')})>"
