"""
psycopg3_c.pq_cython.Conninfo object implementation.
"""

# Copyright (C) 2020 The Psycopg Team

from psycopg3.pq.misc import ConninfoOption


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


cdef _options_from_array(impl.PQconninfoOption *opts):
    rv = []
    cdef int i = 0
    cdef impl.PQconninfoOption* opt
    while 1:
        opt = opts + i
        if opt.keyword is NULL:
            break
        rv.append(
            ConninfoOption(
                keyword=opt.keyword,
                envvar=opt.envvar if opt.envvar is not NULL else None,
                compiled=opt.compiled if opt.compiled is not NULL else None,
                val=opt.val if opt.val is not NULL else None,
                label=opt.label if opt.label is not NULL else None,
                dispchar=opt.dispchar if opt.dispchar is not NULL else None,
                dispsize=opt.dispsize,
            )
        )
        i += 1

    return rv
