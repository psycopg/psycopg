from psycopg3.adapt cimport cloader_func, get_context_func, RowLoader

from psycopg3.types cimport numeric, text


import logging
logger = logging.getLogger("psycopg3.adapt")


cdef class CLoader:
    cdef object pyloader
    cdef cloader_func cloader
    cdef get_context_func get_context


cloaders = {}

cdef void register_c_loader(
    object pyloader,
    cloader_func cloader,
    get_context_func get_context = NULL
):
    """
    Map a python loader to an optimised C version.

    Whenever the python loader would be used the C version may be chosen in
    preference.
    """
    cdef CLoader cl = CLoader()
    cl.pyloader = pyloader
    cl.cloader = cloader
    cl.get_context = get_context
    cloaders[pyloader] = cl


cdef void fill_row_loader(RowLoader *loader, object pyloader):
    loader.pyloader = <PyObject *>pyloader

    cdef CLoader cloader
    cloader = cloaders.get(pyloader)
    if cloader is not None:
        loader.cloader = cloader.cloader
    else:
        cloader = cloaders.get(getattr(pyloader, '__func__', None))
        if cloader is not None and cloader.get_context is not NULL:
            loader.cloader = cloader.cloader
            loader.context = cloader.get_context(pyloader.__self__)


def register_builtin_c_loaders():
    if cloaders:
        logger.warning("c loaders already registered")
        return

    logger.debug("registering optimised c loaders")
    register_numeric_c_loaders()
    register_text_c_loaders()


cdef void register_numeric_c_loaders():
    logger.debug("registering optimised numeric c loaders")
    from psycopg3.types import numeric
    register_c_loader(numeric.load_int, load_int_text)
    register_c_loader(numeric.load_int2_binary, load_int2_binary)
    register_c_loader(numeric.load_int4_binary, load_int4_binary)
    register_c_loader(numeric.load_int8_binary, load_int8_binary)
    register_c_loader(numeric.load_oid_binary, load_oid_binary)
    register_c_loader(numeric.load_bool_binary, load_bool_binary)


cdef void register_text_c_loaders():
    logger.debug("registering optimised text c loaders")
    from psycopg3 import adapt
    from psycopg3.types import text
    register_c_loader(text.StringLoader.load, load_text, get_context_text)
    register_c_loader(text.UnknownLoader.load, load_text, get_context_text)
    register_c_loader(text.load_bytea_text, load_bytea_text)
    register_c_loader(text.load_bytea_binary, load_bytea_binary)
