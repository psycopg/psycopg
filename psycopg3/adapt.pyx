from psycopg3.adapt cimport cloader_func

from psycopg3.types cimport numeric


import logging
logger = logging.getLogger("psycopg3.adapt")


cdef class CLoader:
    cdef object pyloader
    cdef cloader_func cloader


cloaders = {}

cdef void register_c_loader(object pyloader, cloader_func cloader):
    """
    Map a python loader to an optimised C version.

    Whenever the python loader would be used the C version may be chosen in
    preference.
    """
    cdef CLoader cl = CLoader()
    cl.pyloader = pyloader
    cl.cloader = cloader
    cloaders[pyloader] = cl


def register_builtin_c_loaders():
    if cloaders:
        logger.warning("c loaders already registered")
        return

    logger.debug("registering optimised c loaders")
    register_numeric_c_loaders()


cdef void register_numeric_c_loaders():
    logger.debug("registering optimised numeric c loaders")
    from psycopg3.types import numeric
    register_c_loader(numeric.load_int, load_int_text)
    register_c_loader(numeric.load_int2_binary, load_int2_binary)
    register_c_loader(numeric.load_int4_binary, load_int4_binary)
    register_c_loader(numeric.load_int8_binary, load_int8_binary)
    register_c_loader(numeric.load_oid_binary, load_oid_binary)
