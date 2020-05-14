from psycopg3.adapt cimport cloader_func, get_context_func, RowLoader


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
