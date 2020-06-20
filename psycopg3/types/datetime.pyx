"""
Cython adapters for datetime types.
"""

# Copyright (C) 2020 The Psycopg Team


from libc.stdint cimport *

from psycopg3.types.text cimport load_text, get_context_text


cdef void register_text_c_loaders():
    logger.debug("registering optimised datetime c loaders")
    from psycopg3.types import datetime
    register_c_loader(datetime.DateTimeLoader.load, load_text, get_context_text)
    register_c_loader(datetime.load_interval, load_text, get_context_text)
