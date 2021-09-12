#!/usr/bin/env python3
"""
PostgreSQL database adapter for Python - optimisation package
"""

# Copyright (C) 2020-2021 The Psycopg Team

import os
import re
import sys
import subprocess as sp

from setuptools import setup, Extension
from distutils.command.build_ext import build_ext
from distutils import log

# Move to the directory of setup.py: executing this file from another location
# (e.g. from the project root) will fail
here = os.path.abspath(os.path.dirname(__file__))
if os.path.abspath(os.getcwd()) != here:
    os.chdir(here)

with open("psycopg_c/version.py") as f:
    data = f.read()
    m = re.search(r"""(?m)^__version__\s*=\s*['"]([^'"]+)['"]""", data)
    if m is None:
        raise Exception(f"cannot find version in {f.name}")
    version = m.group(1)


def get_config(what: str) -> str:
    pg_config = "pg_config"
    try:
        out = sp.run([pg_config, f"--{what}"], stdout=sp.PIPE, check=True)
    except Exception as e:
        log.error(f"couldn't run {pg_config!r} --{what}: %s", e)
        raise
    else:
        return out.stdout.strip().decode()


class psycopg_build_ext(build_ext):
    def finalize_options(self) -> None:
        self._setup_ext_build()
        super().finalize_options()

    def _setup_ext_build(self) -> None:
        cythonize = None

        # In the sdist there are not .pyx, only c, so we don't need Cython
        # Otherwise Cython is a requirement and is be used to compile pyx to c
        if os.path.exists("psycopg_c/_psycopg.pyx"):
            from Cython.Build import cythonize

        # Add include and lib dir for the libpq.
        includedir = get_config("includedir")
        libdir = get_config("libdir")
        for ext in self.distribution.ext_modules:
            ext.include_dirs.append(includedir)
            ext.library_dirs.append(libdir)

            if sys.platform == "win32":
                # For __imp_htons and others
                ext.libraries.append("ws2_32")

        if cythonize is not None:
            for ext in self.distribution.ext_modules:
                for i in range(len(ext.sources)):
                    base, fext = os.path.splitext(ext.sources[i])
                    if fext == ".c" and os.path.exists(base + ".pyx"):
                        ext.sources[i] = base + ".pyx"

            self.distribution.ext_modules = cythonize(
                self.distribution.ext_modules,
                language_level=3,
                compiler_directives={
                    "always_allow_keywords": False,
                },
                annotate=False,  # enable to get an html view of the C module
            )
        else:
            self.distribution.ext_modules = [pgext, pqext]


# MSVC requires an explicit "libpq"
libpq = "pq" if sys.platform != "win32" else "libpq"

# Some details missing, to be finished by psycopg_build_ext.finalize_options
pgext = Extension(
    "psycopg_c._psycopg",
    [
        "psycopg_c/_psycopg.c",
        "psycopg_c/types/numutils.c",
    ],
    libraries=[libpq],
    include_dirs=[],
)

pqext = Extension(
    "psycopg_c.pq",
    ["psycopg_c/pq.c"],
    libraries=[libpq],
    include_dirs=[],
)

setup(
    version=version,
    ext_modules=[pgext, pqext],
    cmdclass={"build_ext": psycopg_build_ext},
)
