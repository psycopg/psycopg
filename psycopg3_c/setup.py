#!/usr/bin/env python3
"""
PostgreSQL database adapter for Python - optimisation package
"""

# Copyright (C) 2020-2021 The Psycopg Team

import os
import re
import subprocess as sp

from setuptools import setup, Extension
from distutils.command.build_ext import build_ext
from distutils import log

# Move to the directory of setup.py: executing this file from another location
# (e.g. from the project root) will fail
here = os.path.abspath(os.path.dirname(__file__))
if os.path.abspath(os.getcwd()) != here:
    os.chdir(here)

with open("psycopg3_c/version.py") as f:
    data = f.read()
    m = re.search(r"""(?m)^__version__\s*=\s*['"]([^'"]+)['"]""", data)
    if m is None:
        raise Exception(f"cannot find version in {f.name}")
    version = m.group(1)


class psycopg3_build_ext(build_ext):
    def finalize_options(self) -> None:
        self._setup_ext_build()
        super().finalize_options()

    def _setup_ext_build(self) -> None:
        cythonize = None

        # In the sdist there are not .pyx, only c, so we don't need Cython
        # Otherwise Cython is a requirement and is be used to compile pyx to c
        if os.path.exists("psycopg3_c/_psycopg3.pyx"):
            from Cython.Build import cythonize

        # Add the include dir for the libpq.
        try:
            out = sp.run(
                ["pg_config", "--includedir"], stdout=sp.PIPE, check=True
            )
        except Exception as e:
            log.error("cannot build C module: %s", e)
            raise

        includedir = out.stdout.strip().decode("utf8")
        for ext in self.distribution.ext_modules:
            ext.include_dirs.append(includedir)

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


# Some details missing, to be finished by psycopg3_build_ext.finalize_options
pgext = Extension(
    "psycopg3_c._psycopg3",
    [
        "psycopg3_c/_psycopg3.c",
        "psycopg3_c/types/numutils.c",
    ],
    libraries=["pq"],
    include_dirs=[],
)

pqext = Extension(
    "psycopg3_c.pq",
    ["psycopg3_c/pq.c"],
    libraries=["pq"],
    include_dirs=[],
)

setup(
    version=version,
    ext_modules=[pgext, pqext],
    cmdclass={"build_ext": psycopg3_build_ext},
    # For some reason pacakge_data doesn't work in setup.cfg
    package_data={
        "psycopg3_c": ["py.typed"],
        # NOTE: do not include .pyx files: they shoudn't be in the sdist
        # package, so that build is only performed from the .c files (which are
        # distributed instead).
        "": ["*.pxd", "*.pyi"],
    },
)
