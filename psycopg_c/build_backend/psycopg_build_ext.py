"""
Build backend module for psycopg Cython components.

Convert Cython to C if required, compile the C modules adding build from the
libpq and accounting for other platform differences.
"""

# Copyright (C) 2024 The Psycopg Team

import os
import sys
import subprocess as sp

from distutils.command.build_ext import build_ext
from distutils import log


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
        # Add include and lib dir for the libpq.

        # MSVC requires an explicit "libpq"
        libpq = "pq" if sys.platform != "win32" else "libpq"

        for ext in self.distribution.ext_modules:
            ext.libraries.append(libpq)
            ext.include_dirs.append(get_config("includedir"))
            ext.library_dirs.append(get_config("libdir"))

            if sys.platform == "win32":
                # For __imp_htons and others
                ext.libraries.append("ws2_32")

        # In the sdist there are not .pyx, only c, so we don't need Cython.
        # Otherwise Cython is a requirement and it is used to compile pyx to c.
        if os.path.exists("psycopg_c/_psycopg.pyx"):
            from Cython.Build import cythonize  # type: ignore[import-untyped]

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
