"""
Build backend module for psycopg Cython components.

Convert Cython to C if required, compile the C modules adding build from the
libpq and accounting for other platform differences.
"""

# Copyright (C) 2024 The Psycopg Team

import os
import sys
import logging
import subprocess as sp
from distutils.command.build_ext import build_ext

log = logging.getLogger(__name__)


def get_config(what: str) -> str:
    env_map = {
        "includedir": "PSYCOPG_PG_INCLUDEDIR",
        "libdir": "PSYCOPG_PG_LIBDIR",
    }
    if envvar := env_map.get(what):
        if value := os.environ.get(envvar):
            return value

    pg_config = os.environ.get("PSYCOPG_PG_CONFIG", "pg_config")
    try:
        out = sp.run([pg_config, f"--{what}"], stdout=sp.PIPE, check=True)
    except Exception as e:
        msg = (
            f"couldn't determine PostgreSQL {what}: "
            f"set {env_map.get(what, 'PSYCOPG_PG_CONFIG')} "
            f"or make {pg_config!r} available"
        )
        log.error("%s: %s", msg, e)
        raise RuntimeError(msg) from e
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
            from Cython.Build import cythonize

            for ext in self.distribution.ext_modules:
                for i in range(len(ext.sources)):
                    base, fext = os.path.splitext(ext.sources[i])
                    if fext == ".c" and os.path.exists(base + ".pyx"):
                        ext.sources[i] = base + ".pyx"

            self.distribution.ext_modules = cythonize(  # type: ignore[no-untyped-call]
                self.distribution.ext_modules,
                language_level=3,
                compiler_directives={
                    "always_allow_keywords": False,
                    "freethreading_compatible": True,
                },
                annotate=False,  # enable to get an html view of the C module
            )
