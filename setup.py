#!/usr/bin/env python3
"""
psycopg3 -- PostgreSQL database adapter for Python
"""

# Copyright (C) 2020 The Psycopg Team


import re
import os
import subprocess as sp
from setuptools import setup, find_packages, Extension
from distutils.command.build_ext import build_ext
from distutils import log

try:
    from Cython.Build import cythonize
except ImportError:
    cythonize = None

# Grab the version without importing the module
# or we will get import errors on install if prerequisites are still missing
fn = os.path.join(os.path.dirname(__file__), "psycopg3/consts.py")
with open(fn) as f:
    m = re.search(r"""(?mi)^VERSION\s*=\s*["']+([^'"]+)["']+""", f.read())
if m:
    version = m.group(1)
else:
    raise ValueError("cannot find VERSION in the consts module")

# Read the description from the README
with open("README.rst") as f:
    readme = f.read()

# TODO: classifiers
classifiers = """
Intended Audience :: Developers
Programming Language :: Python :: 3
License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)
Topic :: Database
Topic :: Database :: Front-Ends
Topic :: Software Development
Topic :: Software Development :: Libraries :: Python Modules
"""


class our_build_ext(build_ext):
    def finalize_options(self) -> None:
        self._setup_ext_build()
        super().finalize_options()

    def run(self) -> None:
        super().run()

    def _setup_ext_build(self) -> None:
        try:
            from Cython.Build import cythonize
        except ImportError:
            log.warn("Cython is not available: the C module will not be built")
            return

        try:
            out = sp.run(
                ["pg_config", f"--includedir"], stdout=sp.PIPE, check=True
            )
        except Exception as e:
            log.warn("cannot build C module: %s", e)
            return

        includedir = out.stdout.strip().decode("utf8")

        pgext = Extension(
            "psycopg3._psycopg3",
            ["psycopg3/_psycopg3.pyx"],
            libraries=["pq"],
            include_dirs=[includedir],
        )
        pqext = Extension(
            "psycopg3.pq.pq_cython",
            ["psycopg3/pq/pq_cython.pyx"],
            libraries=["pq"],
            include_dirs=[includedir],
        )
        self.distribution.ext_modules = cythonize(
            [pgext, pqext],
            language_level=3,
            # annotate=True,  # enable to get an html view of the C module
        )


setup(
    name="psycopg3",
    description=readme.splitlines()[0],
    long_description="\n".join(readme.splitlines()[2:]).lstrip(),
    author="Daniele Varrazzo",
    author_email="daniele.varrazzo@gmail.com",
    url="https://psycopg.org/psycopg3/",
    python_requires=">=3.6",
    packages=find_packages(exclude=["tests"]),
    classifiers=[x for x in classifiers.split("\n") if x],
    setup_requires=["Cython>=3.0a2"],
    install_requires=["typing_extensions"],
    zip_safe=False,
    include_package_data=True,
    version=version,
    project_urls={
        "Homepage": "https://psycopg.org/",
        "Code": "https://github.com/psycopg/psycopg3",
        "Issue Tracker": "https://github.com/psycopg/psycopg3/issues",
        "Download": "https://pypi.org/project/psycopg3/",
    },
    cmdclass={"build_ext": our_build_ext},
)
