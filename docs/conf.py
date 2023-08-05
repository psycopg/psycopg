# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

import sys
from pathlib import Path

import psycopg

docs_dir = Path(__file__).parent
sys.path.append(str(docs_dir / "lib"))


# -- Project information -----------------------------------------------------

project = "psycopg"
copyright = "2020, Daniele Varrazzo and The Psycopg Team"
author = "Daniele Varrazzo"
release = psycopg.__version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    # Currently breaking docs link (see #562)
    # "sphinx_autodoc_typehints",
    "sql_role",
    "ticket_role",
    "pg3_docs",
    "libpq_docs",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", ".venv"]


# -- Options for HTML output -------------------------------------------------

# The announcement may be in the website but not shipped with the docs
ann_file = docs_dir / "../../templates/docs3-announcement.html"
if ann_file.exists():
    with ann_file.open() as f:
        announcement = f.read()
else:
    announcement = ""

html_css_files = ["psycopg.css"]

# The name of the Pygments (syntax highlighting) style to use.
# Some that I've check don't suck:
# default lovelace tango algol_nu
# list: from pygments.styles import STYLE_MAP; print(sorted(STYLE_MAP.keys()))
pygments_style = "tango"

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "furo"
html_show_sphinx = True
html_show_sourcelink = False
html_theme_options = {
    "announcement": announcement,
    "sidebar_hide_name": False,
    "light_logo": "psycopg.svg",
    "dark_logo": "psycopg.svg",
    "light_css_variables": {
        "admonition-font-size": "1rem",
    },
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# The reST default role (used for this markup: `text`) to use for all documents.
default_role = "obj"

intersphinx_mapping = {
    "py": ("https://docs.python.org/3", None),
    "pg2": ("https://www.psycopg.org/docs/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}

autodoc_member_order = "bysource"

# PostgreSQL docs version to link libpq functions to
libpq_docs_version = "14"

# Where to point on :ticket: role
ticket_url = "https://github.com/psycopg/psycopg/issues/%s"
