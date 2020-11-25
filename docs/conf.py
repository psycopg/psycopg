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

sys.path.append(str(Path(__file__).parent / "lib"))


# -- Project information -----------------------------------------------------

project = "psycopg3"
copyright = "2020, Daniele Varrazzo and The Psycopg Team"
author = "Daniele Varrazzo"
release = "UNRELEASED"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sql_role",
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

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "friendly"

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"
html_show_sphinx = False
html_theme_options = {
    "announcement": """
        <a style=\"text-decoration: none; color: white;\" 
           href=\"https://github.com/sponsors/dvarrazzo\">
           <img height="24px" width="24px" src=\"_static/psycopg-48.png\"/> Sponsor psycopg3 on GitHub
        </a>
    """,
    "sidebar_hide_name": True,
    "light_logo": "psycopg-100.png",
    "dark_logo": "psycopg-100.png",
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
}

autodoc_member_order = "bysource"

# PostgreSQL docs version to link libpq functions to
libpq_docs_version = "13"
