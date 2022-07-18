"""
Sphinx plugin to link to the libpq documentation.

Add the ``:pq:`` role, to create a link to a libpq function, e.g. ::

    :pq:`PQlibVersion()`

will link to::

    https://www.postgresql.org/docs/current/libpq-misc.html #LIBPQ-PQLIBVERSION

"""

# Copyright (C) 2020 The Psycopg Team

import os
import logging
import urllib.request
from pathlib import Path
from functools import lru_cache
from html.parser import HTMLParser

from docutils import nodes, utils
from docutils.parsers.rst import roles

logger = logging.getLogger("sphinx.libpq_docs")


class LibpqParser(HTMLParser):
    def __init__(self, data, version="current"):
        super().__init__()
        self.data = data
        self.version = version

        self.section_id = None
        self.varlist_id = None
        self.in_term = False
        self.in_func = False

    def handle_starttag(self, tag, attrs):
        if tag == "sect1":
            self.handle_sect1(tag, attrs)
        elif tag == "varlistentry":
            self.handle_varlistentry(tag, attrs)
        elif tag == "term":
            self.in_term = True
        elif tag == "function":
            self.in_func = True

    def handle_endtag(self, tag):
        if tag == "term":
            self.in_term = False
        elif tag == "function":
            self.in_func = False

    def handle_data(self, data):
        if not (self.in_term and self.in_func):
            return

        self.add_function(data)

    def handle_sect1(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.section_id = attrs["id"]

    def handle_varlistentry(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.varlist_id = attrs["id"]

    def add_function(self, func_name):
        self.data[func_name] = self.get_func_url()

    def get_func_url(self):
        assert self.section_id, "<sect1> tag not found"
        assert self.varlist_id, "<varlistentry> tag not found"
        return self._url_pattern.format(
            version=self.version,
            section=self.section_id,
            func_id=self.varlist_id.upper(),
        )

    _url_pattern = "https://www.postgresql.org/docs/{version}/{section}.html#{func_id}"


class LibpqReader:
    # must be set before using the rest of the class.
    app = None

    _url_pattern = (
        "https://raw.githubusercontent.com/postgres/postgres/REL_{ver}_STABLE"
        "/doc/src/sgml/libpq.sgml"
    )

    data = None

    def get_url(self, func):
        if not self.data:
            self.parse()

        return self.data[func]

    def parse(self):
        if not self.local_file.exists():
            self.download()

        logger.info("parsing libpq docs from %s", self.local_file)
        self.data = {}
        parser = LibpqParser(self.data, version=self.version)
        with self.local_file.open("r") as f:
            parser.feed(f.read())

    def download(self):
        filename = os.environ.get("LIBPQ_DOCS_FILE")
        if filename:
            logger.info("reading postgres libpq docs from %s", filename)
            with open(filename, "rb") as f:
                data = f.read()
        else:
            logger.info("downloading postgres libpq docs from %s", self.sgml_url)
            data = urllib.request.urlopen(self.sgml_url).read()

        with self.local_file.open("wb") as f:
            f.write(data)

    @property
    def local_file(self):
        return Path(self.app.doctreedir) / f"libpq-{self.version}.sgml"

    @property
    def sgml_url(self):
        return self._url_pattern.format(ver=self.version)

    @property
    def version(self):
        return self.app.config.libpq_docs_version


@lru_cache()
def get_reader():
    return LibpqReader()


def pq_role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    text = utils.unescape(text)

    reader = get_reader()
    if "(" in text:
        func, noise = text.split("(", 1)
        noise = "(" + noise

    else:
        func = text
        noise = ""

    try:
        url = reader.get_url(func)
    except KeyError:
        msg = inliner.reporter.warning(
            f"function {func} not found in libpq {reader.version} docs"
        )
        prb = inliner.problematic(rawtext, rawtext, msg)
        return [prb], [msg]

    # For a function f(), include the () in the signature for consistency
    # with a normal `thing()`
    if noise == "()":
        func, noise = func + noise, ""

    the_nodes = []
    the_nodes.append(nodes.reference(func, func, refuri=url))
    if noise:
        the_nodes.append(nodes.Text(noise))

    return [nodes.literal("", "", *the_nodes, **options)], []


def setup(app):
    app.add_config_value("libpq_docs_version", "14", "html")
    roles.register_local_role("pq", pq_role)
    get_reader().app = app
