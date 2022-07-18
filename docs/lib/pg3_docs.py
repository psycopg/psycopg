"""
Customisation for docs generation.
"""

# Copyright (C) 2020 The Psycopg Team

import os
import re
import logging
import importlib
from typing import Dict
from collections import deque


def process_docstring(app, what, name, obj, options, lines):
    pass


def before_process_signature(app, obj, bound_method):
    ann = getattr(obj, "__annotations__", {})
    if "return" in ann:
        # Drop "return: None" from the function signatures
        if ann["return"] is None:
            del ann["return"]


def process_signature(app, what, name, obj, options, signature, return_annotation):
    pass


def setup(app):
    app.connect("autodoc-process-docstring", process_docstring)
    app.connect("autodoc-process-signature", process_signature)
    app.connect("autodoc-before-process-signature", before_process_signature)

    import psycopg  # type: ignore

    recover_defined_module(
        psycopg, skip_modules=["psycopg._dns", "psycopg.types.shapely"]
    )
    monkeypatch_autodoc()

    # Disable warnings in sphinx_autodoc_typehints because it doesn't seem that
    # there is a workaround for: "WARNING: Cannot resolve forward reference in
    # type annotations"
    logger = logging.getLogger("sphinx.sphinx_autodoc_typehints")
    logger.setLevel(logging.ERROR)


# Classes which may have __module__ overwritten
recovered_classes: Dict[type, str] = {}


def recover_defined_module(m, skip_modules=()):
    """
    Find the module where classes with __module__ attribute hacked were defined.

    Autodoc will get confused and will fail to inspect attribute docstrings
    (e.g. from enums and named tuples).

    Save the classes recovered in `recovered_classes`, to be used by
    `monkeypatch_autodoc()`.

    """
    mdir = os.path.split(m.__file__)[0]
    for fn in walk_modules(mdir):
        assert fn.startswith(mdir)
        modname = os.path.splitext(fn[len(mdir) + 1 :])[0].replace("/", ".")
        modname = f"{m.__name__}.{modname}"
        if modname in skip_modules:
            continue
        with open(fn) as f:
            classnames = re.findall(r"^class\s+([^(:]+)", f.read(), re.M)
            for cls in classnames:
                cls = deep_import(f"{modname}.{cls}")
                if cls.__module__ != modname:
                    recovered_classes[cls] = modname


def monkeypatch_autodoc():
    """
    Patch autodoc in order to use information found by `recover_defined_module`.
    """
    from sphinx.ext.autodoc import Documenter, AttributeDocumenter

    orig_doc_get_real_modname = Documenter.get_real_modname
    orig_attr_get_real_modname = AttributeDocumenter.get_real_modname
    orig_attr_add_content = AttributeDocumenter.add_content

    def fixed_doc_get_real_modname(self):
        if self.object in recovered_classes:
            return recovered_classes[self.object]
        return orig_doc_get_real_modname(self)

    def fixed_attr_get_real_modname(self):
        if self.parent in recovered_classes:
            return recovered_classes[self.parent]
        return orig_attr_get_real_modname(self)

    def fixed_attr_add_content(self, more_content):
        """
        Replace a docstring such as::

            .. py:attribute:: ConnectionInfo.dbname
               :module: psycopg

               The database name of the connection.

               :rtype: :py:class:`str`

        into:

            .. py:attribute:: ConnectionInfo.dbname
               :type: str
               :module: psycopg

               The database name of the connection.

        which creates a more compact representation of a property.

        """
        orig_attr_add_content(self, more_content)
        if not isinstance(self.object, property):
            return
        iret, mret = match_in_lines(r"\s*:rtype: (.*)", self.directive.result)
        iatt, matt = match_in_lines(r"\.\.", self.directive.result)
        if not (mret and matt):
            return
        self.directive.result.pop(iret)
        self.directive.result.insert(
            iatt + 1,
            f"{self.indent}:type: {unrest(mret.group(1))}",
            source=self.get_sourcename(),
        )

    Documenter.get_real_modname = fixed_doc_get_real_modname
    AttributeDocumenter.get_real_modname = fixed_attr_get_real_modname
    AttributeDocumenter.add_content = fixed_attr_add_content


def match_in_lines(pattern, lines):
    """Match a regular expression against a list of strings.

    Return the index of the first matched line and the match object.
    None, None if nothing matched.
    """
    for i, line in enumerate(lines):
        m = re.match(pattern, line)
        if m:
            return i, m
    else:
        return None, None


def unrest(s):
    r"""remove the reST markup from a string

    e.g. :py:data:`~typing.Optional`\[:py:class:`int`] -> Optional[int]

    required because :type: does the types lookup itself apparently.
    """
    s = re.sub(r":[^`]*:`~?([^`]*)`", r"\1", s)  # drop role
    s = re.sub(r"\\(.)", r"\1", s)  # drop escape

    # note that ~psycopg.pq.ConnStatus is converted to pq.ConnStatus
    # which should be interpreted well if currentmodule is set ok.
    s = re.sub(r"(?:typing|psycopg)\.", "", s)  # drop unneeded modules
    s = re.sub(r"~", "", s)  # drop the tilde

    return s


def walk_modules(d):
    for root, dirs, files in os.walk(d):
        for f in files:
            if f.endswith(".py"):
                yield f"{root}/{f}"


def deep_import(name):
    parts = deque(name.split("."))
    seen = []
    if not parts:
        raise ValueError("name must be a dot-separated name")

    seen.append(parts.popleft())
    thing = importlib.import_module(seen[-1])
    while parts:
        attr = parts.popleft()
        seen.append(attr)

        if hasattr(thing, attr):
            thing = getattr(thing, attr)
        else:
            thing = importlib.import_module(".".join(seen))

    return thing
