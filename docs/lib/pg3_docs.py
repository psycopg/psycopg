# mypy: ignore-errors
"""
Customisation for docs generation.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import os
import re
import importlib
from collections import deque


def before_process_signature(app, obj, bound_method):
    if "return" in (ann := getattr(obj, "__annotations__", {})):
        # Drop "return: None" from the function signatures
        # (a string if the module uses `from __future__ import annotations`)
        if ann["return"] in (None, "None"):
            del ann["return"]


# Type annotations of the properties documented as attributes, by full name
property_types: dict[str, str] = {}


def record_property_type(app, what, name, obj, options, lines):
    """
    Record the return annotation of a property documented as attribute.

    Autodoc ignores it, unlike for an ``autoproperty`` directive.
    """
    from sphinx.util.typing import stringify_annotation
    from sphinx.util.inspect import signature

    if what != "attribute" or not isinstance(obj, property) or not obj.fget:
        return
    try:
        sig = signature(obj.fget)
    except (TypeError, ValueError):
        return
    if sig.return_annotation is not sig.empty:
        property_types[name] = stringify_annotation(sig.return_annotation, "smart")


def type_field_to_signature(app, domain, objtype, contentnode):
    """
    Add the type of an attribute to its signature.

    Convert a docstring such as::

        The connection the object is managing.

        :type: `Connection`

    into a signature such as ``connection: Connection``. The type in the
    docstring replaces the one from the annotations, if any.

    If the attribute is a property with no type in the docstring, use its
    return annotation.
    """
    from sphinx import addnodes
    from sphinx.domains.python._annotations import _parse_annotation

    if domain != "py" or objtype not in ("attribute", "property"):
        return

    signodes = list(contentnode.parent.findall(addnodes.desc_signature))
    if found := find_type_field(contentnode):
        flist, field = found
        typenodes = unliteral(field[1][0].deepcopy()).children
        flist.remove(field)
        if not flist.children:
            contentnode.remove(flist)
    elif objtype == "attribute" and signodes:
        sig = signodes[0]
        fullname = f"{sig.get('module')}.{sig.get('fullname')}"
        if not (ann := property_types.get(fullname)):
            return
        typenodes = _parse_annotation(ann, app.env)
    else:
        return

    for signode in signodes:
        for ann in list(signode.findall(addnodes.desc_annotation)):
            if ann.astext().startswith(":"):
                ann.parent.remove(ann)

        name = next(signode.findall(addnodes.desc_name))
        name.parent.insert(
            name.parent.index(name) + 1,
            addnodes.desc_annotation(
                "",
                "",
                addnodes.desc_sig_punctuation("", ":"),
                addnodes.desc_sig_space(),
                *(n.deepcopy() for n in typenodes),
            ),
        )


def unliteral(node):
    """
    Replace the literals in a node with their content.

    Cross-references in signatures are not styled as inline code.
    """
    from docutils import nodes

    for lit in list(node.findall(nodes.literal)):
        parent = lit.parent
        idx = parent.index(lit)
        parent.remove(lit)
        for child in reversed(lit.children):
            parent.insert(idx, child)
    return node


def find_type_field(contentnode):
    """
    Return the ``:type:`` or ``:rtype:`` field of an object description.

    Return a tuple (field list, field), None if not found.
    """
    from docutils import nodes

    for flist in contentnode.children:
        if not isinstance(flist, nodes.field_list):
            continue
        for field in flist.children:
            if field[0].astext() not in ("type", "rtype"):
                continue
            if len(field[1]) == 1 and isinstance(field[1][0], nodes.paragraph):
                return flist, field

    return None


def setup(app):
    app.connect("autodoc-before-process-signature", before_process_signature)
    app.connect("autodoc-process-docstring", record_property_type)
    app.connect("object-description-transform", type_field_to_signature)

    import psycopg  # type: ignore

    recover_defined_module(
        psycopg, skip_modules=["psycopg._dns", "psycopg.types.shapely"]
    )
    merge_attr_docs()


# Classes which may have __module__ overwritten
recovered_classes: dict[type, str] = {}


def recover_defined_module(m, skip_modules=()):
    """
    Find the module where classes with __module__ attribute hacked were defined.

    Autodoc will get confused and will fail to inspect attribute docstrings
    (e.g. from enums and named tuples).

    Save the classes recovered in `recovered_classes`, to be used by
    `merge_attr_docs()`.

    """
    mdir = os.path.split(m.__file__)[0]
    for fn in walk_modules(mdir):
        assert fn.startswith(mdir)
        modname = os.path.splitext(fn[len(mdir) + 1 :])[0].replace("/", ".")
        if (modname := f"{m.__name__}.{modname}") in skip_modules:
            continue
        with open(fn) as f:
            classnames = re.findall(r"^class\s+([^(:]+)", f.read(), re.M)
            for cls in classnames:
                if (cls := deep_import(f"{modname}.{cls}")).__module__ != modname:
                    recovered_classes[cls] = modname


def merge_attr_docs():
    """
    Make the attribute docstrings of `recovered_classes` visible to autodoc.

    Autodoc looks for the docstrings of the attributes (e.g. enum members) in
    the source of the module declared by `__module__`, where they are not
    found, so the members are dropped as undocumented. Copy them to the source
    analyzer of the declared module, which is cached by Sphinx.
    """
    from sphinx.pycode import ModuleAnalyzer

    for cls, modname in recovered_classes.items():
        src = ModuleAnalyzer.for_module(modname)
        src.analyze()
        dst = ModuleAnalyzer.for_module(cls.__module__)
        dst.analyze()

        offset = max(dst.tagorder.values(), default=0) + 1
        prefix = f"{cls.__qualname__}."
        for name, order in src.tagorder.items():
            if name.startswith(prefix):
                dst.tagorder.setdefault(name, order + offset)

        for (scope, name), doc in src.attr_docs.items():
            if scope == cls.__qualname__:
                dst.attr_docs.setdefault((scope, name), doc)


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
