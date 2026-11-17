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


def type_field_to_signature(app, domain, objtype, contentnode):
    """
    Add the type of an attribute (or module data) to its signature.

    Convert a docstring such as::

        The connection the object is managing.

        :type: `Connection`

    into a signature such as ``connection: Connection``. The type in the
    docstring replaces the one from the annotations, if any.

    If an attribute has no type in the docstring and none in the signature
    (a ``:type:`` option, or one found by autodoc), use the annotation of the
    object: the return annotation of a property, or the class annotation.
    Autodoc doesn't use the former, and hand-written directives neither.
    """
    from sphinx.domains.python._annotations import _parse_annotation

    if domain != "py" or objtype not in ("attribute", "property", "data"):
        return

    signodes = own_signatures(contentnode)
    if found := find_type_field(contentnode):
        flist, field = found
        typenodes = unliteral(field[1][0].deepcopy()).children
        flist.remove(field)
        if not flist.children:
            contentnode.remove(flist)
        for signode in signodes:
            set_signature_type(signode, typenodes)

    elif objtype == "attribute":
        for signode in signodes:
            if signature_type(signode) is None:
                if ann := introspect_type(signode):
                    set_signature_type(signode, _parse_annotation(ann, app.env))


def signature_type(signode):
    """Return the ``: type`` annotation node of a signature, None if missing."""
    from sphinx import addnodes

    for ann in signode.findall(addnodes.desc_annotation):
        if ann.astext().startswith(":"):
            return ann
    return None


def set_signature_type(signode, typenodes):
    """Set the ``: type`` annotation of a signature, replacing an existing one."""
    from sphinx import addnodes

    if (ann := signature_type(signode)) is not None:
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


def introspect_type(signode):
    """
    Return the annotation of the attribute described by a signature, as string.

    Return None if the object or the annotation can't be found.
    """
    import typing
    import inspect

    from sphinx.util.typing import stringify_annotation
    from sphinx.util.inspect import signature

    if not (found := find_owner(signode)):
        return None
    owner, name = found

    ann = inspect.Parameter.empty
    attr = inspect.getattr_static(owner, name, None)
    if isinstance(attr, property):
        try:
            ann = signature(attr.fget).return_annotation
        except (TypeError, ValueError):
            return None
    else:
        for cls in owner.__mro__:
            try:
                hints = typing.get_type_hints(cls)
            except Exception:
                hints = inspect.get_annotations(cls)
            if name in hints:
                ann = hints[name]
                break

    if ann is inspect.Parameter.empty:
        return None
    return stringify_annotation(ann, "smart")


def find_owner(signode):
    """
    Return the class owning the attribute described by a signature.

    Return a tuple (class, attribute name), None if not found.
    """
    try:
        path, name = f"{signode['module']}.{signode['fullname']}".rsplit(".", 1)
        owner = deep_import(path)
    except (KeyError, ImportError, AttributeError):
        return None
    if not isinstance(owner, type):
        return None
    return owner, name


def drop_enum_values(app, domain, objtype, contentnode):
    """
    Don't show the values of the enum members (e.g. ``OK = 0``).

    The values are implementation details, not useful to the user.
    """
    import enum

    from sphinx import addnodes

    if domain != "py" or objtype != "attribute":
        return
    for signode in own_signatures(contentnode):
        if not (found := find_owner(signode)):
            continue
        owner, name = found
        if not (isinstance(owner, enum.EnumMeta) and name in owner.__members__):
            continue
        for ann in list(signode.findall(addnodes.desc_annotation)):
            if ann.astext().strip().startswith("="):
                ann.parent.remove(ann)


def own_signatures(contentnode):
    """Return the signatures of an object description, not of its members."""
    from sphinx import addnodes

    return [
        n for n in contentnode.parent.children if isinstance(n, addnodes.desc_signature)
    ]


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


def unwrap_short_signatures(app, domain, objtype, contentnode):
    """
    Render on a single line the signatures that fit `maximum_signature_line_length`.

    Sphinx measures the signature text received from autodoc, which contains
    fully qualified names (e.g. ``~collections.abc.Iterator``) and the class
    name, so it can split a signature that is short once rendered.
    """
    from sphinx import addnodes

    if domain != "py" or not (max_len := app.config.maximum_signature_line_length):
        return

    # Overloaded signatures are unwrapped only together, for consistency
    signodes = own_signatures(contentnode)
    if any(len(signode.astext()) > max_len for signode in signodes):
        return
    for signode in signodes:
        for plist in signode.findall(addnodes.desc_parameterlist):
            if plist.get("multi_line_parameter_list"):
                plist["multi_line_parameter_list"] = False


def drop_separator_tooltips(app, doctree):
    """
    Drop the tooltips from the ``*`` and ``/`` separators in signatures.

    Sphinx renders them as an abbreviation explaining e.g. "Keyword-only
    parameters separator (PEP 3102)", which is more distracting than useful.
    """
    from sphinx import addnodes
    from docutils import nodes

    for abbr in list(doctree.findall(nodes.abbreviation)):
        if isinstance(abbr.parent, addnodes.desc_sig_operator):
            abbr.replace_self(nodes.Text(abbr.astext()))


def tidy_signature(app, what, name, obj, options, signature, return_annotation):
    """
    Fix signature details that Sphinx renders badly.

    - Render a default such as ``<class 'psycopg.Connection'>`` as its name.
      The repr of a class is not valid Python, so the signature would not be
      parsed: it would be rendered as plain text, on a single line.

    - Render ``Callable[..., T]`` correctly: Sphinx renders it as
      ``Callable[[...], T]``. Reported in
      https://github.com/sphinx-doc/sphinx/issues/14856

    - Render the internal ``BaseCursor`` (e.g. in row factories) as the public
      classes it stands for.
    """

    def tidy(text):
        if text:
            text = re.sub(r"<class '(?:[\w.]+\.)?(\w+)'>", r"\1", text)
            text = re.sub(r"Callable\[\[\.\.\.\],", "Callable[...,", text)
            text = re.sub(
                r"\bBaseCursor\[Any, Any\]",
                "~psycopg.Cursor[Any] | ~psycopg.AsyncCursor[Any]",
                text,
            )
        return text

    return tidy(signature), tidy(return_annotation)


def unlink_typevars(app, env, node, contnode):
    """
    Don't try to link type variables (e.g. `!T` in `!Range[T]`).

    They are generic placeholders, not documented objects.
    """
    import typing

    if node.get("refdomain") != "py":
        return None
    try:
        obj = deep_import(node.get("reftarget", ""))
    except Exception:
        return None
    return contnode if isinstance(obj, typing.TypeVar) else None


# Objects and modules from the Python docs that are not useful to link
unlinked_objects = {"typing.Any", "typing.Self"}
unlinked_modules = {"collections.abc"}


def unlink_builtins(app, env, node, contnode):
    """
    Don't link builtin types and constants (`str`, `None`...) to Python docs.

    Same for the objects in `unlinked_objects` and `unlinked_modules`.

    The links are not useful and they make signatures noisy. Run before
    intersphinx, which would resolve them.
    """
    import builtins

    if node.get("refdomain") != "py":
        return None
    name = node.get("reftarget", "")
    if name in unlinked_objects or f"typing.{name}" in unlinked_objects:
        return contnode
    if name.rpartition(".")[0] in unlinked_modules:
        return contnode
    name = name.removeprefix("builtins.")
    if name not in (names := vars(builtins)):
        return None
    if (obj := names[name]) is None or obj is True or obj is False:
        return contnode
    if isinstance(obj, type) and not issubclass(obj, BaseException):
        return contnode  # exceptions docs are useful, functions too
    return None


def setup(app):
    app.connect("missing-reference", unlink_builtins, priority=400)
    app.connect("missing-reference", unlink_typevars, priority=400)
    app.connect("doctree-read", drop_separator_tooltips)
    app.connect("autodoc-before-process-signature", before_process_signature)
    app.connect("autodoc-process-signature", tidy_signature)
    app.connect("object-description-transform", type_field_to_signature)
    app.connect("object-description-transform", unwrap_short_signatures)
    app.connect("object-description-transform", drop_enum_values)

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
