"""
Customisation for docs generation.
"""

# Copyright (C) 2020 The Psycopg Team


def process_docstring(app, what, name, obj, options, lines):
    pass


def before_process_signature(app, obj, bound_method):
    ann = getattr(obj, "__annotations__", {})
    if "return" in ann:
        # Drop "return: None" from the function signatures
        if ann["return"] is None:
            del ann["return"]
        elif ann["return"] == "PGcancel":
            ann["return"] = "psycopg3.pq.PGcancel"


def process_signature(
    app, what, name, obj, options, signature, return_annotation
):
    pass


def setup(app):
    app.connect("autodoc-process-docstring", process_docstring)
    app.connect("autodoc-process-signature", process_signature)
    app.connect("autodoc-before-process-signature", before_process_signature)
