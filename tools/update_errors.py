#!/usr/bin/env python
# type: ignore
"""
Generate per-sqlstate errors from PostgreSQL source code.

The script can be run at a new PostgreSQL release to refresh the module.
"""

# Copyright (C) 2020 The Psycopg Team

import os
import re
import sys
import logging
from urllib.request import urlopen
from collections import defaultdict, namedtuple

from psycopg.errors import get_base_exception

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    classes, errors = fetch_errors("9.6 10 11 12 13 14 15".split())

    fn = os.path.dirname(__file__) + "/../psycopg/psycopg/errors.py"
    update_file(fn, generate_module_data(classes, errors))

    fn = os.path.dirname(__file__) + "/../docs/api/errors.rst"
    update_file(fn, generate_docs_data(classes, errors))


def parse_errors_txt(url):
    classes = {}
    errors = defaultdict(dict)

    page = urlopen(url)
    for line in page.read().decode("ascii").splitlines():
        # Strip comments and skip blanks
        line = line.split("#")[0].strip()
        if not line:
            continue

        # Parse a section
        m = re.match(r"Section: (Class (..) - .+)", line)
        if m:
            label, class_ = m.groups()
            classes[class_] = label
            continue

        # Parse an error
        m = re.match(r"(.....)\s+(?:E|W|S)\s+ERRCODE_(\S+)(?:\s+(\S+))?$", line)
        if m:
            sqlstate, macro, spec = m.groups()
            # skip sqlstates without specs as they are not publicly visible
            if not spec:
                continue
            errlabel = spec.upper()
            errors[class_][sqlstate] = errlabel
            continue

        # We don't expect anything else
        raise ValueError("unexpected line:\n%s" % line)

    return classes, errors


errors_txt_url = (
    "http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob_plain;"
    "f=src/backend/utils/errcodes.txt;hb=%s"
)


Error = namedtuple("Error", "sqlstate errlabel clsname basename")


def fetch_errors(versions):
    classes = {}
    errors = defaultdict(dict)

    for version in versions:
        logger.info("fetching errors from version %s", version)
        tver = tuple(map(int, version.split()[0].split(".")))
        tag = "%s%s_STABLE" % (
            (tver[0] >= 10 and "REL_" or "REL"),
            version.replace(".", "_"),
        )
        c1, e1 = parse_errors_txt(errors_txt_url % tag)
        classes.update(c1)

        for c, cerrs in e1.items():
            errors[c].update(cerrs)

    # clean up data

    # success and warning - never raised
    del classes["00"]
    del classes["01"]
    del errors["00"]
    del errors["01"]

    specific = {
        "38002": "ModifyingSqlDataNotPermittedExt",
        "38003": "ProhibitedSqlStatementAttemptedExt",
        "38004": "ReadingSqlDataNotPermittedExt",
        "39004": "NullValueNotAllowedExt",
        "XX000": "InternalError_",
    }

    seen = set(
        """
        Error Warning InterfaceError DataError DatabaseError ProgrammingError
        IntegrityError InternalError NotSupportedError OperationalError
        """.split()
    )

    for c, cerrs in errors.items():
        for sqstate, errlabel in list(cerrs.items()):
            if sqstate in specific:
                clsname = specific[sqstate]
            else:
                clsname = errlabel.title().replace("_", "")
            if clsname in seen:
                raise Exception("class already existing: %s" % clsname)
            seen.add(clsname)

            basename = get_base_exception(sqstate).__name__
            cerrs[sqstate] = Error(sqstate, errlabel, clsname, basename)

    return classes, errors


def generate_module_data(classes, errors):
    yield ""

    for clscode, clslabel in sorted(classes.items()):
        yield f"""
# {clslabel}
"""
        for _, e in sorted(errors[clscode].items()):
            yield f"""\
class {e.clsname}({e.basename},
    code={e.sqlstate!r}, name={e.errlabel!r}):
    pass
"""
    yield ""


def generate_docs_data(classes, errors):
    Line = namedtuple("Line", "colstate colexc colbase, sqlstate")
    lines = [Line("SQLSTATE", "Exception", "Base exception", None)]

    for clscode in sorted(classes):
        for _, error in sorted(errors[clscode].items()):
            lines.append(
                Line(
                    f"``{error.sqlstate}``",
                    f"`!{error.clsname}`",
                    f"`!{error.basename}`",
                    error.sqlstate,
                )
            )

    widths = [max(len(line[c]) for line in lines) for c in range(3)]
    h = Line(*(["=" * w for w in widths] + [None]))
    lines.insert(0, h)
    lines.insert(2, h)
    lines.append(h)

    h1 = "-" * (sum(widths) + len(widths) - 1)
    sqlclass = None

    yield ""
    for line in lines:
        cls = line.sqlstate[:2] if line.sqlstate else None
        if cls and cls != sqlclass:
            yield re.sub(r"(Class\s+[^\s]+)", r"**\1**", classes[cls])
            yield h1
            sqlclass = cls

        yield (
            "%-*s %-*s %-*s"
            % (
                widths[0],
                line.colstate,
                widths[1],
                line.colexc,
                widths[2],
                line.colbase,
            )
        ).rstrip()

    yield ""


def update_file(fn, new_lines):
    logger.info("updating %s", fn)

    with open(fn, "r") as f:
        lines = f.read().splitlines()

    istart, iend = [
        i
        for i, line in enumerate(lines)
        if re.match(r"\s*(#|\.\.)\s*autogenerated:\s+(start|end)", line)
    ]

    lines[istart + 1 : iend] = new_lines

    with open(fn, "w") as f:
        for line in lines:
            f.write(line + "\n")


if __name__ == "__main__":
    sys.exit(main())
