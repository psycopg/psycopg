#!/usr/bin/env python
"""Generate per-sqlstate errors from PostgreSQL source code.

The script can be run at a new PostgreSQL release to refresh the module.
"""

# Copyright (C) 2020 The Psycopg Team


import os
import re
import sys
from urllib.request import urlopen
from collections import defaultdict

from psycopg3.errors import get_base_exception


def main():

    fn = os.path.dirname(__file__) + "/../psycopg3/errors.py"

    with open(fn, "r") as f:
        lines = f.read().splitlines()

    istart, iend = [
        i
        for i, line in enumerate(lines)
        if re.match(r"\s*#\s*autogenerated:\s+(start|end)", line)
    ]

    classes, errors = fetch_errors(["9.5", "9.6", "10", "11", "12"])
    lines[istart + 1 : iend] = generate_module_data(classes, errors)

    with open(fn, "w") as f:
        for line in lines:
            f.write(line + "\n")


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
        m = re.match(
            r"(.....)\s+(?:E|W|S)\s+ERRCODE_(\S+)(?:\s+(\S+))?$", line
        )
        if m:
            errcode, macro, spec = m.groups()
            # skip errcodes without specs as they are not publically visible
            if not spec:
                continue
            errlabel = spec.upper()
            errors[class_][errcode] = errlabel
            continue

        # We don't expect anything else
        raise ValueError("unexpected line:\n%s" % line)

    return classes, errors


errors_txt_url = (
    "http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob_plain;"
    "f=src/backend/utils/errcodes.txt;hb=%s"
)


def fetch_errors(versions):
    classes = {}
    errors = defaultdict(dict)

    for version in versions:
        print(version, file=sys.stderr)
        tver = tuple(map(int, version.split()[0].split(".")))
        tag = "%s%s_STABLE" % (
            (tver[0] >= 10 and "REL_" or "REL"),
            version.replace(".", "_"),
        )
        c1, e1 = parse_errors_txt(errors_txt_url % tag)
        classes.update(c1)

        for c, cerrs in e1.items():
            errors[c].update(cerrs)

    return classes, errors


def generate_module_data(classes, errors):
    tmpl = """
@sqlcode(%(errcode)r)
class %(cls)s(%(base)s):
    pass
"""
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

    for clscode, clslabel in sorted(classes.items()):
        if clscode in ("00", "01"):
            # success and warning - never raised
            continue

        yield f"\n# {clslabel}"

        for errcode, errlabel in sorted(errors[clscode].items()):
            if errcode in specific:
                clsname = specific[errcode]
            else:
                clsname = errlabel.title().replace("_", "")
            if clsname in seen:
                raise Exception("class already existing: %s" % clsname)
            seen.add(clsname)

            base = get_base_exception(errcode)

            yield tmpl % {
                "cls": clsname,
                "errcode": errcode,
                "base": base.__name__,
            }


if __name__ == "__main__":
    sys.exit(main())
